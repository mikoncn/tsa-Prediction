import pandas as pd
import holidays
from dateutil.easter import easter

# ==========================================
# 1. Holiday Configuration (The "Master List")
# ==========================================

# Tier 1: Major Holidays (Extended Travel Window +/- 6 days)
TIER_1_HOLIDAYS = [
    'Christmas Day', 
    'Thanksgiving', 
    'Independence Day', 
    'New Year\'s Day'
]

# Tier 2: Standard Holidays (Long Weekend Window +/- 3 days)
TIER_2_HOLIDAYS = [
    'Martin Luther King Jr. Day', 
    'Memorial Day', 
    'Labor Day', 
    'Washington\'s Birthday', # Presidents' Day
    'Columbus Day', 
    'Veterans Day',
    'Juneteenth'
]

# Full Whitelist used for "Nearest Holiday" calculation
TARGET_HOLIDAYS = TIER_1_HOLIDAYS + TIER_2_HOLIDAYS

# ==========================================
# 2. Logic Functions
# ==========================================

def get_us_holidays(start_year=2018, end_year=2030):
    """Return dict of {date: name} for all US holidays in range."""
    return holidays.US(years=range(start_year, end_year + 1))

def get_holiday_features(date_series):
    """
    Generate detailed holiday features for a given Series of dates.
    Returns a DataFrame with columns: 
    [is_holiday, is_holiday_exact_day, is_holiday_travel_window, holiday_name, is_spring_break]
    """
    # Ensure Series for .dt accessor consistency
    dates = pd.to_datetime(date_series)
    if isinstance(dates, pd.DatetimeIndex):
        dates = dates.to_series(index=dates)
        
    years = dates.dt.year.unique()
    min_year, max_year = int(min(years)), int(max(years))
    
    # Load Holidays
    us_holidays = get_us_holidays(min_year, max_year)
    
    # Pre-calculate Easter & Super Bowl for these years to avoid repeated calc
    easter_dates = {}
    good_fridays = {}
    
    for y in range(min_year, max_year + 1):
        try:
            e_date = pd.Timestamp(easter(y))
            easter_dates[e_date] = 'Easter Sunday'
            good_fridays[e_date - pd.Timedelta(days=2)] = 'Good Friday'
        except: pass
        
    # Result container
    results = []
    
    for d in dates:
        dt = d.date()
        res = {
            'is_holiday': 0,
            'is_holiday_exact_day': 0,
            'is_holiday_travel_window': 0,
            'holiday_name': '',
            'is_spring_break': 0
        }
        
        # 1. Base Holiday Check
        name = us_holidays.get(dt)
        if not name:
            if d in easter_dates: name = easter_dates[d]
            elif d in good_fridays: name = good_fridays[d]
            
        if name:
            res['is_holiday'] = 1
            res['holiday_name'] = name
            
            # Check if it is a "Target Holiday" (Exact Day)
            if any(t in name for t in TARGET_HOLIDAYS) or name == 'Good Friday':
                res['is_holiday_exact_day'] = 1
        
        # 2. Travel Window Logic (The "Window" Check)
        # Check against ALL target holidays to see if d falls in their window
        # This scans the calendar around d
        # Optimization: Scan only reasonable range? No, easier to iterate targets for this year
        
        # Better approach: We need to know if 'd' is close to ANY target holiday
        # Let's find the closest target holiday
        # (This duplicates some "nearest_holiday" logic but is needed for the window flag)
        
        # Actually, for the 'is_holiday_travel_window' flag, we can be simpler:
        # Is this day within the window of a Tier 1 or Tier 2 holiday?
        
        # We need the DATE of the target holidays for this year (and prev/next year for boundary)
        # This is expensive to do per row. 
        # Vectorized approach is better, but function is per-row? No, input is Series.
        pass # Will implement vectorized below
        
        results.append(res)
        
    # Vectorized Implementation
    df_res = pd.DataFrame(results) # Init with base
    df_res['date'] = dates.values # Use values to ignore index alignment
    
    # Re-eval Windows efficiently
    # Generate all Target Holiday Dates
    target_events = []
    for date, name in us_holidays.items():
        if any(t in name for t in TARGET_HOLIDAYS):
            target_events.append({'date': pd.Timestamp(date), 'name': name, 'type': 'tier1' if any(t1 in name for t1 in TIER_1_HOLIDAYS) else 'tier2'})
            
    # Add Easter/Good Friday
    for y in range(min_year, max_year + 1):
        try:
            ed = pd.Timestamp(easter(y))
            target_events.append({'date': ed, 'name': 'Easter Sunday', 'type': 'tier2'}) # Easter is short window
            target_events.append({'date': ed - pd.Timedelta(days=2), 'name': 'Good Friday', 'type': 'tier2'})
        except: pass
        
    # Mark Windows
    # Create mask
    window_mask = pd.Series(0, index=df_res.index)
    window_names = pd.Series('', index=df_res.index) # Append names?
    
    for event in target_events:
        e_date = event['date']
        w_size = 6 if event['type'] == 'tier1' else 3
        
        # Define window range
        win_start = e_date - pd.Timedelta(days=w_size)
        win_end = e_date + pd.Timedelta(days=w_size)
        
        # Find rows in this window
        mask = (df_res['date'] >= win_start) & (df_res['date'] <= win_end) & (df_res['date'] != e_date)
        
        df_res.loc[mask, 'is_holiday_travel_window'] = 1
        
        # Optional: Add window name if empty
        # If multiple windows overlap, we just say "Travel Window"
        # Or "Travel Window (HolidayName)"
        # For simplicity in 'traffic_full', we often just want the flag.
        
        # Updating name for windows? Current logic in merge_db was:
        # holiday_name = "Travel Window (Name)"
        # Let's support that
        
        current_names = df_res.loc[mask, 'holiday_name']
        new_name = f"Travel Window ({event['name']})"
        
        # Only overwrite if empty or append?
        # If it's already a holiday (e.g. overlap), keep existing name
        # If it's empty, set it.
        # If it's "Travel Window (Other)", maybe append?
        
        # Vectorized update
        # 1. Empty names -> Set
        # 2. Non-empty -> Keep (Priority to actual holiday or existing window)
        mask_empty = mask & (df_res['holiday_name'] == '')
        df_res.loc[mask_empty, 'holiday_name'] = new_name
        
    # 3. Spring Break
    # Month 3, 4 + Weekend + Not Holiday
    df_res['dow'] = df_res['date'].dt.dayofweek
    df_res['month'] = df_res['date'].dt.month
    
    mask_sb = (df_res['month'].isin([3, 4])) & \
              (df_res['dow'].isin([5, 6])) & \
              (df_res['is_holiday'] == 0)
              
    df_res.loc[mask_sb, 'is_spring_break'] = 1
    
    return df_res[['is_holiday', 'is_holiday_exact_day', 'is_holiday_travel_window', 'holiday_name', 'is_spring_break']]

# ==========================================
# 3. New Optimization Features (Classic v2)
# ==========================================

def get_holiday_intensity(holiday_name):
    """
    Returns an intensity score for a given holiday.
    High intensity = Major drop on exact day (Christmas, Thanksgiving).
    Medium intensity = Standard long weekend (Labor Day, MLK).
    """
    if not holiday_name: return 0
    name = holiday_name.lower()
    
    # High Intensity (Extreme Drops)
    if any(h in name for h in ['christmas', 'thanksgiving', 'new year']):
        return 3
    
    # Medium Intensity (Standard Holiday Window)
    if any(h in name for h in ['labor', 'memorial', 'king', 'washington', 'independence']):
        return 2
    
    # Low Intensity (Minor or Window days)
    return 1

def get_clean_lag_date(date, holiday_dates, lag_days=7):
    """
    Recursively finds the nearest past date that is 'lag_days' away 
    and is NOT a holiday. Handles the 'Pattern A' New Year hangover.
    """
    current_lag_date = date - pd.Timedelta(days=lag_days)
    
    # Limit recursion to 4 weeks to avoid infinite loops
    for _ in range(4):
        if current_lag_date.date() not in holiday_dates:
            return current_lag_date
        current_lag_date -= pd.Timedelta(days=7)
        
    return current_lag_date # Fallback if everything is a holiday (unlikely for 4 weeks)

