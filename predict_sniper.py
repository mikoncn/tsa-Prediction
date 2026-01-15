import pandas as pd
import numpy as np
import sqlite3
import warnings
import sys
import json
from xgboost import XGBRegressor
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')

DB_PATH = 'tsa_data.db'
CSV_PATH = 'TSA_Final_Analysis.csv'

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def load_data():
    # 1. Load Core TSA Data
    df = pd.read_csv(CSV_PATH)
    df['ds'] = pd.to_datetime(df['date'])
    df['y'] = df['throughput']
    df = df.sort_values('ds').reset_index(drop=True)
    
    # 2. Load Flight Data (Same Day Match)
    conn = get_db_connection()
    df_flights = pd.read_sql("SELECT date, SUM(arrival_count) as total_flights FROM flight_stats GROUP BY date", conn)
    conn.close()
    
    df_flights['date'] = pd.to_datetime(df_flights['date'])
    
    # 3. Merge (Same Day!)
    df = df.merge(df_flights, left_on='ds', right_on='date', how='left')
    
    # Fill NA
    avg_flights = df[df['total_flights'] > 0]['total_flights'].mean()
    if pd.isna(avg_flights): avg_flights = 0
    df['total_flights'] = df['total_flights'].fillna(avg_flights)
    
    return df

def train_and_predict(target_date_str):
    df = load_data()
    
    # Feature Engineering (Simplified for Speed)
    df['day_of_week'] = df['ds'].dt.dayofweek
    df['month'] = df['ds'].dt.month
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    
    # Core Feature: Same Day Flights
    df['flight_current'] = df['total_flights'] 
    
    # Lag Features (Still useful)
    df['lag_7'] = df['y'].shift(7).fillna(method='bfill')
    df['lag_364'] = df['y'].shift(364).fillna(method='bfill')
    
    # [NEW] Sync with Full Feature Set from train_xgb.py
    features = [
        'day_of_week', 'month', 'is_weekend', 'flight_current', 
        'weather_index', 'is_holiday', 'is_spring_break',
        'is_holiday_exact_day', 'is_holiday_travel_window',
        'lag_7', 'lag_364'
    ]
    
    # Train Mask
    # Exclude Pandemic
    mask_pandemic = (df['ds'] >= '2020-03-01') & (df['ds'] <= '2021-12-31')
    
    train_df = df[(~mask_pandemic) & (df['y'].notnull())].copy()
    
    # Fill NA for features within model df
    for col in features:
        train_df[col] = train_df[col].fillna(0)
    
    X_train = train_df[features]
    y_train = train_df['y']
    
    # Train Model (Fast)
    model = XGBRegressor(
        n_estimators=500, # Faster
        learning_rate=0.05,
        max_depth=5,
        n_jobs=-1,
        random_state=42
    )
    model.fit(X_train, y_train)
    
    # Prepare Target Input
    target_date = pd.to_datetime(target_date_str)
    
    # Get Static Features for Target Date from the master DataFrame
    target_row = df[df['ds'] == target_date]
    if target_row.empty:
        # Fallback if target date isn't in CSV
        weather_idx = 0
        is_h = 0
        is_sb = 0
        is_h_exact = 0
        is_h_window = 0
        lag_7_val = 0
        lag_364_val = 0
    else:
        target_data = target_row.iloc[0]
        weather_idx = target_data.get('weather_index', 0)
        is_h = target_data.get('is_holiday', 0)
        is_sb = target_data.get('is_spring_break', 0)
        is_h_exact = target_data.get('is_holiday_exact_day', 0)
        is_h_window = target_data.get('is_holiday_travel_window', 0)
        lag_7_val = target_data.get('throughput_lag_7', 0)
        # Lag 364 for input
        lag_364_val = df[df['ds'] == (target_date - timedelta(days=364))]['y'].values[0] if not df[df['ds'] == (target_date - timedelta(days=364))].empty else 0

    # 1. Calendar
    day_of_week = target_date.dayofweek
    month = target_date.month
    is_weekend = 1 if day_of_week >= 5 else 0
    
    # 2. Flight Data (Real-time from DB)
    conn = get_db_connection()
    row = conn.execute("SELECT SUM(arrival_count) FROM flight_stats WHERE date = ?", (target_date_str,)).fetchone()
    conn.close()
    
    flight_current = row[0] if row and row[0] else 0
    
    # If no flight data for target day (Sniper failure), fallback?
    is_fallback = False
    if flight_current == 0:
        # Calculate average from loaded DF for fallback
        avg_flights = df['total_flights'].mean()
        
        # Fallback to yesterday (Lag 1) or Average
        yesterday_str = (target_date - timedelta(days=1)).strftime("%Y-%m-%d")
        conn = get_db_connection()
        row_y = conn.execute("SELECT SUM(arrival_count) FROM flight_stats WHERE date = ?", (yesterday_str,)).fetchone()
        conn.close()
        flight_current = row_y[0] if row_y and row_y[0] else avg_flights
        is_fallback = True
        
    # Construct Input
    X_target = pd.DataFrame([{
        'day_of_week': day_of_week,
        'month': month,
        'is_weekend': is_weekend,
        'flight_current': flight_current,
        'weather_index': weather_idx,
        'is_holiday': is_h,
        'is_spring_break': is_sb,
        'is_holiday_exact_day': is_h_exact,
        'is_holiday_travel_window': is_h_window,
        'lag_7': lag_7_val,
        'lag_364': lag_364_val
    }])
    
    # Predict
    pred = model.predict(X_target)[0]
    
    return {
        "date": target_date_str,
        "predicted_throughput": int(pred),
        "flight_volume": int(flight_current),
        "is_fallback": is_fallback,
        "model": "Sniper V1"
    }

if __name__ == "__main__":
    if len(sys.argv) < 2:
        # Dynamic Target Logic based on OpenSky Data Availability
        # Rule: OpenSky batch updates complete around 10:00 AM Beijing Time.
        # - Before 10:00 AM: T-1 data is incomplete -> Use T-2.
        # - After 10:00 AM: T-1 data is ready -> Use T-1.
        
        now = datetime.now()
        hour_threshold = 10
        
        if now.hour < hour_threshold:
            # It's early morning (e.g., 5 AM). Yesterday's (T-1) data isn't fully baked.
            # Target T-2 (The day before yesterday).
            target_date = now - timedelta(days=2)
            print(f"   [Logic] Current time {now.strftime('%H:%M')} < 10:00. OpenSky T-1 incomplete.")
            print(f"   [Logic] Auto-switching target to T-2: {target_date.strftime('%Y-%m-%d')}")
        else:
            # It's after 10 AM. T-1 should be ready.
            target_date = now - timedelta(days=1)
            print(f"   [Logic] Current time {now.strftime('%H:%M')} >= 10:00. OpenSky T-1 ready.")
            print(f"   [Logic] Targeting T-1: {target_date.strftime('%Y-%m-%d')}")
            
        target = target_date.strftime("%Y-%m-%d")
    else:
        target = sys.argv[1]
        
    try:
        prediction = train_and_predict(target)
        
        # Ensure JSON output
        if isinstance(prediction, dict):
            print(json.dumps(prediction))
    except Exception as e:
        print(json.dumps({"error": str(e)}))
