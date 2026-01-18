import pandas as pd
import numpy as np
import holidays
import os
import sqlite3
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_percentage_error
import warnings
import sys

# Add src to path if run directly
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.config import DB_PATH, FORECAST_MODEL_PATH

warnings.filterwarnings('ignore')

# 1. 加载数据 (From DB)
print("Loading data from SQLite (traffic_full)...")
conn = sqlite3.connect(DB_PATH)
df = pd.read_sql("SELECT * FROM traffic_full", conn)
conn.close()
df['ds'] = pd.to_datetime(df['date'])
df['y'] = df['throughput']
df = df.sort_values('ds').reset_index(drop=True)

# [NEW] Load Flight Stats (OpenSky) & Weather
try:
    print("Loading Weather & Flight Stats from SQLite...")
    import sqlite3
    conn_extra = sqlite3.connect("tsa_data.db", timeout=30)
    
    # 1. Flights
    df_flights = pd.read_sql("SELECT date, SUM(arrival_count) as total_flights FROM flight_stats GROUP BY date", conn_extra)
    df_flights['date'] = pd.to_datetime(df_flights['date'])
    
    # 2. Weather
    df_weather = pd.read_sql("SELECT date, index_value as weather_index FROM daily_weather_index", conn_extra)
    df_weather['date'] = pd.to_datetime(df_weather['date'])
    
    conn_extra.close()
    
    # Merge Flights
    df = df.merge(df_flights, left_on='ds', right_on='date', how='left')
    df.drop(columns=['date_y'], inplace=True, errors='ignore') 
    df.rename(columns={'date_x': 'date'}, inplace=True, errors='ignore')
    
    # Merge Weather
    if 'weather_index' not in df.columns:
        print("   Merging weather_index from daily_weather_index...")
        df = df.merge(df_weather, left_on='ds', right_on='date', how='left')
        df.drop(columns=['date_y'], inplace=True, errors='ignore')
        df.rename(columns={'date_x': 'date'}, inplace=True, errors='ignore')
    
    if 'weather_index' in df.columns:
        df['weather_index'] = df['weather_index'].fillna(0).astype(int)
    else:
        print("   WARNING: weather_index missing after merge! Forcing 0.")
        df['weather_index'] = 0

    # [OPTIMIZATION] Forward Fill
    df['total_flights'] = df['total_flights'].fillna(method='ffill').fillna(0)
    
    print(f"   Merged {len(df_flights)} flight records.")
except Exception as e:
    print(f"   WARNING: Could not load flight stats/weather: {e}")
    if 'total_flights' not in df.columns: df['total_flights'] = 0
    if 'weather_index' not in df.columns: df['weather_index'] = 0
print("Generating features for XGBoost...")

# A. 时间特征 (Time Components)
df['day_of_week'] = df['ds'].dt.dayofweek
df['month'] = df['ds'].dt.month
df['year'] = df['ds'].dt.year
df['day_of_year'] = df['ds'].dt.dayofyear
df['week_of_year'] = df['ds'].dt.isocalendar().week.astype(int)
df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)

# B. 滞后特征 (Lag Features)
# 注意: XGBoost 需要每一行都有完整的特征值
df['lag_7'] = df['throughput_lag_7']
df['lag_364'] = df['y'].shift(364)

# C. 业务特征 (Business Logic)
# [New] The "Off-Peak Workday" feature we created for Prophet
match_month = df['ds'].dt.month.isin([1, 2, 9, 10])
match_day = df['ds'].dt.dayofweek.isin([1, 2]) # Tue, Wed
df['is_off_peak_workday'] = (match_month & match_day).astype(int)

# [NEW] Flight# C. 历史滞后特征
df['y_lag_7'] = df['y'].shift(7).fillna(method='bfill')

# [NEW] Hybrid Lag Strategy (Aligned Seasonality)
# Baseline: Shift 364 days (matches Day of Week)
df['lag_364'] = df['y'].shift(364).fillna(method='bfill')

# Correction: For Fixed-Date Holidays, Day-of-Week matching is wrong.
# If today is Christmas (Dec 25), we want compare to last Dec 25 (Lag 365/366), not last year's same DOW.
# Vectorized implementation using numpy as requested.
import numpy as np

# Create a 'lag_365' for fixed comparison
df['lag_365'] = df['y'].shift(365).fillna(method='bfill')

# If is_holiday_exact_day == 1, use lag_365. Else, use lag_364.
# Note: This is a simplification. Ideally check if it's a "Floating Holiday" (MLK) vs "Fixed" (Xmas).
# But for now, 'is_holiday_exact_day' roughly captures major peaks where date matters more?
# Actually MLK is DOW based. Christmas is Date based.
# Let's refine: Only apply Hybrid Lag to [New Year, Independence, Veterans, Christmas, Good Friday?]
# Good Friday is relative. 
# Fixed Dates: New Year's (Jan 1), Independence (Jul 4), Veterans (Nov 11), Christmas (Dec 25).

fixed_date_holidays = [
    "New Year's Day", 
    "Independence Day", 
    "Veterans Day", 
    "Christmas Day"
]

# Identify rows that are Fixed Date Holidays
# We need to reconstruct the holiday name lookup or use date properties
# Re-using the 'us_holidays' dict loop from earlier is inefficient here.
# Let's use date matching.
df['is_fixed_holiday'] = 0
# Very specific mask construction
mask_fixed = df['ds'].apply(lambda d: 
    (d.month == 1 and d.day == 1) or
    (d.month == 7 and d.day == 4) or
    (d.month == 11 and d.day == 11) or
    (d.month == 12 and d.day == 25)
)
df.loc[mask_fixed, 'is_fixed_holiday'] = 1

# Apply Vectorized Swap
# If Fixed Holiday: Use Lag 365 (Alignment by Date)
# Else: Use Lag 364 (Alignment by Week)
df['lag_364'] = np.where(df['is_fixed_holiday'] == 1, df['lag_365'], df['lag_364'])

# Clean up temp cols
df.drop(columns=['lag_365', 'is_fixed_holiday'], inplace=True)

df['throughput_lag_7'] = df['y_lag_7'] # Alias for consistency
df['flight_lag_1'] = df['total_flights'].shift(1).fillna(method='bfill')
df['flight_ma_7'] = df['total_flights'].rolling(window=7).mean().shift(1).fillna(method='bfill')

# [NEW] Whitelist & Clamping Logic for Historical Data
print("   Calculating holiday distances for training data (12 Major Holidays + Good Friday)...")
us_holidays = holidays.US(years=range(2019, 2030))

# 12 Major Holidays Whitelist
target_holidays = [
    "New Year's Day", 
    "Martin Luther King Jr. Day", 
    "Washington's Birthday", # Presidents' Day
    "Memorial Day", 
    "Juneteenth National Independence Day", 
    "Independence Day", 
    "Labor Day", 
    "Columbus Day", 
    "Veterans Day", 
    "Thanksgiving", 
    "Christmas Day"
]

major_holiday_dates = []

# 1. Add Standard Federal Holidays
for date, name in us_holidays.items():
    if any(target in name for target in target_holidays):
        major_holiday_dates.append(pd.Timestamp(date))

# 2. Add Good Friday (Easter - 2 days) - Manual Calculation
from dateutil.easter import easter
for y in range(2019, 2030):
    easter_date = easter(y)
    good_friday = pd.Timestamp(easter_date) - pd.Timedelta(days=2)
    major_holiday_dates.append(good_friday)

# Remove duplicates if any and sort
major_holiday_dates = sorted(list(set(major_holiday_dates)))

df['days_to_nearest_holiday'] = 15 # Default
for idx, row in df.iterrows():
    d = row['ds']
    min_dist = 999 
    best_dist = 15
    for h_date in major_holiday_dates:
        diff_days = (d - h_date).days
        if abs(diff_days) < abs(min_dist):
            min_dist = diff_days
            best_dist = diff_days
    
    # Clamp logic: +/- 14 window. If outside, clamp to +/- 15
    if best_dist > 14: best_dist = 15
    elif best_dist < -14: best_dist = -15
    df.at[idx, 'days_to_nearest_holiday'] = best_dist

# [NEW] Weather Rebound Logic (Lag 1)
# Capture "Backlog" effect: Yesterday Bad -> Today Rebound
df['weather_lag_1'] = df['weather_index'].shift(1).fillna(0) # Assume good weather if unknown

# [NEW] Long Weekend Logic
# 0=Mon, 4=Fri. (Strict definition for "Long Weekend" anchors)
df['is_long_weekend'] = 0
# Logic: If it is a holiday AND it is Mon/Fri
mask_long = (df['is_holiday'] == 1) & (df['day_of_week'].isin([0, 4]))
df.loc[mask_long, 'is_long_weekend'] = 1

# D. 填充缺失值
features = [
    'day_of_week', 'month', 'year', 'day_of_year', 'week_of_year', 'is_weekend',
    'weather_index', 'is_holiday', 'is_spring_break', 'is_off_peak_workday',
    'is_holiday_exact_day', 'days_to_nearest_holiday',
    'weather_lag_1', 'is_long_weekend',
    'lag_7', 'lag_364', 'flight_lag_1', 'flight_ma_7'
]

# 丢弃无法计算 lag_364 的早期数据
df_model = df.dropna(subset=['lag_364']).copy()
for col in features:
    df_model[col] = df_model[col].fillna(0)

# 3. 划分训练集与测试集 (Backtest Strategy)
# 训练集: < 2026-01-01 且 剔除疫情 (2020-03-01 ~ 2021-12-31)
pandemic_start = pd.Timestamp('2020-03-01')
pandemic_end = pd.Timestamp('2021-12-31')
train_cutoff = pd.Timestamp('2025-12-31')

mask_train_period = (df_model['ds'] <= train_cutoff)
mask_pandemic = (df_model['ds'] >= pandemic_start) & (df_model['ds'] <= pandemic_end)

train_df = df_model[mask_train_period & (~mask_pandemic)]

# 测试集: 2026-01-01 ~ 2026-01-13
test_start = pd.Timestamp('2026-01-01')
test_end = pd.Timestamp('2026-01-13')
test_df = df_model[(df_model['ds'] >= test_start) & (df_model['ds'] <= test_end)].copy()

X_train = train_df[features]
y_train = train_df['y']

X_test = test_df[features]
y_test = test_df['y']

print(f"Training XGBoost on {len(X_train)} rows...")
print(f"Testing on {len(X_test)} rows ({test_start.date()} ~ {test_end.date()})")

# 4. 训练模型 (The Beast)
model = XGBRegressor(
    n_estimators=1000,
    learning_rate=0.05,
    max_depth=5,
    subsample=0.8,
    colsample_bytree=0.8,
    n_jobs=-1,
    random_state=42
)

model.fit(X_train, y_train)

# 5. 预测与评估
y_pred = model.predict(X_test)

test_df['yhat_xgb'] = y_pred
test_df['diff'] = test_df['yhat_xgb'] - test_df['y']
test_df['error_pct'] = (test_df['diff'].abs() / test_df['y']) * 100

mape = test_df['error_pct'].mean()

# ...
# ...
# 6. 生成报告
report_lines = []
report_lines.append("\n" + "="*60)
report_lines.append(f"🥊 XGBOOST BACKTEST REPORT (MAPE: {mape:.2f}%)")
report_lines.append("="*60)
report_lines.append(f"{'Date':<12} | {'Actual':<10} | {'XGBoost':<10} | {'Diff':<10} | {'Error %':<8}")
report_lines.append("-" * 60)

for _, row in test_df.iterrows():
    d_str = row['ds'].strftime('%Y-%m-%d')
    act = int(row['y'])
    pred = int(row['yhat_xgb'])
    diff = int(row['diff'])
    err = row['error_pct']
    
    flag = "⚠️" if err > 5.0 else "✅"
    report_lines.append(f"{d_str} | {act:<10,} | {pred:<10,} | {diff:<10,} | {err:>6.2f}% {flag}")

report_lines.append("-" * 60)

# [NEW] Save Validation Results to CSV for Frontend
validation_df = test_df[['ds', 'y', 'yhat_xgb', 'diff', 'error_pct']].rename(columns={
    'ds': 'date', 
    'y': 'actual', 
    'yhat_xgb': 'predicted',
    'diff': 'difference',
    'error_pct': 'error_rate'
})
validation_df.to_csv("xgb_validation.csv", index=False)
print("Validation results saved to xgb_validation.csv")

with open("xgb_report.txt", "w", encoding="utf-8") as f:
    pred = int(row['yhat_xgb'])
    diff = int(row['diff'])
    err = row['error_pct']
    
    flag = "⚠️" if err > 5.0 else "✅"
    report_lines.append(f"{d_str} | {act:<10,} | {pred:<10,} | {diff:<10,} | {err:>6.2f}% {flag}")

report_lines.append("-" * 60)

# 特征重要性
report_lines.append("\n[Feature Importance]")
importances = model.feature_importances_
indices = np.argsort(importances)[::-1]
for i in range(len(features)):
    feat_name = features[indices[i]]
    feat_imp = importances[indices[i]]
    report_lines.append(f"{i+1}. {feat_name:<20}: {feat_imp:.4f}")

with open("xgb_report.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(report_lines))

# ... (Backtest code remains)

print(f"XGBoost finished. MAPE: {mape:.2f}%")
print("Report saved to xgb_report.txt")

# ==========================================
# 7. 部署模式: 预测未来 5 天 (Production Forecast)
# ==========================================
print("\n[FORECAST] Generating Future Forecast (Next 5 Days)...")

# ==========================================
print("\n[FORECAST] Generating Future Forecast (Next 7 Days)...")

# [CRITICAL UPDATE] Retrain on FULL DATA (Including Jan 2026)
# 为了预测明天，我们需要利用直到昨天的最新数据，而不是停留在 2025 年底
print("   [RETRAIN] Retraining model on ALL available history (2019-Present)...")
mask_full_train = (~mask_pandemic) & (df_model['y'].notnull())
full_train_df = df_model[mask_full_train]

X_full = full_train_df[features]
y_full = full_train_df['y']

model_full = XGBRegressor(
    n_estimators=1200, # Slightly more robust
    learning_rate=0.05,
    max_depth=5,
    subsample=0.8,
    colsample_bytree=0.8,
    n_jobs=-1,
    random_state=42
)
model_full.fit(X_full, y_full)
print(f"   Full Model Trained on {len(full_train_df)} rows.")
    
# [NEW] Save Model for Sniper (Persistence)
print(f"   [PERSISTENCE] Saving forecast model to {FORECAST_MODEL_PATH}...")
model_full.save_model(FORECAST_MODEL_PATH)
print("   [PERSISTENCE] Model saved successfully.")

# 关键修正: 找到最后一条"真实有数据"的日期 (忽略未来骨架)
last_actual_row = df[df['y'].notnull()].iloc[-1]
last_actual_date = last_actual_row['ds']
print(f"Last Actual Data Date: {last_actual_date.date()}")

# 从"有数据"的后一天开始预测
print("   [DEBUG] STARTING FORECAST GENERATION PHASE")
try:
    # [FIX] Ensure we cover enough future days even if data is stale
    print("   [DEBUG] Generating future_dates...")
    future_dates = pd.date_range(start=last_actual_date + pd.Timedelta(days=1), periods=14)
    print(f"   [DEBUG] Generated {len(future_dates)} days: {future_dates[0]} to {future_dates[-1]}")

    # 构建未来特征 DataFrame
    future_df = pd.DataFrame({'ds': future_dates})

    # ... (Feature Engineering continues)

    # A. 时间特征
    print("   [DEBUG] Generating Time Features...")
    future_df['day_of_week'] = future_df['ds'].dt.dayofweek
    future_df['month'] = future_df['ds'].dt.month
    future_df['year'] = future_df['ds'].dt.year
    future_df['day_of_year'] = future_df['ds'].dt.dayofyear
    future_df['week_of_year'] = future_df['ds'].dt.isocalendar().week.astype(int)
    future_df['is_weekend'] = future_df['day_of_week'].isin([5, 6]).astype(int)

    # B. 滞后特征 (Lags)
    # 对于 Lag-7，我们需要过去 7 天的数据
    # 对于 Lag-364，我们需要去年的数据
    # 注意: 这里简化处理，直接从历史数据 df 中查找对应日期的值
    def get_lag_value(target_date, lag_days):
        past_date = target_date - pd.Timedelta(days=lag_days)
        # 在 df 中查找 (如果 df 没有，可能需要递归预测? 简单起见假设 df 足够长)
        row = df[df['ds'] == past_date]
        if not row.empty:
            return row.iloc[0]['y']
        else:
            return 0 # Fallback

    print("   [DEBUG] Generating Lag Features...")
    future_df['lag_7'] = future_df['ds'].apply(lambda x: get_lag_value(x, 7))
    future_df['lag_364'] = future_df['ds'].apply(lambda x: get_lag_value(x, 364))
    print("   [DEBUG] Lag Features Done.")
except Exception as e:
    print(f"   [CRITICAL ERROR] Forecast Generation Failed: {e}")
    import traceback
    traceback.print_exc()
    raise e # Re-raise to crash properly

# C. 业务特征
match_month = future_df['ds'].dt.month.isin([1, 2, 9, 10])
match_day = future_df['ds'].dt.dayofweek.isin([1, 2])
future_df['is_off_peak_workday'] = (match_month & match_day).astype(int)

# D. 外部特征 (Real Holiday Logic)
print("   Generating Future Holiday Features...")
us_holidays = holidays.US(years=[2026])

# [NEW] Whitelist & Clamping Logic (User Requested)
# Only calculate distance to these specific MAJOR holidays
target_holidays = [
    "New Year's Day", 
    "Martin Luther King Jr. Day", 
    "Washington's Birthday", # Presidents' Day
    "Memorial Day", 
    "Juneteenth National Independence Day", 
    "Independence Day", 
    "Labor Day", 
    "Columbus Day", 
    "Veterans Day", 
    "Thanksgiving", 
    "Christmas Day"
]

# Pre-filter holiday dates for performance and strictness
major_holiday_dates = []

# 1. Standard Holidays
for date, name in us_holidays.items():
    if any(target in name for target in target_holidays):
        major_holiday_dates.append(pd.Timestamp(date))

# 2. Add Good Friday for Forecast Year(s)
# future_df spans small range, but safely add for current year
from dateutil.easter import easter
# Calculate for relevant years in filtered range (e.g., this year and next)
unique_years = future_df['ds'].dt.year.unique()
for y in unique_years:
    easter_date = easter(y)
    good_friday = pd.Timestamp(easter_date) - pd.Timedelta(days=2)
    major_holiday_dates.append(good_friday)

major_holiday_dates = sorted(list(set(major_holiday_dates)))
        
future_df['is_holiday'] = 0
future_df['is_holiday_exact_day'] = 0
future_df['holiday_name'] = None # [NEW]
future_df['days_to_nearest_holiday'] = 15 # Default to "Far Away" (Clamped Max)

# [FIX] Ensure holidays object covers the future period explicitly
us_holidays_future = holidays.US(years=range(2024, 2030))

for idx, row in future_df.iterrows():
    d = row['ds']
    # Robust date conversion
    d_date = d.date() if hasattr(d, 'date') else pd.to_datetime(d).date()
    
    # 1. Exact Holiday Flag
    if d_date in us_holidays_future:
        name = us_holidays_future.get(d_date)
        future_df.at[idx, 'is_holiday'] = 1
        future_df.at[idx, 'holiday_name'] = name # [NEW] Store name
        
        if any(m in name for m in target_holidays):
            future_df.at[idx, 'is_holiday_exact_day'] = 1
            
        print(f"   [DEBUG] Found Holiday: {d_date} (Type: {type(d_date)}) -> Name: {name}")
    else:
        # Debug why Jan 19 2026 is missed
        if str(d_date) == '2026-01-19':
             print(f"   [DEBUG_MISS] 2026-01-19 check failed! d_date type: {type(d_date)}, In keys? {d_date in us_holidays_future}")
    
    # Check Good Friday for exact flag too? 
    # Good Friday is not in us_holidays usually. Let's manually check.
    
    # Check Good Friday for exact flag too? 
    # Good Friday is not in us_holidays usually. Let's manually check.
    if d in major_holiday_dates:
        # Check if this date corresponds to a non-federal holiday like Good Friday
        if d_date not in us_holidays:
            # Manually set is_holiday for Good Friday
            future_df.at[idx, 'is_holiday'] = 1
            # It's a target holiday
            future_df.at[idx, 'is_holiday_exact_day'] = 1

    # 2. Continuous Distance Feature (Clamped +/- 15)
    min_dist = 999 
    best_dist = 15 
        
    for h_date in major_holiday_dates:
        diff_days = (d - h_date).days
        if abs(diff_days) < abs(min_dist):
            min_dist = diff_days
            best_dist = diff_days
            
    if best_dist > 14: best_dist = 15
    elif best_dist < -14: best_dist = -15
        
    future_df.at[idx, 'days_to_nearest_holiday'] = best_dist
        

    
    # [NEW] Long Weekend
    is_long = 0
    w_dow = row['ds'].dayofweek
    if future_df.at[idx, 'is_holiday'] == 1 and w_dow in [0, 4]:
        is_long = 1
    future_df.at[idx, 'is_long_weekend'] = is_long

# [FIX] Load Real Weather Forecast (From SQLite)
print("   Merging Real Weather Forecast (from DB)...")
try:
    conn_w = sqlite3.connect("tsa_data.db")
    df_weather = pd.read_sql("SELECT date, weather_index FROM daily_weather_index", conn_w)
    conn_w.close()
    
    df_weather['date'] = pd.to_datetime(df_weather['date'])
    # Merge weather_index
    future_df = future_df.merge(df_weather[['date', 'weather_index']], left_on='ds', right_on='date', how='left')
    future_df['weather_index'] = future_df['weather_index'].fillna(0).astype(int)
    # Drop temp col
    if 'date' in future_df.columns:
        future_df.drop(columns=['date'], inplace=True)
    print("   Weather features merged.")
except Exception as e:
    print(f"   WARNING: Failed to load weather features ({e}). Defaulting to 0.")
    future_df['weather_index'] = 0

# [NEW] Calculate Weather Lag 1 (Post-Merge)
future_df['weather_lag_1'] = future_df['weather_index'].shift(1).fillna(0)
# Fix the first row using historical data
try:
    if len(df) > 0:
        last_hist_weather = df.iloc[-1]['weather_index']
        future_df.at[0, 'weather_lag_1'] = last_hist_weather
except Exception as e:
    print(f"   Warning: Could not patch first row weather lag: {e}")

# [NEW] Flight Stats Integration (OpenSky)
print("   Merging OpenSky Flight Stats...")
try:
    import sqlite3
    conn = sqlite3.connect("tsa_data.db")
    # Query flight stats aggregated by date
    flight_query = """
        SELECT date, SUM(arrival_count) as total_flights
        FROM flight_stats
        GROUP BY date
    """
    df_flights = pd.read_sql(flight_query, conn)
    conn.close()
    
    df_flights['date'] = pd.to_datetime(df_flights['date'])
    
    # Merge into future_df (needs lag)
    # Strategy: We need historical flight data to calculate valid lags.
    # Since future_df is only 7 days, we might miss the context for rolling windows if we just merge left.
    # Ideally, we should have merged this upstream into 'df' (historical) and 'future_df' (forecast).
    
    # Let's do a quick lookup helper since we are in the flow
    # But wait, looking at the code above, we already processed 'df' (History) at the top. 
    # We should have merged it there!
    # TO FIX: We will do a "Patch" here for future_df, 
    # but strictly speaking we missed adding it to the TRAINING data 'df'.
    
    pass 
except Exception as e:
    print(f"   WARNING: Failed to load flight stats ({e})")

# [FIX] Real Spring Break Logic (March/April + Weekend + Not Holiday)
# Logic: Month is 3 or 4, Weekend, Not Holiday
future_df['is_spring_break'] = 0
mask_sb = (future_df['ds'].dt.month.isin([3, 4])) & \
          (future_df['ds'].dt.dayofweek.isin([5, 6])) & \
          (future_df['is_holiday'] == 0)
future_df.loc[mask_sb, 'is_spring_break'] = 1

# [NEW] Generate Future Flight Features
# Since we don't know future flights, we use the last known values or recent average.
# Simplest approach: Use the last known flight_ma_7 from the historical 'df'
last_known_ma7 = df.iloc[-1]['flight_ma_7']
last_known_lag1 = df.iloc[-1]['total_flights'] # Use today's flights as tomorrow's lag1

future_df['flight_ma_7'] = last_known_ma7
future_df['flight_lag_1'] = last_known_lag1 # Assume constant capacity for short term

# Refinement: If we have partial future data in flight_stats (unlikely for future), use it.
# But generally we assume persistence for T+7 forecast.

# E. 预测
X_future = future_df[features]
y_future_pred = model_full.predict(X_future)

future_df['predicted_throughput'] = y_future_pred.astype(int)

# 保存预测结果
future_df[['ds', 'predicted_throughput']].to_csv("xgb_forecast.csv", index=False)

print("\n[FORECAST RESULTS] Future Forecast:")
print(future_df[['ds', 'predicted_throughput']].to_string(index=False))

# [NEW] Save to Persistent History Log (SQLite)
history_db = "tsa_data.db"
today = pd.Timestamp.now().strftime('%Y-%m-%d')

# Prepare new rows
# Prepare new rows
new_log = future_df[['ds', 'predicted_throughput', 'weather_index', 'is_holiday', 'flight_lag_1', 'is_weekend', 'holiday_name']].copy()
new_log.columns = ['target_date', 'predicted_throughput', 'weather_index', 'is_holiday', 'flight_volume', 'is_weekend', 'holiday_name']
new_log['model_run_date'] = today

# [FIX] Ensure date columns are strings
new_log['target_date'] = new_log['target_date'].dt.strftime('%Y-%m-%d')
new_log['model_run_date'] = str(today)

try:
    import sqlite3
    conn = sqlite3.connect(history_db)
    cursor = conn.cursor()
    
    # 1. Remove existing entries for same forecast produced today/future to avoid duplicates
    # Strategy: Delete any existing prediction for this target_date made on this model_run_date
    # Or just simple insert? Let's do simple insert for log, app.py will filter latest.
    
    # Actually, to prevent exploding table size during debugging, let's delete if exists same run
    dt_list = new_log['target_date'].tolist()
    cursor.executemany("DELETE FROM prediction_history WHERE target_date = ? AND model_run_date = ?", 
                       [(d, today) for d in dt_list])
    
    # [FIX] Foolproof type conversion
    records = []
    for _, row in new_log.iterrows():
        records.append({
            'target_date': str(row['target_date']),
            'predicted_throughput': int(row['predicted_throughput']),
            'model_run_date': str(row['model_run_date']),
            'weather_index': int(row.get('weather_index', 0)),
            'is_holiday': int(row.get('is_holiday', 0)),
            'flight_volume': int(row.get('flight_lag_1', 0)), # Use lag_1 as proxy for volume known at prediction time
            'is_weekend': int(row.get('is_weekend', 0))
        })
    
    cursor.executemany('''
        INSERT INTO prediction_history (
            target_date, predicted_throughput, model_run_date, 
            weather_index, is_holiday, flight_volume, is_weekend
        )
        VALUES (
            :target_date, :predicted_throughput, :model_run_date,
            :weather_index, :is_holiday, :flight_volume, :is_weekend
        )
    ''', records)
    
    conn.commit()
    conn.close()
    print(f"Forecast logged to {history_db} (table: prediction_history) for future verification.")
    
except Exception as e:
    print(f"ERROR logging to database: {e}")
    # Fallback to CSV if DB fails?
    try:
        if not os.path.exists("prediction_history.csv"):
            new_log.to_csv("prediction_history.csv", index=False)
        else:
            new_log.to_csv("prediction_history.csv", mode='a', header=False, index=False)
        print("Fallback: Logged to CSV.")
    except:
        pass
