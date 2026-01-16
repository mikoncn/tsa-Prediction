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
from src.config import DB_PATH

warnings.filterwarnings('ignore')

# 1. 加载数据 (From DB)
print("Loading data from SQLite (traffic_full)...")
conn = sqlite3.connect(DB_PATH)
df = pd.read_sql("SELECT * FROM traffic_full", conn)
conn.close()
df['ds'] = pd.to_datetime(df['date'])
df['y'] = df['throughput']
df = df.sort_values('ds').reset_index(drop=True)

# [NEW] Load Flight Stats (OpenSky)
try:
    print("Loading Flight Stats from SQLite...")
    import sqlite3
    conn_flights = sqlite3.connect("tsa_data.db", timeout=30)
    df_flights = pd.read_sql("SELECT date, SUM(arrival_count) as total_flights FROM flight_stats GROUP BY date", conn_flights)
    conn_flights.close()
    
    df_flights['date'] = pd.to_datetime(df_flights['date'])
    df = df.merge(df_flights, left_on='ds', right_on='date', how='left')
    df.drop(columns=['date_y'], inplace=True, errors='ignore') # cleanup
    df.rename(columns={'date_x': 'date'}, inplace=True, errors='ignore')
    
    # [OPTIMIZATION] Use Forward Fill (Forward Persistence) instead of Mean
    # Logic: If data is missing (e.g. API outage), assume flights are same as yesterday.
    df['total_flights'] = df['total_flights'].fillna(method='ffill')
    # If beginning is still NaN (no yesterday), fallback to 0
    df['total_flights'] = df['total_flights'].fillna(0)
    
    print(f"   Merged {len(df_flights)} flight records.")
except Exception as e:
    print(f"   WARNING: Could not load flight stats: {e}")
    df['total_flights'] = 0

# 2. 特征工程 (Feature Engineering)
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

# [NEW] Flight Features
df['flight_lag_1'] = df['total_flights'].shift(1).fillna(method='bfill')
df['flight_ma_7'] = df['total_flights'].rolling(window=7, min_periods=1).mean().shift(1).fillna(method='bfill')

# D. 填充缺失值
features = [
    'day_of_week', 'month', 'year', 'day_of_year', 'week_of_year', 'is_weekend',
    'weather_index', 'is_holiday', 'is_spring_break', 'is_off_peak_workday',
    'is_holiday_exact_day', 'is_holiday_travel_window',
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
print("   [PERSISTENCE] Saving model to sniper_model.json...")
model_full.save_model("sniper_model.json")
print("   [PERSISTENCE] Model saved successfully.")

# 关键修正: 找到最后一条"真实有数据"的日期 (忽略未来骨架)
last_actual_row = df[df['y'].notnull()].iloc[-1]
last_actual_date = last_actual_row['ds']
print(f"Last Actual Data Date: {last_actual_date.date()}")

# 从"有数据"的后一天开始预测
future_dates = pd.date_range(start=last_actual_date + pd.Timedelta(days=1), periods=7)

# 构建未来特征 DataFrame
future_df = pd.DataFrame({'ds': future_dates})

# ... (Feature Engineering continues)

# A. 时间特征
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

future_df['lag_7'] = future_df['ds'].apply(lambda x: get_lag_value(x, 7))
future_df['lag_364'] = future_df['ds'].apply(lambda x: get_lag_value(x, 364))

# C. 业务特征
match_month = future_df['ds'].dt.month.isin([1, 2, 9, 10])
match_day = future_df['ds'].dt.dayofweek.isin([1, 2])
future_df['is_off_peak_workday'] = (match_month & match_day).astype(int)

# D. 外部特征 (Real Holiday Logic)
print("   Generating Future Holiday Features...")
us_holidays = holidays.US(years=[2026])
major_holidays = ['Christmas Day', 'Thanksgiving', 'Independence Day']

future_df['is_holiday'] = 0
future_df['is_holiday_exact_day'] = 0
future_df['is_holiday_travel_window'] = 0

for idx, row in future_df.iterrows():
    d = row['ds']
    # Check if exact day
    if d in us_holidays:
        name = us_holidays.get(d)
        future_df.at[idx, 'is_holiday'] = 1
        if any(m in name for m in major_holidays):
            future_df.at[idx, 'is_holiday_exact_day'] = 1
            
    # Check window (Naive looping for simplicity on small set)
    for h_date, h_name in us_holidays.items():
        h_date = pd.Timestamp(h_date)
        if any(m in h_name for m in major_holidays):
            days_diff = (d - h_date).days
            if abs(days_diff) <= 7 and days_diff != 0:
                 future_df.at[idx, 'is_holiday_travel_window'] = 1

# [FIX] Load Real Weather Forecast
print("   Merging Real Weather Forecast...")
try:
    df_weather = pd.read_csv("weather_features.csv")
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
new_log = future_df[['ds', 'predicted_throughput']].copy()
new_log.columns = ['target_date', 'predicted_throughput']
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
            'model_run_date': str(row['model_run_date'])
        })
    
    cursor.executemany('''
        INSERT INTO prediction_history (target_date, predicted_throughput, model_run_date)
        VALUES (:target_date, :predicted_throughput, :model_run_date)
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
