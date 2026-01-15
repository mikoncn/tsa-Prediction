import pandas as pd
import numpy as np
import holidays
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_percentage_error
import warnings
warnings.filterwarnings('ignore')

# 1. 加载数据
print("Loading data...")
df = pd.read_csv("TSA_Final_Analysis.csv")
df['ds'] = pd.to_datetime(df['date'])
df['y'] = df['throughput']
df = df.sort_values('ds').reset_index(drop=True)

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

# D. 填充缺失值
features = [
    'day_of_week', 'month', 'year', 'day_of_year', 'week_of_year', 'is_weekend',
    'weather_index', 'is_holiday', 'is_spring_break', 'is_off_peak_workday',
    'is_holiday_exact_day', 'is_holiday_travel_window',
    'lag_7', 'lag_364'
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
print("\n🔮 Generating Future Forecast (Next 5 Days)...")

# ==========================================
# 7. 部署模式: 预测未来 7 天 (Production Forecast)
# ==========================================
print("\n🔮 Generating Future Forecast (Next 7 Days)...")

# 关键修正: 找到最后一条"真实有数据"的日期 (忽略未来骨架)
last_actual_row = df[df['y'].notnull()].iloc[-1]
last_actual_date = last_actual_row['ds']
print(f"Last Actual Data Date: {last_actual_date.date()}")

# 从"有数据"的后一天开始预测
future_dates = pd.date_range(start=last_actual_date + pd.Timedelta(days=1), periods=7)

# 构建未来特征 DataFrame
future_df = pd.DataFrame({'ds': future_dates})

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

future_df['weather_index'] = 0 
future_df['is_spring_break'] = 0 

# E. 预测
X_future = future_df[features]
y_future_pred = model.predict(X_future)

future_df['predicted_throughput'] = y_future_pred.astype(int)

# 保存预测结果
future_df[['ds', 'predicted_throughput']].to_csv("xgb_forecast.csv", index=False)

print("\n📅 Future Forecast:")
print(future_df[['ds', 'predicted_throughput']].to_string(index=False))
