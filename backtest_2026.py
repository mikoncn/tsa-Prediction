import pandas as pd
from prophet import Prophet
import numpy as np

# 1. 加载数据
print("Loading data...")
df = pd.read_csv("TSA_Final_Analysis.csv")
df['ds'] = pd.to_datetime(df['date'])
df['y'] = df['throughput']
df = df.sort_values('ds').reset_index(drop=True)

# 2. 特征工程
df['lag_7'] = df['throughput_lag_7']
df['lag_364'] = df['y'].shift(364)

# [NEW] Off-Peak Workday Bias Correction
# 逻辑: 淡季(1,2,9,10月) 的 周二/周三(dayofweek 1,2) 往往流量惨淡，模型容易高估
# 标记这些日子为 1，让模型学习"打折"
print("Generating bias correction features (Off-Peak Tue/Wed)...")
match_month = df['ds'].dt.month.isin([1, 2, 9, 10])
match_day = df['ds'].dt.dayofweek.isin([1, 2]) # 0=Mon, 1=Tue, 2=Wed
df['is_off_peak_workday'] = (match_month & match_day).astype(int)

df_model = df.dropna(subset=['lag_364']).copy()
for col in ['weather_index', 'is_holiday', 'is_spring_break', 'lag_7', 'lag_364', 'is_off_peak_workday']:
    df_model[col] = df_model[col].fillna(0)

# 3. 划分训练集与测试集
# 训练集: < 2026-01-01 且 剔除疫情 (2020-03-01 ~ 2021-12-31)
pandemic_start = '2020-03-01'
pandemic_end = '2021-12-31'
train_cutoff = '2025-12-31'

mask_train_period = (df_model['ds'] <= train_cutoff)
mask_pandemic = (df_model['ds'] >= pandemic_start) & (df_model['ds'] <= pandemic_end)

train_df = df_model[mask_train_period & (~mask_pandemic)]

# 测试集: 2026-01-01 ~ 2026-01-13
test_start = '2026-01-01'
test_end = '2026-01-13'
test_df = df_model[(df_model['ds'] >= test_start) & (df_model['ds'] <= test_end)].copy()

print(f"Training on {len(train_df)} rows (End: {train_df['ds'].max()})")
print(f"Testing on {len(test_df)} rows ({test_start} ~ {test_end})")

# 4. 训练模型
# [Hyperparameter Tuning] changepoint_prior_scale increased from 0.05 to 0.15
# 让模型更快适应 2025 年底到 2026 年初的趋势变化
m = Prophet(
    daily_seasonality=False,
    yearly_seasonality=True,
    weekly_seasonality=True,
    seasonality_mode='multiplicative',
    changepoint_prior_scale=0.15 
)
m.add_regressor('weather_index')
m.add_regressor('is_holiday')
m.add_regressor('is_spring_break')
m.add_regressor('lag_7')
m.add_regressor('lag_364')
m.add_regressor('is_off_peak_workday') # [NEW]

m.fit(train_df)

# 5. 预测 (回测)
forecast = m.predict(test_df)

# 6. 计算误差
results = pd.merge(test_df[['ds', 'y']], forecast[['ds', 'yhat']], on='ds')
results['diff'] = results['yhat'] - results['y']
results['error_pct'] = (results['diff'].abs() / results['y']) * 100

report_lines = []
report_lines.append("\n" + "="*60)
report_lines.append("🧐 BACKTEST REPORT: 2026-01-01 ~ 2026-01-13")
report_lines.append("="*60)
report_lines.append(f"{'Date':<12} | {'Actual':<10} | {'Predict':<10} | {'Diff':<10} | {'Error %':<8}")
report_lines.append("-" * 60)

mape = results['error_pct'].mean()
total_abs_diff = results['diff'].abs().sum()

for _, row in results.iterrows():
    d_str = row['ds'].strftime('%Y-%m-%d')
    act = int(row['y'])
    pred = int(row['yhat'])
    diff = int(row['diff'])
    err = row['error_pct']
    
    # Visual indicator for bad predictions (>5%)
    flag = "⚠️" if err > 5.0 else "✅"
    
    report_lines.append(f"{d_str} | {act:<10,} | {pred:<10,} | {diff:<10,} | {err:>6.2f}% {flag}")

report_lines.append("-" * 60)
report_lines.append(f"Overall MAPE (平均绝对百分比误差): {mape:.2f}%")
report_lines.append("="*60)

# 验证 2022 年是否真的被模型考虑了
report_lines.append("\n[Model Inspection] Training Data Years used:")
report_lines.append(str(train_df['ds'].dt.year.unique()))

with open("backtest_report.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(report_lines))

print("Backtest report saved to backtest_report.txt")
