import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_percentage_error
import warnings
warnings.filterwarnings('ignore')

# 1. 加载数据
print("Loading data for 2025 Full Scale Backtest...")
df = pd.read_csv("TSA_Final_Analysis.csv")
df['ds'] = pd.to_datetime(df['date'])
df['y'] = df['throughput']
df = df.sort_values('ds').reset_index(drop=True)

# 2. 特征工程 (一致性保持)
print("Generating features...")
df['day_of_week'] = df['ds'].dt.dayofweek
df['month'] = df['ds'].dt.month
df['year'] = df['ds'].dt.year
df['day_of_year'] = df['ds'].dt.dayofyear
df['week_of_year'] = df['ds'].dt.isocalendar().week.astype(int)
df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)

df['lag_7'] = df['throughput_lag_7']
df['lag_364'] = df['y'].shift(364)

# Off-Peak Workday
match_month = df['ds'].dt.month.isin([1, 2, 9, 10])
match_day = df['ds'].dt.dayofweek.isin([1, 2])
df['is_off_peak_workday'] = (match_month & match_day).astype(int)

features = [
    'day_of_week', 'month', 'year', 'day_of_year', 'week_of_year', 'is_weekend',
    'weather_index', 'is_holiday', 'is_spring_break', 'is_off_peak_workday',
    'is_holiday_exact_day', 'is_holiday_travel_window',
    'lag_7', 'lag_364'
]

# 3. 严格数据切分 (Strict Split)
# 训练集: 截止到 2024-12-31 (完全不看2025)
# 并且剔除疫情 (2020-03-01 ~ 2021-12-31)
pandemic_start = pd.Timestamp('2020-03-01')
pandemic_end = pd.Timestamp('2021-12-31')
train_cutoff = pd.Timestamp('2024-12-31') # [CHANGED]

df_model = df.dropna(subset=['lag_364']).copy()
for col in features:
    df_model[col] = df_model[col].fillna(0)

mask_train_period = (df_model['ds'] <= train_cutoff)
mask_pandemic = (df_model['ds'] >= pandemic_start) & (df_model['ds'] <= pandemic_end)

train_df = df_model[mask_train_period & (~mask_pandemic)]

# 测试集: 2025 全年 (The Gauntlet)
test_start = pd.Timestamp('2025-01-01')
test_end = pd.Timestamp('2025-12-31')
test_df = df_model[(df_model['ds'] >= test_start) & (df_model['ds'] <= test_end)].copy()

print(f"Training Data: {len(train_df)} rows (End: {train_df['ds'].max().date()})")
print(f"Test Data: {len(test_df)} rows ({test_start.date()} ~ {test_end.date()})")

X_train = train_df[features]
y_train = train_df['y']
X_test = test_df[features]
y_test = test_df['y']

# 4. 训练 XGBoost
print("Training XGBoost Model...")
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

# 5. 预测
print("Predicting 2025...")
y_pred = model.predict(X_test)

test_df['yhat'] = y_pred.astype(int)
test_df['diff'] = test_df['yhat'] - test_df['y']
test_df['error_pct'] = (test_df['diff'].abs() / test_df['y']) * 100

overall_mape = test_df['error_pct'].mean()

# 6. 生成分析报告
lines = []
lines.append("="*60)
lines.append(f"🛡️ 2025 FULL YEAR BACKTEST REPORT")
lines.append(f"Overall MAPE: {overall_mape:.2f}%")
lines.append("="*60)

# 月度误差分析
lines.append("\n[Monthly Performance]")
lines.append(f"{'Month':<10} | {'MAPE':<10} | {'Bad Days (>10%)'}")
lines.append("-" * 50)

monthly_stats = test_df.groupby('month')['error_pct'].agg(['mean', lambda x: (x>10).sum()])
monthly_stats.columns = ['MAPE', 'Bad_Days']

for m, row in monthly_stats.iterrows():
    lines.append(f"{m:<10} | {row['MAPE']:>6.2f}%   | {row['Bad_Days']}")

# 找出误差最大的 Top 5 日子
lines.append("\n[Top 5 Worst Predictions]")
worst_days = test_df.sort_values('error_pct', ascending=False).head(5)
for _, row in worst_days.iterrows():
    d_str = row['ds'].strftime('%Y-%m-%d')
    lines.append(f"{d_str}: Actual={int(row['y']):,}, Pred={int(row['yhat']):,}, Err={row['error_pct']:.2f}%")

# 保存报告
with open("backtest_2025_report.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(lines))

print(f"Backtest Complete. Overall MAPE: {overall_mape:.2f}%")
print("See backtest_2025_report.txt for details.")
