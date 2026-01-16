import pandas as pd
from prophet import Prophet
import matplotlib.pyplot as plt

# 1. 加载数据 (From DB)
print("Loading data from SQLite (traffic_full)...")
import sqlite3
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.config import DB_PATH

conn = sqlite3.connect(DB_PATH)
df = pd.read_sql("SELECT * FROM traffic_full", conn)
conn.close()

# 格式转换
df['ds'] = pd.to_datetime(df['date'])
df['y'] = df['throughput']

# 关键修复: 必须按日期升序排列，否则 shift 计算会错乱
df = df.sort_values('ds').reset_index(drop=True)

# 2. 特征工程 (On-the-Fly)
print("Generating Lag Features...")
# Lag 7: 已经在数据库里有了，重命名一下方便引用 (或者直接用 throughput_lag_7)
df['lag_7'] = df['throughput_lag_7']

# Lag 364: 同比特征 (Shift 52 weeks = 364 days, aligning day-of-week)
df['lag_364'] = df['y'].shift(364)

# [NEW] Off-Peak Workday Bias Correction (Synced from Backtest V2)
print("Generating bias correction features (Off-Peak Tue/Wed)...")
match_month = df['ds'].dt.month.isin([1, 2, 9, 10])
match_day = df['ds'].dt.dayofweek.isin([1, 2]) # 0=Mon, 1=Tue, 2=Wed
df['is_off_peak_workday'] = (match_month & match_day).astype(int)

# DEBUG: Check tail of raw data
print("\n[DEBUG] Raw Data Tail (Last 5 rows):")
print(df[['ds', 'y', 'lag_364']].tail())

# DEBUG: Check specific target dates
target_check_dates = ['2026-01-16', '2026-01-17', '2026-01-18']
print(f"\n[DEBUG] Checking Target Dates {target_check_dates}:")
print(df[df['ds'].astype(str).isin(target_check_dates)][['ds', 'y', 'lag_364']])

# 填充 Lags 的空值 (开头的一年数据没有 lag_364)
# Prophet 不允许 regressor 有 NaN，所以我们必须丢弃开头，或者填充
# 我们选择丢弃 lag_364 为空的行 (即第一年数据无法用于训练)
df_model = df.dropna(subset=['lag_364']) 

# 填补其他可能的空值
features = ['weather_index', 'is_holiday', 'is_spring_break', 'lag_7', 'lag_364', 'is_off_peak_workday']
for col in features:
    df_model[col] = df_model[col].fillna(0) 

print(f"Data ready. Total rows: {len(df_model)}")
print(f"Model Date Range: {df_model['ds'].min()} to {df_model['ds'].max()}")

# 3. 初始化模型 (The Engine)
print("Initializing Prophet Model...")
m = Prophet(
    daily_seasonality=False,
    yearly_seasonality=True,
    weekly_seasonality=True,
    seasonality_mode='multiplicative',
    changepoint_prior_scale=0.15 # Tuned for fast trend adaptation
)

# 4. 注入核武器 (Regressors)
print("Adding Regressors...")
m.add_regressor('weather_index')
m.add_regressor('is_holiday')
m.add_regressor('is_spring_break')
m.add_regressor('lag_7')
m.add_regressor('lag_364')
m.add_regressor('is_off_peak_workday') # [NEW]

# 5. 训练 (Fit)
# 训练集: y 不为空的行
train_df = df_model[df_model['y'].notnull()]

# [NEW] 关键清洗: 剔除 2020-2021 疫情严重期间的数据
# 这段时间的客流是极其异常的 (下降95%)，如果不剔除，会严重误导模型的"周期性"判断
# 我们保留 2019 (正常) 和 2022以后 (恢复后)
pandemic_start = '2020-03-01'
pandemic_end = '2021-12-31'
print(f"Filtering out Pandemic Era ({pandemic_start} ~ {pandemic_end})...")

mask_pandemic = (train_df['ds'] >= pandemic_start) & (train_df['ds'] <= pandemic_end)
train_df = train_df[~mask_pandemic]

print(f"Fitting model on {len(train_df)} rows (Clean Normal Data)...")
m.fit(train_df)

# 6. 预测 (Predict)
# 预测集: 包含未来的行
print("Predicting...")
forecast = m.predict(df_model)

# 7. 输出战报 (The Alpha)
target_dates = ['2026-01-14', '2026-01-15', '2026-01-16', '2026-01-17', '2026-01-18']
report_lines = []
report_lines.append("\n" + "="*50)
report_lines.append("🚀 FUTURE FORECAST REPORT")
report_lines.append("="*50)

subset = forecast[forecast['ds'].astype(str).isin(target_dates)]

if len(subset) == 0:
    report_lines.append("❌ Critical Error: No forecast generated for target dates!")
else:
    for _, row in subset.iterrows():
        d_str = row['ds'].strftime('%Y-%m-%d')
        yhat = int(row['yhat'])
        lower = int(row['yhat_lower'])
        upper = int(row['yhat_upper'])
        reg_effect = row['extra_regressors_multiplicative']
        
        report_lines.append(f"📅 {d_str} | 🔮 预测: {yhat:,} 人次")
        report_lines.append(f"   范围: [{lower:,} ~ {upper:,}]")
        report_lines.append(f"   因子加成: {reg_effect:.4f} (基准=0, >0为正向拉动)")
        report_lines.append("-" * 30)

    # 8. 特殊检查: 1月16日 (明天) 的详细构成
    report_lines.append("\n🔍 DEEP DIVE: 2026-01-16 (Tomorrow)")
    report_lines.append("="*50)
    target_row = forecast[forecast['ds'].astype(str) == '2026-01-16']
    
    if len(target_row) > 0:
        target_day = target_row.iloc[0]
        trend = target_day['trend']
        weekly = target_day['weekly']
        yearly = target_day['yearly']
        
        report_lines.append(f"基础趋势 (Trend): {int(trend):,}")
        report_lines.append(f"周期性 (Seasonality):")
        report_lines.append(f"  - Weekly: {weekly:.4f}")
        report_lines.append(f"  - Yearly: {yearly:.4f}")
        
        report_lines.append(f"外部回归 (Regressors):")
        if 'weather_index' in target_day:
            report_lines.append(f"  - Weather Index Effect: {target_day['weather_index']:.4f}")
        if 'is_holiday' in target_day:
            report_lines.append(f"  - Holiday Effect: {target_day['is_holiday']:.4f}")
        if 'lag_7' in target_day:
            report_lines.append(f"  - Lag-7 Effect: {target_day['lag_7']:.4f}")
        if 'lag_364' in target_day:
            report_lines.append(f"  - Lag-364 Effect: {target_day['lag_364']:.4f}")

        report_lines.append(f"\nFinal yhat = Trend * (1 + Weekly + Yearly + Regressors)")
        report_lines.append("="*50)

# Write to file
with open("model_report.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(report_lines))

print("Report saved to model_report.txt")
