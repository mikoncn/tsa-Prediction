
import os
import sqlite3
import pandas as pd
import numpy as np
import xgboost as xgb
from datetime import datetime, timedelta
import sys

# 设置路径
sys.path.append(os.getcwd())
from src.config import DB_PATH
from src.utils.holiday_utils import get_holiday_features, TARGET_HOLIDAYS, get_us_holidays
from dateutil.easter import easter

def generate_base_features(df):
    """
    生成基础时间特征和滞后特征
    注意：这里仅保留老模型的“Isolated”特征集
    """
    df['ds'] = pd.to_datetime(df['ds'])
    df['day_of_week'] = df['ds'].dt.dayofweek
    df['month'] = df['ds'].dt.month
    df['year'] = df['ds'].dt.year
    df['day_of_year'] = df['ds'].dt.dayofyear
    df['week_of_year'] = df['ds'].dt.isocalendar().week.astype(int)
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)

    # Lags
    df['lag_7'] = df['y'].shift(7)
    df['lag_364'] = df['y'].shift(364)
    
    # Fillna lags (using backfill for early data in the training set)
    df['lag_7'] = df['lag_7'].fillna(method='bfill')
    df['lag_364'] = df['lag_364'].fillna(method='bfill')
    
    return df

def generate_advanced_features(df):
    """
    生成高级业务逻辑和天气特征
    """
    # 1. 假日基础特征
    h_feats = get_holiday_features(df['ds'])
    for c in h_feats.columns:
        df[c] = h_feats[c].values

    # 2. Optimization v2: Holiday Intensity & Ratio
    from src.utils.holiday_utils import get_holiday_intensity, get_clean_lag_date
    df['holiday_intensity'] = df['holiday_name'].apply(get_holiday_intensity)
    
    # Calculate Monthly Median for Ratios
    # We use a 30-day rolling median as a proxy for 'normal' levels
    df['normal_median'] = df['y'].rolling(window=30, center=True, min_periods=1).median().shift(1).fillna(method='bfill')
    df['holiday_ratio'] = 1.0
    mask_h = df['is_holiday_exact_day'] == 1
    df.loc[mask_h, 'holiday_ratio'] = df['y'] / df['normal_median']
    # In production, this would be a historical lookup. For backtest, we can use a lagged ratio or mean ratio.
    # Let's use a constant mean holiday ratio for each holiday name to avoid look-ahead
    holiday_ratios = df[mask_h].groupby('holiday_name')['holiday_ratio'].mean().to_dict()
    df['holiday_ratio_feat'] = df['holiday_name'].map(holiday_ratios).fillna(1.0)

    # 3. Optimization v2: lag_7_clean (recursive)
    holiday_dates_set = set(df[df['is_holiday'] == 1]['ds'].dt.date)
    df['lag_7_clean_date'] = df['ds'].apply(lambda x: get_clean_lag_date(x, holiday_dates_set, 7))
    
    # Map cleaned date to its y value (requires a lookup dict)
    val_map = df.set_index('ds')['y'].to_dict()
    df['lag_7_clean'] = df['lag_7_clean_date'].map(val_map).fillna(df['lag_7'])

    # 4. Optimization v2: lag_holiday_yoy
    # Matches same holiday last year
    # Pre-calculate holiday map: { (year, holiday_name): y }
    h_map = {}
    for idx, row in df[df['is_holiday_exact_day'] == 1].iterrows():
        h_map[(row['ds'].year, row['holiday_name'])] = row['y']
    
    def get_holiday_yoy(row):
        if row['is_holiday_exact_day'] == 0: return row['lag_364']
        prev_val = h_map.get((row['ds'].year - 1, row['holiday_name']))
        return prev_val if prev_val is not None else row['lag_364']
        
    df['lag_holiday_yoy'] = df.apply(get_holiday_yoy, axis=1)

    # 5. 距离重大假日的距离 (Ramp-up)
    us_holidays = get_us_holidays(2019, 2030)
    major_holiday_dates = []
    for date, name in us_holidays.items():
        if any(target in name for target in TARGET_HOLIDAYS):
            major_holiday_dates.append(pd.Timestamp(date))
    
    # 加入 Good Friday
    for y in df['ds'].dt.year.unique():
        try:
            ed = pd.Timestamp(easter(y))
            major_holiday_dates.append(ed - pd.Timedelta(days=2))
        except: pass
    major_holiday_dates = sorted(list(set(major_holiday_dates)))
    
    def get_dist(d):
        d_ts = pd.Timestamp(d)
        min_diff = 999
        for h in major_holiday_dates:
            diff = (d_ts - h).days
            if abs(diff) < abs(min_diff):
                min_diff = diff
        return np.clip(min_diff, -15, 15)

    df['days_to_nearest_holiday'] = df['ds'].apply(get_dist)

    # 6. Off-Peak 工作日 (Tue/Wed in Jan/Feb/Sep/Oct)
    match_month = df['ds'].dt.month.isin([1, 2, 9, 10])
    match_day = df['ds'].dt.dayofweek.isin([1, 2])
    df['is_off_peak_workday'] = (match_month & match_day).astype(int)

    # 7. 长周末
    df['is_long_weekend'] = 0
    mask_long = (df['is_holiday'] == 1) & (df['day_of_week'].isin([0, 4]))
    df.loc[mask_long, 'is_long_weekend'] = 1

    return df

def run_rolling_backtest():
    print("🚀 开始步进式回测 (Rolling Walk-Forward)...")
    
    # 1. 加载数据
    conn = sqlite3.connect(DB_PATH)
    raw_df = pd.read_sql("SELECT date as ds, throughput as y, weather_index FROM traffic_full ORDER BY date", conn)
    conn.close()
    
    # 2. 特征工程 (Full Dataset)
    df = generate_base_features(raw_df)
    df = generate_advanced_features(df)
    df['weather_index'] = df['weather_index'].fillna(0)
    
    # 3. 定义特征集 (Old Model Isolated + Optimization v2)
    features = [
        'day_of_week', 'month', 'year', 'day_of_year', 'week_of_year', 'is_weekend',
        'is_holiday', 'is_holiday_exact_day', 'is_holiday_travel_window', 
        'lag_7', 'lag_364', 'weather_index',
        'is_off_peak_workday', 'days_to_nearest_holiday', 'is_long_weekend', 'is_spring_break',
        # v2 Features
        'lag_7_clean', 'lag_holiday_yoy', 'holiday_intensity', 'holiday_ratio_feat'
    ]
    
    # 4. 设定回测区间: 2025-01-01 到 2026-01-20
    test_dates = pd.date_range(start='2025-01-01', end='2026-01-20')
    results = []

    print(f"📊 正在模拟 {len(test_dates)} 天的每日重练过程...")

    for current_date in test_dates:
        # A. 准备训练集: 当前日期之前的所有数据
        train_mask = pd.to_datetime(df['ds']) < current_date
        # 丢除早期无法计算 lag_364 的数据
        train_df = df[train_mask].dropna(subset=['lag_364'])
        
        # B. 准备预测集: 仅包含当前这一天
        predict_mask = pd.to_datetime(df['ds']) == current_date
        predict_df = df[predict_mask].copy()
        
        if predict_df.empty:
            continue
            
        # C. 训练模型
        model = xgb.XGBRegressor(n_estimators=500, max_depth=5, learning_rate=0.05, n_jobs=-1, random_state=42)
        model.fit(train_df[features], train_df['y'])
        
        # D. 预测
        pred = model.predict(predict_df[features])[0]
        actual = predict_df['y'].values[0]
        error = abs(pred - actual) / actual
        
        print(f"[{current_date.date()}] 实际: {actual:,.0f} | 预测: {pred:,.0f} | 误差: {error:.2%}")
        
        results.append({
            'ds': current_date.date(),
            'actual': actual,
            'pred': pred,
            'mape': error
        })

    # 5. 汇总结果
    res_df = pd.DataFrame(results)
    avg_mape = res_df['mape'].mean()
    
    print("\n" + "="*40)
    print(f"🏆 步进式回测汇总 (2026-01)")
    print(f"平均 MAPE: {avg_mape:.4%}")
    print("="*40)
    
    res_df.to_csv('rolling_backtest_results_2026.csv', index=False)
    print("📦 结果已保存至 rolling_backtest_results_2026.csv")

if __name__ == "__main__":
    run_rolling_backtest()
