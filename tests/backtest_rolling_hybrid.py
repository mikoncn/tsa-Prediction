
import os
import sqlite3
import pandas as pd
import numpy as np
import xgboost as xgb
from datetime import datetime, timedelta
import sys
import pickle

# 设置路径
sys.path.append(os.getcwd())
from src.config import DB_PATH
from src.utils.holiday_utils import (
    get_holiday_features, TARGET_HOLIDAYS, get_us_holidays, 
    get_holiday_intensity, get_clean_lag_date
)
from src.models.model_utils import get_aggregated_weather_features
from dateutil.easter import easter

def generate_hybrid_features(df):
    """
    生成混合模型特征：老模型 v2 逻辑 + 天气影子模型 + 报复性指数
    """
    df['ds'] = pd.to_datetime(df['ds'])
    df['day_of_week'] = df['ds'].dt.dayofweek
    df['month'] = df['ds'].dt.month
    df['year'] = df['ds'].dt.year
    df['day_of_year'] = df['ds'].dt.dayofyear
    df['week_of_year'] = df['ds'].dt.isocalendar().week.astype(int)
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)

    # 1. 基础滞后
    df['lag_7'] = df['y'].shift(7).fillna(method='bfill')
    df['lag_364'] = df['y'].shift(364).fillna(method='bfill')

    # 2. 假日增强 (Classic v2)
    h_feats = get_holiday_features(df['ds'])
    for c in h_feats.columns:
        df[c] = h_feats[c].values
    
    # 距离重大假日的距离 (Ramp-up)
    us_holidays = get_us_holidays(2019, 2030)
    major_holiday_dates = []
    for date, name in us_holidays.items():
        if any(target in name for target in TARGET_HOLIDAYS):
            major_holiday_dates.append(pd.Timestamp(date))
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

    # 业务标识
    df['holiday_intensity'] = df['holiday_name'].apply(get_holiday_intensity)
    match_month = df['ds'].dt.month.isin([1, 2, 9, 10])
    match_day = df['ds'].dt.dayofweek.isin([1, 2])
    df['is_off_peak_workday'] = (match_month & match_day).astype(int)
    df['is_long_weekend'] = 0
    mask_long = (df['is_holiday'] == 1) & (df['day_of_week'].isin([0, 4]))
    df.loc[mask_long, 'is_long_weekend'] = 1

    # 3. 优化滞后 (v2)
    holiday_dates_set = set(df[df['is_holiday'] == 1]['ds'].dt.date)
    df['lag_7_clean_date'] = df['ds'].apply(lambda x: get_clean_lag_date(x, holiday_dates_set, 7))
    val_map = df.set_index('ds')['y'].to_dict()
    df['lag_7_clean'] = df['lag_7_clean_date'].map(val_map).fillna(df['lag_7'])
    
    h_map = {}
    for idx, row in df[df['is_holiday_exact_day'] == 1].iterrows():
        h_map[(row['ds'].year, row['holiday_name'])] = row['y']
    def get_holiday_yoy(row):
        if row['is_holiday_exact_day'] == 0: return row['lag_364']
        prev_val = h_map.get((row['ds'].year - 1, row['holiday_name']))
        return prev_val if prev_val is not None else row['lag_364']
    df['lag_holiday_yoy'] = df.apply(get_holiday_yoy, axis=1)

    # 4. 报复性指数 (Revenge Index)
    df['weather_index'] = df['weather_index'].fillna(0)
    df['w_lag_1'] = df['weather_index'].shift(1).fillna(0)
    df['w_lag_2'] = df['weather_index'].shift(2).fillna(0)
    df['w_lag_3'] = df['weather_index'].shift(3).fillna(0)
    df['revenge_index'] = (df['w_lag_1'] * 0.5) + (df['w_lag_2'] * 0.3) + (df['w_lag_3'] * 0.2)

    # 5. 影子模型 (Shadow Model) - 预测取消率
    df['predicted_cancel_rate'] = 0.0
    shadow_path = os.path.join('src', 'models', 'shadow_weather_model.pkl')
    if os.path.exists(shadow_path):
        print("  Loading Shadow Model...")
        with open(shadow_path, 'rb') as f:
            shadow_model = pickle.load(f)
        
        conn = sqlite3.connect(DB_PATH)
        df_weather = get_aggregated_weather_features(conn)
        conn.close()
        
        shadow_features = [
            'max_snow', 'mean_snow', 'max_snow_sq', 'mean_snow_sq',
            'max_wind', 'mean_wind', 'max_precip', 'mean_precip', 
            'min_temp', 'mean_temp', 'national_severity', 'month', 'day_of_year'
        ]
        X_shadow = df_weather[shadow_features].fillna(0)
        df_weather['predicted_cancel_rate'] = shadow_model.predict(X_shadow)
        
        # Merge back
        df['ds_date'] = df['ds'].dt.floor('D')
        df_weather['date'] = pd.to_datetime(df_weather['date'])
        df = df.merge(df_weather[['date', 'predicted_cancel_rate']], left_on='ds_date', right_on='date', how='left')
        df['predicted_cancel_rate'] = df['predicted_cancel_rate_y'].fillna(0) if 'predicted_cancel_rate_y' in df.columns else df['predicted_cancel_rate'].fillna(0)
        
    df['lag_7_adjusted'] = df['lag_7_clean'] * (1 - df['predicted_cancel_rate'])

    return df

def run_hybrid_rolling_backtest():
    print("🚀 开始混合模型步进式回测 (Hybrid Rolling Walk-Forward)...")
    
    # 1. 加载数据
    conn = sqlite3.connect(DB_PATH)
    raw_df = pd.read_sql("SELECT date as ds, throughput as y, weather_index FROM traffic_full ORDER BY date", conn)
    conn.close()
    
    # 2. 特征工程
    df = generate_hybrid_features(raw_df)
    
    # 3. 定义特征集
    features = [
        'day_of_week', 'month', 'year', 'day_of_year', 'week_of_year', 'is_weekend',
        'is_holiday', 'is_holiday_exact_day', 'is_holiday_travel_window', 
        'lag_7_clean', 'lag_holiday_yoy', 'weather_index',
        'is_off_peak_workday', 'days_to_nearest_holiday', 'is_long_weekend', 'is_spring_break',
        'holiday_intensity', 'revenge_index', 'predicted_cancel_rate', 'lag_7_adjusted'
    ]
    
    # 4. 设定回测区间
    test_dates = pd.date_range(start='2025-01-01', end='2026-01-20')
    results = []

    print(f"📊 正在模拟 {len(test_dates)} 天的每日重练过程...")

    for current_date in test_dates:
        train_mask = pd.to_datetime(df['ds']) < current_date
        train_df = df[train_mask].dropna(subset=['lag_holiday_yoy'])
        
        predict_mask = pd.to_datetime(df['ds']) == current_date
        predict_df = df[predict_mask].copy()
        
        if predict_df.empty: continue
            
        model = xgb.XGBRegressor(n_estimators=1000, max_depth=5, learning_rate=0.05, n_jobs=-1, random_state=42)
        model.fit(train_df[features], train_df['y'])
        
        pred = model.predict(predict_df[features])[0]
        actual = predict_df['y'].values[0]
        error = abs(pred - actual) / actual
        
        results.append({
            'ds': current_date.date(),
            'actual': actual,
            'pred': pred,
            'mape': error
        })

    res_df = pd.DataFrame(results)
    avg_mape = res_df['mape'].mean()
    
    print("\n" + "="*40)
    print(f"🏆 混合模型步进式回测汇总 (2025-2026)")
    print(f"平均 MAPE: {avg_mape:.4%}")
    print("="*40)
    
    res_df.to_csv('rolling_backtest_hybrid_results.csv', index=False)
    print("📦 结果已保存至 rolling_backtest_hybrid_results.csv")

if __name__ == "__main__":
    run_hybrid_rolling_backtest()
