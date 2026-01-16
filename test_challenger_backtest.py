# test_challenger_backtest.py
# 功能：独立的 Challenger 模型回测脚本 (2026-01-01 ~ 2026-01-14)
# 不影响现有生产环境代码

import pandas as pd
import numpy as np
import os
import lightgbm as lgb
from sklearn.metrics import mean_absolute_percentage_error
import traceback
import sqlite3
import holidays

DB_PATH = os.path.join(os.getcwd(), 'tsa_data.db')

def load_data_and_split():
    print(f"Loading data from SQLite: {DB_PATH}")
    
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"DB Not Found: {DB_PATH}")

    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        
        # 1. Load Main Traffic & Features
        df = pd.read_sql("SELECT * FROM traffic_full", conn)
        df['date'] = pd.to_datetime(df['date'])
        
        # 2. Merge Flight Stats
        df_flights = pd.read_sql("SELECT date, SUM(arrival_count) as total_flights FROM flight_stats GROUP BY date", conn)
        conn.close()
        
        df_flights['date'] = pd.to_datetime(df_flights['date'])
        df = df.merge(df_flights, on='date', how='left')
        
        # Persistence fill
        df['total_flights'] = df['total_flights'].fillna(method='ffill').fillna(0)

    except Exception as e:
        print(f"CRITICAL DB ERROR: {e}")
        traceback.print_exc()
        raise e

    # [FEATURE ENGINEERING]
    df = df.sort_values('date').reset_index(drop=True)
    
    df['lag_7'] = df['throughput'].shift(7)
    df['lag_364'] = df['throughput'].shift(364)
    df['flight_last'] = df['total_flights'].shift(1).fillna(method='ffill')
    df['flight_ma_7'] = df['total_flights'].rolling(window=7, min_periods=1).mean().shift(1)
    
    # [BACKTEST SPLIT]
    TRAIN_END = '2025-12-31'
    TEST_START = '2026-01-01'
    TEST_END = '2026-01-14'
    
    # Filter Pandemic
    mask_pandemic = (df['date'] >= '2020-03-01') & (df['date'] <= '2021-12-31')
    
    # Train
    train_df = df[(df['date'] <= TRAIN_END) & (~mask_pandemic)].copy()
    train_df = train_df.dropna(subset=['throughput', 'lag_364'])
    
    # Test
    test_df = df[(df['date'] >= TEST_START) & (df['date'] <= TEST_END)].copy()
    
    print(f"Training Range: {train_df['date'].min().date()} -> {train_df['date'].max().date()} ({len(train_df)} rows)")
    print(f"Testing Range:  {test_df['date'].min().date()} -> {test_df['date'].max().date()} ({len(test_df)} rows)")
    
    # Ensure Test Features are ready (re-calc dynamic logic to be safe)
    # Holidays
    us_holidays = holidays.US(years=[2026])
    test_df['is_holiday'] = test_df['date'].apply(lambda d: 1 if d in us_holidays else 0)
    
    # Note: Test set from DB already has 'throughput'. We will use it as y_test.
    # But for PREDICTION, we should pretend we don't know it?
    # Actually, Lags like lag_7 use real history.
    # In a real backtest, lag_7 at Jan 8 should see Jan 1 actual.
    # Since we are testing Jan 1-14, lag_7 for Jan 8 IS known (Jan 1).
    # So using static lag columns from 'df' is correct.
    
    features = ['day_of_week', 'month', 'year', 'day', 'is_weekend', 
                'weather_index', 'is_holiday', 'lag_7', 'lag_364', 
                'flight_ma_7']
                
    # Date parts
    for d in [train_df, test_df]:
        d['day_of_week'] = d['date'].dt.dayofweek
        d['month'] = d['date'].dt.month
        d['year'] = d['date'].dt.year
        d['day'] = d['date'].dt.day
        d['is_weekend'] = d['date'].dt.dayofweek.isin([5, 6]).astype(int)

    X_train = train_df[features].fillna(0)
    y_train = train_df['throughput']
    
    X_test = test_df[features].fillna(0)
    y_test = test_df['throughput']
    
    return X_train, y_train, X_test, y_test, test_df

def run_backtest():
    X_train, y_train, X_test, y_test, test_df = load_data_and_split()
    
    print("Training LightGBM...")
    lgb_train = lgb.Dataset(X_train, y_train)
    
    params = {
        'objective': 'regression',
        'metric': 'mape',
        'boosting_type': 'gbdt',
        'num_leaves': 31,
        'learning_rate': 0.05,
        'feature_fraction': 0.9,
        'bagging_fraction': 0.8,
        'bagging_freq': 5,
        'verbose': -1,
        'seed': 42
    }
    
    model = lgb.train(params, lgb_train, num_boost_round=1000)
    
    print("Warning: LightGBM trained.")
    preds = model.predict(X_test)
    
    # Evaluation
    from sklearn.metrics import mean_absolute_percentage_error
    mape = mean_absolute_percentage_error(y_test, preds)
    print(f"\n=== BACKTEST RESULTS (2026-01-01 ~ 2026-01-14) ===")
    print(f"MAPE: {mape:.4f} ({mape*100:.2f}%)")
    
    # Detailed Report
    test_df['predicted'] = preds
    test_df['actual'] = y_test
    test_df['abs_error'] = abs(test_df['actual'] - test_df['predicted'])
    test_df['ape'] = test_df['abs_error'] / test_df['actual']
    
    print("\nDetailed Daily Performance:")
    print(test_df[['date', 'throughput', 'predicted', 'ape']].to_string())
    
    test_df.to_csv('challenger_backtest_eval.csv', index=False)
    print("\nSaved detailed results to 'challenger_backtest_eval.csv'")

if __name__ == "__main__":
    run_backtest()
