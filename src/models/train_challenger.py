# train_challenger.py - LightGBM Challenger (Robust Edition)
# 功能：使用 LightGBM 直接训练，并包含完整的特征工程 (Flights + Weather + Lags)
# [UPDATE] Now reading from SQLite (traffic_full) instead of CSV

import pandas as pd
import numpy as np
import os
import json
import lightgbm as lgb
from sklearn.metrics import mean_absolute_percentage_error
import traceback
import sqlite3
import holidays

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.config import DB_PATH

# TRAIN_FILE = os.path.join(os.getcwd(), 'TSA_Final_Analysis.csv') # Legacy
# DB_PATH = os.path.join(os.getcwd(), 'tsa_data.db')

def load_data_and_split():
    print(f"Loading data from SQLite: {DB_PATH}")
    
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"DB Not Found: {DB_PATH}")

    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        
        # 1. Load Main Traffic & Features from 'traffic_full'
        print("Reading 'traffic_full' table...")
        df = pd.read_sql("SELECT * FROM traffic_full", conn)
        df['date'] = pd.to_datetime(df['date'])
        
        # 2. Merge Flight Stats from 'flight_stats'
        print("Merging Flight Stats from 'flight_stats'...")
        df_flights = pd.read_sql("SELECT date, SUM(arrival_count) as total_flights FROM flight_stats GROUP BY date", conn)
        
        conn.close() # Close connection early
        
        df_flights['date'] = pd.to_datetime(df_flights['date'])
        df = df.merge(df_flights, on='date', how='left')
        
        # Forward fill missing flight stats (persistence)
        df['total_flights'] = df['total_flights'].fillna(method='ffill').fillna(0)
        print(f"   Merged {len(df_flights)} flight records.")
        
    except Exception as e:
        print(f"CRITICAL DB ERROR: {e}")
        traceback.print_exc()
        raise e

    # [FEATURE ENGINEERING] Add Lags (Critical for Time Series)
    # Note: We must sort by date first
    df = df.sort_values('date').reset_index(drop=True)
    
    # Throughput Lags
    df['lag_7'] = df['throughput'].shift(7)
    df['lag_364'] = df['throughput'].shift(364)
    
    # Flight Lags (Proxies for future capacity)
    df['flight_last'] = df['total_flights'].shift(1).fillna(method='ffill')
    df['flight_ma_7'] = df['total_flights'].rolling(window=7, min_periods=1).mean().shift(1)
    
    # [PRODUCTION MODE] Train on ALL available data
    # Filter Pandemic
    mask_pandemic = (df['date'] >= '2020-03-01') & (df['date'] <= '2021-12-31')
    
    # Train set = All valid history (drop rows with NaN targets or Lags if critical, 
    # but LightGBM handles NaNs. However, we drop early rows where lag_364 is NaN to stabilize)
    train_df = df[~mask_pandemic].copy()
    
    # Clean Target NaNs and Critical Lags
    train_df = train_df.dropna(subset=['throughput', 'lag_364'])
    
    print(f"Training on Full History: {train_df['date'].min().date()} -> {train_df['date'].max().date()} ({len(train_df)} rows)")
    
    y_train = train_df['throughput']
    
    # Features to use (Sync with XGBoost capabilities)
    # Ensure these exist in traffic_full (Date parts usually need regen)
    train_df['day_of_week'] = train_df['date'].dt.dayofweek
    train_df['month'] = train_df['date'].dt.month
    train_df['year'] = train_df['date'].dt.year
    train_df['day'] = train_df['date'].dt.day
    train_df['is_weekend'] = train_df['day_of_week'].isin([5, 6]).astype(int)

    # Refined Feature List
    training_features = ['day_of_week', 'month', 'year', 'day', 'is_weekend', 
                         'weather_index', 'is_holiday', 'lag_7', 'lag_364', 
                         'flight_ma_7'] 
                         
    X_train = train_df[training_features].copy()
    X_train = X_train.fillna(0) # Safety fill
    
    # [FUTURE GENERATION]
    # Anchor to the last VALID ACTUAL date
    last_actual_date = train_df['date'].max()
    print(f"Last Actual Date: {last_actual_date.date()}")
    
    # Generate next 7 days (Safe horizon for Lag-7)
    future_dates = [last_actual_date + pd.Timedelta(days=i) for i in range(1, 8)]
    future_df = pd.DataFrame({'date': future_dates})
    
    # Merge existing features (Weather/Holiday/Throughput for Lags) from original df
    # Need to keep 'throughput' column in df for lag lookup
    cols_needed = ['date', 'weather_index', 'is_holiday', 'holiday_name', 'throughput']
    
    # If using DB traffic_full, it might have future rows (skeleton).
    # If not, we have to rebuild them. 
    # But usually traffic_full has future rows for weather.
    future_df = future_df.merge(df[cols_needed], on='date', how='left')
    
    # Date Features
    future_df['day_of_week'] = future_df['date'].dt.dayofweek
    future_df['month'] = future_df['date'].dt.month
    future_df['year'] = future_df['date'].dt.year
    future_df['day'] = future_df['date'].dt.day
    future_df['is_weekend'] = future_df['date'].dt.dayofweek.isin([5, 6]).astype(int)
    
    # Helper for Lags
    def get_lag_value(target_date, lag_days, source_df, col_name='throughput'):
        past_date = target_date - pd.Timedelta(days=lag_days)
        # Look in source_df (history)
        row = source_df[source_df['date'] == past_date]
        if not row.empty:
            return row.iloc[0][col_name]
        return 0 
        
    us_holidays = holidays.US(years=[2024, 2025, 2026])
    
    # Calculate Future through Lags
    # Need to pass original 'df' (complete with flights)
    future_df['lag_7'] = future_df['date'].apply(lambda x: get_lag_value(x, 7, df, 'throughput'))
    future_df['lag_364'] = future_df['date'].apply(lambda x: get_lag_value(x, 364, df, 'throughput'))
    
    # Calculate Future Flight Features (Persistence)
    # We use the LAST known Flight MA 7 from history and assume it flatlines based on day of week?
    # No, 'flight_ma_7' is a rolling mean. Flatlining the last value is decent for 7 days.
    last_known_flight_ma = df.iloc[-1]['flight_ma_7']
    future_df['flight_ma_7'] = last_known_flight_ma 
    
    # [CRITICAL FIX] Dynamically Calculate Holidays
    # Do NOT trust 'future_df' merged columns if they are 0
    # Recalculate is_holiday
    future_df['is_holiday'] = future_df['date'].apply(lambda d: 1 if d in us_holidays else 0)
    
    # Fill missing weather with 0 (since we can't easily dynamic calc weather), 
    # but check if merge actually brought meaningful values? 
    # CSV head showed "3" for weather_index at 2026-01-26... wait, tail showed "3" at -5 index.
    # So weather seems populated. Holiday was 0.
    future_df['weather_index'] = future_df['weather_index'].fillna(0)
    
    # Construct X_future
    X_future = future_df[training_features].copy()
    X_future = X_future.fillna(0)
    
    return X_train, y_train, X_future, future_df

def train_and_predict(X_train, y_train, X_future, future_df):
    print("Starting LightGBM training (Production)...")
    
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
    
    print("Generating Future Forecast...")
    preds = model.predict(X_future)
    
    # Save Forecast
    future_df['forecast'] = preds
    future_df.to_csv('challenger_forecast.csv', index=False)
    
    # Summary
    summary = {
        "model": "LightGBM (Full Data)",
        "mape": 0.0, 
        "forecast": future_df[['date', 'forecast']].astype({'date': str}).to_dict(orient='records')
    }
    with open("challenger_summary.json", "w") as f:
        json.dump(summary, f)

def main():
    try:
        print("Script (Production Mode) Started...")
        X_train, y_train, X_future, future_df = load_data_and_split()
        train_and_predict(X_train, y_train, X_future, future_df)
        print("Script Finished Successfully.")
    except Exception as e:
        print("CRASH IN MAIN:")
        traceback.print_exc()

if __name__ == "__main__":
    main()
