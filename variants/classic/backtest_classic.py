import pandas as pd
import numpy as np
import os
import sqlite3
from xgboost import XGBRegressor
import sys

sys.path.append(os.getcwd())
from src.config import DB_PATH

def run_classic_backtest():
    print("🚀 Running CLASSIC Backtest (No Weather Penalties)...")
    
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT date, throughput FROM traffic_full", conn)
    conn.close()
    
    df['ds'] = pd.to_datetime(df['date'])
    df['y'] = df['throughput']
    df = df.sort_values('ds')
    
    # Features
    df['day_of_week'] = df['ds'].dt.dayofweek
    df['month'] = df['ds'].dt.month
    df['lag_7'] = df['y'].shift(7).fillna(method='bfill')
    df['lag_364'] = df['y'].shift(364).fillna(method='bfill')
    
    features = ['day_of_week', 'month', 'lag_7', 'lag_364']
    df = df.dropna(subset=['y', 'lag_364'])
    
    years = [2025, 2026]
    for year in years:
        train_df = df[df['ds'].dt.year < year]
        test_df = df[df['ds'].dt.year == year]
        
        if test_df.empty: continue
        
        model = XGBRegressor(n_estimators=1000, learning_rate=0.05, max_depth=5, n_jobs=-1, random_state=42)
        model.fit(train_df[features], train_df['y'])
        
        y_pred = model.predict(test_df[features])
        test_df['pred'] = y_pred
        test_df['err'] = (abs(test_df['pred'] - test_df['y']) / test_df['y']) * 100
        
        if year == 2026:
             target = test_df[test_df['ds'] == '2026-01-25']
             if not target.empty:
                 jan25_str = f"   Jan 25 Error: {target['err'].values[0]:.1f}% (Expected to be high without weather logic)\\n"
                 print(jan25_str)
                 
        # Save Detailed CSV
        output_csv = f"classic_errors_{year}.csv"
        test_df[['ds', 'y', 'pred', 'err', 'day_of_week', 'month']].to_csv(output_csv, index=False)
        print(f"Saved detailed errors to {output_csv}")

if __name__ == "__main__":
    run_classic_backtest()
