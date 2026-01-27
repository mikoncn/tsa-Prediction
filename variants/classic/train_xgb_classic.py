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
from src.config import DB_PATH, FORECAST_MODEL_PATH

warnings.filterwarnings('ignore')

def run():
    # 1. 加载数据 (From DB)
    print("Loading data from SQLite (Classic Mode)...")
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql("SELECT * FROM traffic_full", conn)
    except Exception as e:
        print(f"Error reading traffic_full: {e}")
        conn.close()
        return
    conn.close()

    df['ds'] = pd.to_datetime(df['date'])
    df['y'] = df['throughput']
    df = df.sort_values('ds').reset_index(drop=True)

    # A. 时间特征 (Time Components)
    df['day_of_week'] = df['ds'].dt.dayofweek
    df['month'] = df['ds'].dt.month
    df['year'] = df['ds'].dt.year
    df['day_of_year'] = df['ds'].dt.dayofyear
    df['week_of_year'] = df['ds'].dt.isocalendar().week.astype(int)
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)

    # B. 滞后特征 (Lag Features)
    df['lag_7'] = df['y'].shift(7).fillna(method='bfill')
    df['lag_364'] = df['y'].shift(364).fillna(method='bfill')

    # C. 假特节日特征 (Holiday Features)
    from src.utils.holiday_utils import get_holiday_features
    h_feats = get_holiday_features(df['ds'])
    for c in h_feats.columns:
        df[c] = h_feats[c].values

    # D. 填充缺失值
    features = [
        'day_of_week', 'month', 'year', 'day_of_year', 'week_of_year', 'is_weekend',
        'is_holiday', 'lag_7', 'lag_364'
    ]

    # Clean
    df_model = df.dropna(subset=['lag_364']).copy()
    
    # 3. 划分训练集与测试集
    train_cutoff = pd.Timestamp('2025-12-31')
    train_df = df_model[df_model['ds'] <= train_cutoff]
    test_df = df_model[df_model['ds'] > train_cutoff]

    X_train = train_df[features]
    y_train = train_df['y']
    X_test = test_df[features]
    y_test = test_df['y']

    # 4. 训练模型
    model = XGBRegressor(n_estimators=1000, learning_rate=0.05, max_depth=5, n_jobs=-1, random_state=42)
    model.fit(X_train, y_train)

    if not X_test.empty:
        y_pred = model.predict(X_test)
        mape = mean_absolute_percentage_error(y_test, y_pred)
        print(f"Classic XGBoost finished. Jan 2026 MAPE: {mape*100:.2f}%")

    print("\n[FINISH] Classic Training Complete. No weather circuit breakers were applied.")

if __name__ == "__main__":
    run()
