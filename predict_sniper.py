# predict_sniper.py - 智能狙击客流预测内核
# 功能：利用“即时”飞行数据（T-1 或 T-2）对 TSA 客流量进行补盲预测。
# 业务逻辑：当官方 TSA 数据尚未公布时，通过天空中的航班流量推算出地面上的旅客人数。

import pandas as pd
import numpy as np
import sqlite3
import warnings
import sys
import json
from xgboost import XGBRegressor
from datetime import datetime, timedelta

# 禁用无关的警告信息，保持输出整洁
warnings.filterwarnings('ignore')

DB_PATH = 'tsa_data.db'
CSV_PATH = 'TSA_Final_Analysis.csv'

def get_db_connection():
    """获取数据库连接"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def load_data():
    """
    核心数据对齐逻辑。
    1. 从 CSV 读取官方历史客流数据。
    2. 从 SQLite 读取 OpenSky 记录的各机场航班量。
    3. 按日期进行合并，形成包含“客流+飞行量”的联合训练集。
    """
    # 1. 加载 TSA 客流历史
    df = pd.read_csv(CSV_PATH)
    df['ds'] = pd.to_datetime(df['date'])
    df['y'] = df['throughput']
    df = df.sort_values('ds').reset_index(drop=True)
    
    # 2. 加载航班数据（按日聚合）
    conn = get_db_connection()
    df_flights = pd.read_sql("SELECT date, SUM(arrival_count) as total_flights FROM flight_stats GROUP BY date", conn)
    conn.close()
    
    df_flights['date'] = pd.to_datetime(df_flights['date'])
    
    # 3. 数据合并
    df = df.merge(df_flights, left_on='ds', right_on='date', how='left')
    
    # 补全空缺：对于没有飞行记录的更早历史，使用近期平均值填充，防止模型对 0 产生偏差
    avg_flights = df[df['total_flights'] > 0]['total_flights'].mean()
    if pd.isna(avg_flights): avg_flights = 0
    df['total_flights'] = df['total_flights'].fillna(avg_flights)
    
    return df

def train_and_predict(target_date_str):
    """
    执行一次快速训练并给出指定日期的狙击预测。
    """
    df = load_data()
    
    # 特征工程：生成对预测具有显著影响的自变量
    df['day_of_week'] = df['ds'].dt.dayofweek
    df['month'] = df['ds'].dt.month
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    
    # 核心实战特征：当天的真实飞行量
    df['flight_current'] = df['total_flights'] 
    
    # 时间对齐特征：获取过去 7 天和 364 天（去年同日）的滞后值
    df['lag_7'] = df['throughput_lag_7'] 
    df['lag_364'] = df['y'].shift(364).fillna(method='bfill')
    
    # 定义模型要用到的全部列（必须与训练和预测完全一致）
    features = [
        'day_of_week', 'month', 'is_weekend', 'flight_current', 
        'weather_index', 'is_holiday', 'is_spring_break',
        'is_holiday_exact_day', 'is_holiday_travel_window',
        'lag_7', 'lag_364'
    ]
    
    # 训练掩码：剔除受疫情特殊行情干扰的数据 (2020-2021)
    mask_pandemic = (df['ds'] >= '2020-03-01') & (df['ds'] <= '2021-12-31')
    
    train_df = df[(~mask_pandemic) & (df['y'].notnull())].copy()
    
    # 鲁棒性填充：确保训练集没有 NaN
    for col in features:
        train_df[col] = train_df[col].fillna(0)
    
    X_train = train_df[features]
    y_train = train_df['y']
    
    # 配置 XGBoost 快速回归器
    model = XGBRegressor(
        n_estimators=500, 
        learning_rate=0.05,
        max_depth=5,
        n_jobs=-1,
        random_state=42
    )
    model.fit(X_train, y_train)
    
    # 准备目标日期的特征向量
    target_date = pd.to_datetime(target_date_str)
    
    # 从主数据集中提取静态特征（天气、节日等）
    target_row = df[df['ds'] == target_date]
    if target_row.empty:
        # 如果是远期日期且尚未生成特征骨架，采用保守默认值
        weather_idx, is_h, is_sb, is_h_exact, is_h_window, lag_7_val, lag_364_val = 0,0,0,0,0,0,0
    else:
        target_data = target_row.iloc[0]
        weather_idx = target_data.get('weather_index', 0)
        is_h = target_data.get('is_holiday', 0)
        is_sb = target_data.get('is_spring_break', 0)
        is_h_exact = target_data.get('is_holiday_exact_day', 0)
        is_h_window = target_data.get('is_holiday_travel_window', 0)
        lag_7_val = target_data.get('throughput_lag_7', 0)
        # 去年同日滞后项需特殊回溯寻找
        lag_364_val = df[df['ds'] == (target_date - timedelta(days=364))]['y'].values[0] if not df[df['ds'] == (target_date - timedelta(days=364))].empty else 0

    # 生成基础日历特征
    day_of_week = target_date.dayofweek
    month = target_date.month
    is_weekend = 1 if day_of_week >= 5 else 0
    
    # 从数据库实时获取当天的真实航班总量（该数据由 fetch_opensky.py 维持实时性）
    conn = get_db_connection()
    row = conn.execute("SELECT SUM(arrival_count) FROM flight_stats WHERE date = ?", (target_date_str,)).fetchone()
    conn.close()
    
    flight_volume = row[0] if row and row[0] else 0
    
    # 容错逻辑：如果当天没有抓取到航班（如 API 封禁或延迟），启用后备均值模式，确保结果不崩溃。
    is_fallback = False
    if flight_volume == 0:
        avg_flights = df['total_flights'].mean()
        # 尝试拿昨天的飞行量作为替代预测依据
        yesterday_str = (target_date - timedelta(days=1)).strftime("%Y-%m-%d")
        conn = get_db_connection()
        row_y = conn.execute("SELECT SUM(arrival_count) FROM flight_stats WHERE date = ?", (yesterday_str,)).fetchone()
        conn.close()
        flight_volume = row_y[0] if row_y and row_y[0] else avg_flights
        is_fallback = True
        
    # 打包输入特征向量
    X_target = pd.DataFrame([{
        'day_of_week': day_of_week,
        'month': month,
        'is_weekend': is_weekend,
        'flight_current': flight_volume,
        'weather_index': weather_idx,
        'is_holiday': is_h,
        'is_spring_break': is_sb,
        'is_holiday_exact_day': is_h_exact,
        'is_holiday_travel_window': is_h_window,
        'lag_7': lag_7_val,
        'lag_364': lag_364_val
    }])
    
    # 最终预测执行
    pred = model.predict(X_target)[0]
    
    return {
        "date": target_date_str,
        "predicted_throughput": int(pred),
        "flight_volume": int(flight_volume),
        "is_fallback": is_fallback,
        "model": "Sniper V1 (Time-Aware)"
    }

if __name__ == "__main__":
    if len(sys.argv) < 2:
        # 【核心智能切换逻辑】：
        # OpenSky 每日批量更新通常在 北京时间 上午 10:00 完成归档。
        # 1. 如果当前 < 10:00：T-1 (昨天) 数据尚不可读，脚本自动对准 T-2 (前天) 进行狙击。
        # 2. 如果当前 >= 10:00：T-1 (昨天) 数据已就绪，脚本自动对准 T-1 进行狙击。
        
        now = datetime.now()
        hour_threshold = 10
        
        if now.hour < hour_threshold:
            target_date = now - timedelta(days=2)
            print(f"   [智能识别] 当前时间早于 10:00，OpenSky T-1 数据尚未完全归档。")
            print(f"   [目标设定] 自动瞄准 T-2 (前日): {target_date.strftime('%Y-%m-%d')}")
        else:
            target_date = now - timedelta(days=1)
            print(f"   [智能识别] 当前时间已过 10:00，OpenSky T-1 航班数据已出炉。")
            print(f"   [目标设定] 自动瞄准 T-1 (昨日): {target_date.strftime('%Y-%m-%d')}")
            
        target = target_date.strftime("%Y-%m-%d")
    else:
        # 支持通过命令行手动指定日期进行补盲预测。用法: python predict_sniper.py 2026-01-14
        target = sys.argv[1]
        
    try:
        prediction = train_and_predict(target)
        
        # 将结果以标准 JSON 格式输出，供 Flask/Dashboard 调用
        if isinstance(prediction, dict):
            print(json.dumps(prediction))
    except Exception as e:
        print(json.dumps({"error": str(e)}))
