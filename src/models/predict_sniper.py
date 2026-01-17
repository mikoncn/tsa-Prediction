# predict_sniper.py - 智能狙击客流预测内核
# 功能：利用“即时”飞行数据（T-1 或 T-2）对 TSA 客流量进行补盲预测。
# 业务逻辑：当官方 TSA 数据尚未公布时，通过天空中的航班流量推算出地面上的旅客人数。

import pandas as pd
import numpy as np
import sqlite3
import warnings
import sys
import os
import json
from datetime import datetime, timedelta
from xgboost import XGBRegressor

# Add src to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.config import DB_PATH, SNIPER_MODEL_PATH

# 禁用无关的警告信息，保持输出整洁
warnings.filterwarnings('ignore')

# DB_PATH = 'tsa_data.db' 
# CSV_PATH = 'TSA_Final_Analysis.csv' # Removed

def get_db_connection():
    """获取数据库连接"""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn

def load_data():
    """
    核心数据对齐逻辑。
    1. 从 CSV 读取官方历史客流数据。
    2. 从 SQLite 读取 OpenSky 记录的各机场航班量。
    3. 按日期进行合并，形成包含“客流+飞行量”的联合训练集。
    """
    # 1. 加载 TSA 客流历史 (From DB)
    conn = get_db_connection()
    df = pd.read_sql("SELECT * FROM traffic_full", conn)
    # Don't close conn yet if we need it later, or just close it.
    # Actually, lines below open conn again. So let's close it or keep it open.
    # But wait, line 40 opens conn again. Let's just use it and close it.
    conn.close()
    
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
    
    # [OPTIMIZATION] Forward Fill for missing flight data
    df['total_flights'] = df['total_flights'].fillna(method='ffill').fillna(0)
    
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
    
    # [NEW] Hybrid Lag Strategy
    import numpy as np
    df['lag_364'] = df['y'].shift(364).fillna(method='bfill')
    df['lag_365'] = df['y'].shift(365).fillna(method='bfill')
    
    # Simple Fixed Holiday Mask (Vectorized)
    mask_fixed = df['ds'].apply(lambda d: 
        (d.month == 1 and d.day == 1) or
        (d.month == 7 and d.day == 4) or
        (d.month == 11 and d.day == 11) or
        (d.month == 12 and d.day == 25)
    )
    df['lag_364'] = np.where(mask_fixed, df['lag_365'], df['lag_364'])
    df.drop(columns=['lag_365'], inplace=True)
    
    # [NEW] Weather Lag 1
    df['weather_lag_1'] = df['daily_weather_index'].shift(1).fillna(0) if 'daily_weather_index' in df.columns else np.zeros(len(df))
    # Wait, 'daily_weather_index' might not be in df? load_data merges flight_stats but maybe not weather table?
    # Let's check load_data. It only fetches 'traffic_full' and 'flight_stats'.
    # traffic_full usually has 'weather_index'. 
    df['weather_lag_1'] = df['weather_index'].shift(1).fillna(0)

    # [NEW] Long Weekend
    df['is_long_weekend'] = 0
    mask_long = (df['is_holiday'] == 1) & (df['day_of_week'].isin([0, 4]))
    df.loc[mask_long, 'is_long_weekend'] = 1

    # [NEW] Whitelist & Clamping Logic for Historical Data
    target_holidays = [
        "New Year's Day", 
        "Martin Luther King Jr. Day", 
        "Washington's Birthday", # Presidents' Day
        "Memorial Day", 
        "Juneteenth National Independence Day", 
        "Independence Day", 
        "Labor Day", 
        "Columbus Day", 
        "Veterans Day", 
        "Thanksgiving", 
        "Christmas Day"
    ]
    
    import holidays
    us_holidays = holidays.US(years=range(2019, 2030))
    major_holiday_dates = []
    
    # 1. Standard
    for date, name in us_holidays.items():
        if any(target in name for target in target_holidays):
            major_holiday_dates.append(pd.Timestamp(date))

    # 2. Good Friday
    from dateutil.easter import easter
    for y in range(2019, 2030):
        easter_date = easter(y)
        good_friday = pd.Timestamp(easter_date) - pd.Timedelta(days=2)
        major_holiday_dates.append(good_friday)

    df['days_to_nearest_holiday'] = 15 # Default
    for idx, row in df.iterrows():
        d = row['ds']
        min_dist = 999 
        best_dist = 15
        for h_date in major_holiday_dates:
            diff_days = (d - h_date).days
            if abs(diff_days) < abs(min_dist):
                min_dist = diff_days
                best_dist = diff_days
        
        if best_dist > 14: best_dist = 15
        elif best_dist < -14: best_dist = -15
        df.at[idx, 'days_to_nearest_holiday'] = best_dist
    
    # 定义模型要用到的全部列（必须与训练和预测完全一致）
    features = [
        'day_of_week', 'month', 'is_weekend', 'flight_current', 
        'weather_index', 'is_holiday', 'is_spring_break',
        'is_holiday_exact_day', 'days_to_nearest_holiday',
        'weather_lag_1', 'is_long_weekend',
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
    
    # [NEW] Model Persistence Logic
    model_file = SNIPER_MODEL_PATH
    is_loaded = False
    
    # Try to load model
    try:
        model = XGBRegressor()
        model.load_model(model_file)
        # Verify if model is valid (optional print)
        # print("   [Sniper] Loaded pre-trained model.")
        is_loaded = True
    except Exception as e:
        print(f"   [Sniper] Persistence Load Failed ({e}). Falling back to live training.")
        
    # Fallback: Train if load failed
    if not is_loaded:
        model = XGBRegressor(
            n_estimators=500, 
            learning_rate=0.05,
            max_depth=5,
            n_jobs=-1,
            random_state=42
        )
        model.fit(X_train, y_train)
    
    # Prepare Target Input
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
        lag_7_val = target_data.get('throughput_lag_7', 0)
        lag_364_val = df[df['ds'] == (target_date - timedelta(days=364))]['y'].values[0] if not df[df['ds'] == (target_date - timedelta(days=364))].empty else 0

        # [NEW] Real-time Day Distance Calculation
        # Whitelist
        target_holidays = [
            "New Year's Day", 
            "Martin Luther King Jr. Day", 
            "Washington's Birthday", # Presidents' Day
            "Memorial Day", 
            "Juneteenth National Independence Day", 
            "Independence Day", 
            "Labor Day", 
            "Columbus Day", 
            "Veterans Day", 
            "Thanksgiving", 
            "Christmas Day"
        ]
        
        # Get holidays for current year + next year (to handle Dec-Jan transitions)
        import holidays
        cur_year = target_date.year
        # [FIX] Add previous year too for Dec-Jan boundary lookback
        us_holidays = holidays.US(years=[cur_year-1, cur_year, cur_year + 1]) 
        
        major_holiday_dates = []
        for date, name in us_holidays.items():
            if any(target in name for target in target_holidays):
                major_holiday_dates.append(pd.Timestamp(date))
        
        # Add Good Friday
        from dateutil.easter import easter
        for y in [cur_year-1, cur_year, cur_year+1]:
            easter_date = easter(y)
            good_friday = pd.Timestamp(easter_date) - pd.Timedelta(days=2)
            major_holiday_dates.append(good_friday)

        min_dist = 999 
        best_dist = 15 # Default clamped
        
        for h_date in major_holiday_dates:
            diff_days = (target_date - h_date).days
            if abs(diff_days) < abs(min_dist):
                min_dist = diff_days
                best_dist = diff_days
        
        # Clamping
        if best_dist > 14: best_dist = 15
        elif best_dist < -14: best_dist = -15
        
        days_to_holiday_val = best_dist
        
        # [NEW] Real-time Weather Lag 1 (Safety Fallback)
        # Fetch yesterday's weather from DB
        try:
            yesterday_str = (target_date - timedelta(days=1)).strftime("%Y-%m-%d")
            conn = get_db_connection()
            # Assuming table is daily_weather_index? Or traffic_full?
            # traffic_full has history. daily_weather_index has raw.
            # Let's query traffic_full first for stability, or daily_weather_index if recent.
            # Best source: daily_weather_index
            row_w = conn.execute("SELECT index_value FROM daily_weather_index WHERE date = ?", (yesterday_str,)).fetchone()
            conn.close()
            
            if row_w and row_w[0] is not None:
                weather_lag_val = row_w[0]
            else:
                # [SAFETY] Data missing -> Assume clean slate (0) to avoid crash
                print(f"   [Warning] Yesterday's weather missing for {yesterday_str}. Using 0.")
                weather_lag_val = 0
        except Exception as e:
             print(f"   [Error] Failed to fetch weather lag: {e}. Using 0.")
             weather_lag_val = 0
             
        # [NEW] Real-time Long Weekend
        is_long_val = 0
        # Re-calc is_holiday for target? We have is_h from 'target_data' lookup usually.
        # But wait, target_data comes from df which is history. 
        # If target is future, we need to know if it IS a holiday.
        # We did lookup 'is_holiday' (is_h) earlier from target_row.
        if is_h == 1 and target_date.dayofweek in [0, 4]:
            is_long_val = 1

    # 生成基础日历特征
    day_of_week = target_date.dayofweek
    month = target_date.month
    is_weekend = 1 if day_of_week >= 5 else 0
    
    # 从数据库实时获取当天的真实航班总量
    conn = get_db_connection()
    row = conn.execute("SELECT SUM(arrival_count) FROM flight_stats WHERE date = ?", (target_date_str,)).fetchone()
    conn.close()
    
    flight_volume = row[0] if row and row[0] else 0
    
    # [SMART SNIPER LOGIC]
    # 如果数据库里的数据少得离谱 (比如只有 17 条)，说明数据可能不完整或未抓取。
    # 此时应触发“即时抓取 (Just-in-Time Fetch)”，而不是直接用错误的 17 去预测。
    if flight_volume < 500: # 正常阈值通常 > 3000
        print(f"   [JIT触发] 检测到航班量异常偏低 ({flight_volume})，尝试现场抓取...")
        try:
            # 动态导入防止循环引用
            import fetch_opensky
            
            # [FIX] 响应用户需求，保持与主抓取逻辑一致，使用全部 Top 10 机场
            # 而不是仅抓取 Top 5。虽然速度稍慢，但数据口径完全统一。
            top_airports = fetch_opensky.AIRPORTS
            
            for icao in top_airports:
                count = fetch_opensky.fetch_arrival_count(target_date_str, icao)
                if count:
                    total_jit += count
                    jit_data.append((target_date_str, icao, count))
            
            if total_jit > flight_volume:
                print(f"   [JIT成功] 现场抓取到 {total_jit} 架次，更新数据库...")
                flight_volume = total_jit * 2 # 粗略估算：Top 5 约占总量的 50%? 或者只用 Top 5 代表趋势。
                # 更稳妥：把抓到的存入 DB，再次查询 sum
                fetch_opensky.save_to_db(jit_data)
                
                # Re-query distinct sum from DB to be accurate
                conn = get_db_connection()
                row = conn.execute("SELECT SUM(arrival_count) FROM flight_stats WHERE date = ?", (target_date_str,)).fetchone()
                if row and row[0]:
                    flight_volume = row[0]
                conn.close()
                
        except Exception as e:
            print(f"   [JIT失败] 现场抓取遇到问题: {e}")

    # 容错逻辑：如果 JIT 后依然为 0 (如 API 429)，则启用后备均值模式
    is_fallback = False
    if flight_volume < 100: # 依然过低
        print(f"   [降级模式] 航班数据 ({flight_volume}) 不足以支撑预测，切换至历史均值。")
        avg_flights = df['total_flights'].mean()
        # 尝试拿昨天的飞行量作为替代预测依据
        yesterday_str = (target_date - timedelta(days=1)).strftime("%Y-%m-%d")
        conn = get_db_connection()
        row_y = conn.execute("SELECT SUM(arrival_count) FROM flight_stats WHERE date = ?", (yesterday_str,)).fetchone()
        conn.close()
        # 如果昨天有数，用昨天的；否则用历史平均
        flight_volume = row_y[0] if (row_y and row_y[0] is not None and row_y[0] > 500) else avg_flights
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
        'days_to_nearest_holiday': days_to_holiday_val,
        'weather_lag_1': weather_lag_val,
        'is_long_weekend': is_long_val,
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
