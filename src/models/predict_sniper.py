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
    # 特征工程：生成对预测具有显著影响的自变量
    df['day_of_week'] = df['ds'].dt.dayofweek
    df['month'] = df['ds'].dt.month
    df['year'] = df['ds'].dt.year
    df['day_of_year'] = df['ds'].dt.dayofyear
    df['week_of_year'] = df['ds'].dt.isocalendar().week.astype(int)
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)

    # [NEW] Business Logic Feature (Off-Peak Workday)
    match_month = df['ds'].dt.month.isin([1, 2, 9, 10])
    match_day = df['ds'].dt.dayofweek.isin([1, 2]) # Tue, Wed
    df['is_off_peak_workday'] = (match_month & match_day).astype(int)
    
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
    df['weather_lag_1'] = df['weather_index'].shift(1).fillna(0)
    
    # [NEW] Revenge Travel Index (Sync with train_xgb.py)
    df['w_lag_1'] = df['weather_index'].shift(1).fillna(0)
    df['w_lag_2'] = df['weather_index'].shift(2).fillna(0)
    df['w_lag_3'] = df['weather_index'].shift(3).fillna(0)
    df['revenge_index'] = (df['w_lag_1'] * 0.5) + (df['w_lag_2'] * 0.3) + (df['w_lag_3'] * 0.2)
    
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
        'day_of_week', 'month', 'year', 'day_of_year', 'week_of_year', 'is_weekend', 
        'is_off_peak_workday', # New
        'flight_current', 
        'weather_index', 'is_holiday', 'is_spring_break',
        'is_holiday_exact_day', 'days_to_nearest_holiday',
        'revenge_index', 'is_long_weekend',
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
    
    # [FIX] Decouple from main forecast model (train_xgb.py uses incompatible features)
    model_file = SNIPER_MODEL_PATH
    is_loaded = False
    
    # Try to load dedicated Sniper model
    try:
        model = XGBRegressor()
        model.load_model(model_file)
        # Check feature consistency implicitly by successful prediction later? 
        # XGBoost doesn't strictly check names on load, only on predict.
        is_loaded = True
    except Exception as e:
        # print(f"   [Sniper] Persistence Load Failed ({e}). Falling back to live training.")
        pass # Silent fallback
        
    # Force Retrain if features mismatch or file missing (cleanest approach for stability)
    # Given the previous error, let's prioritize on-the-fly training to ensure feature alignment.
    # It takes < 2 seconds for this data size.
    
    # Define Model
    model = XGBRegressor(
        n_estimators=500, 
        learning_rate=0.05,
        max_depth=5,
        n_jobs=-1,
        random_state=42
    )
    
    # Train
    model.fit(X_train, y_train)
    
    # Save for next time (Self-Healing)
    try:
        model.save_model(model_file)
    except:
        pass
    
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

        # [FIX] Force-fetch Weather Index from DB if 0 (handling future dates not in traffic_full)
        if weather_idx == 0:
            try:
                print(f"   [Weather Fix] Attempting to fetch weather index for {target_date_str} from daily_weather_index...")
                conn_w = get_db_connection()
                # Check if table exists first to be safe
                rw_chk = conn_w.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='daily_weather_index'").fetchone()
                if not rw_chk:
                    print("   [Weather Fix] daily_weather_index table does not exist!")
                else:
                    row_w = conn_w.execute("SELECT weather_index FROM daily_weather_index WHERE date = ?", (target_date_str,)).fetchone()
                    
                    if row_w:
                         # Handle sqlite.Row or tuple
                        val = row_w['weather_index'] if isinstance(row_w, sqlite3.Row) else row_w[0]
                        if val is not None:
                            weather_idx = int(val)
                            print(f"   [Weather Fix] Success! Real-time weather index: {weather_idx}")
                        else:
                            print("   [Weather Fix] Found row but index_value is None.")
                    else:
                        print(f"   [Weather Fix] No weather data found for {target_date_str}.")
                conn_w.close()
            except Exception as e:
                print(f"   [Weather Fix] Critical Error: {e}")

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
            
        # [NEW] Real-time Revenge Index
        revenge_val = 0
        try:
            conn_rev = get_db_connection()
            # Fetch last 3 days weather
            d1 = (target_date - timedelta(days=1)).strftime("%Y-%m-%d")
            d2 = (target_date - timedelta(days=2)).strftime("%Y-%m-%d")
            d3 = (target_date - timedelta(days=3)).strftime("%Y-%m-%d")
            
            # Use daily_weather_index for most accurate source
            # We can use traffic_full if we trust updated backfill, but daily_weather_index is source of truth.
            rows = conn_rev.execute(f"SELECT date, index_value FROM daily_weather_index WHERE date IN ('{d1}', '{d2}', '{d3}')").fetchall()
            conn_rev.close()
            
            w_map = {r[0]: r[1] for r in rows}
            w1 = w_map.get(d1, 0)
            w2 = w_map.get(d2, 0)
            w3 = w_map.get(d3, 0)
            
            revenge_val = (w1 * 0.5) + (w2 * 0.3) + (w3 * 0.2)
            # print(f"   [Revenge Index] {d1}:{w1}, {d2}:{w2}, {d3}:{w3} -> {revenge_val}")
        except Exception as e:
            print(f"   [Error] Failed revenge index calc: {e}")
            revenge_val = 0

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
                if count and count >= 10:
                    total_jit += count
                    jit_data.append((target_date_str, icao, count))
                elif count:
                    print(f"   [Sniper-JIT] 丢弃低质量数据: {icao} ({count} 架次)")
            
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
    # 打包输入特征向量
    # Business Feature calc for target
    t_month = target_date.month
    t_dow = target_date.dayofweek
    is_off_peak = 1 if (t_month in [1,2,9,10] and t_dow in [1,2]) else 0
    
    X_target = pd.DataFrame([{
        'day_of_week': day_of_week,
        'month': month,
        'year': target_date.year,
        'day_of_year': target_date.dayofyear,
        'week_of_year': target_date.isocalendar()[1],
        'is_weekend': is_weekend,
        'is_off_peak_workday': is_off_peak,
        'flight_current': flight_volume,
        'weather_index': weather_idx,
        'is_holiday': is_h,
        'is_spring_break': is_sb,
        'is_holiday_exact_day': is_h_exact,
        'days_to_nearest_holiday': days_to_holiday_val,
        'revenge_index': revenge_val,
        'is_long_weekend': is_long_val,
        'lag_7': lag_7_val,
        'lag_364': lag_364_val
    }])
    
    # 最终预测执行
    pred = model.predict(X_target)[0]

    # [NEW] Flight Cancellation Velocity & Blindness Logic
    # 1. Calculate Baseline (MA30)
    conn = get_db_connection()
    # Get last 30 days of flight volumes (excluding today)
    past_30_query = f"""
        SELECT AVG(arrival_count) 
        FROM (
            SELECT arrival_count FROM flight_stats 
            WHERE date < '{target_date_str}' 
            ORDER BY date DESC LIMIT 30
        )
    """
    row_ma = conn.execute(past_30_query).fetchone()
    conn.close()
    
    ma_30_flights = row_ma[0] if row_ma and row_ma[0] else 5000 # Default fallback
    
    # 2. Calculate Velocity
    cancel_velocity = 0.0
    if ma_30_flights > 0:
        cancel_velocity = 1.0 - (flight_volume / ma_30_flights)
    
    # 3. Detect Data Outage (Blindness)
    is_data_outage = False
    if flight_volume < 100 or is_fallback:
        is_data_outage = True
        print(f"   [⚠️ 数据盲区] 航班量极低或已触发降级 (Vol={flight_volume}, Fallback={is_fallback})")

    # 4. Apply Dynamic Circuit Breaker
    circuit_breaker_triggered = False
    original_pred = pred
    final_penalty = 1.0
    reason = ""

    # Strategy A: Blind Flight Protocol (Trust Weather Double)
    if is_data_outage:
        if weather_idx >= 35:
            final_penalty = 0.60 # -40% (Relaxed from -50%)
            reason = f"盲飞模式 + 极端天气 (Idx={weather_idx}) -> 下调 40%"
        elif weather_idx >= 20: 
            final_penalty = 0.80 # -20% (Relaxed from -30%)
            reason = f"盲飞模式 + 严重天气 (Idx={weather_idx}) -> 下调 20%"
        elif weather_idx >= 15:
            final_penalty = 0.90 # -10% (Relaxed from -15%)
            reason = f"盲飞模式 + 中度天气 (Idx={weather_idx}) -> 下调 10%"
        else:
            reason = "盲飞模式 + 天气正常 -> 无需调整"

    # Strategy B: Visible Flight Cancellation (Trust Reality)
    else:
        # Priority 1: High Cancellation Rate (Hard Evidence)
        if cancel_velocity >= 0.50:
            final_penalty = 0.50 # -50% (Massive Cancellations)
            reason = f"航班由于天气/其他原因腰斩 (取消率 {cancel_velocity:.1%}) -> 下调 50%"
        elif cancel_velocity >= 0.20:
            final_penalty = 0.80 # -20% (Significant Cancellations)
            reason = f"航班取消率较高 ({cancel_velocity:.1%}) -> 下调 20%"
        
        # Priority 2: Weather Index (if flights look normal but weather is scary?)
        # Only apply weather penalty if it's STRONGER than flight penalty
        # usually weather implies flight cancel, but if flights are somehow full?
        # We take the MIN (worst case) of penalties.
        
        w_penalty = 1.0
        w_reason = ""
        if weather_idx >= 35:
            w_penalty = 0.70
            w_reason = f"极端天气 (Idx={weather_idx})"
        elif weather_idx >= 20:
            w_penalty = 0.85
            w_reason = f"严重天气 (Idx={weather_idx})"
            
        if w_penalty < final_penalty:
            final_penalty = w_penalty
            reason = f"{w_reason} [覆盖航班数据] -> 下调 {int((1-final_penalty)*100)}%"

    # Apply Final Penalty
    if final_penalty < 1.0:
        pred = pred * final_penalty
        circuit_breaker_triggered = True
        print(f"   [🛡️ 熔断生效] {reason}")

    return {
        "date": target_date_str,
        "predicted_throughput": int(pred),
        "flight_volume": int(flight_volume),
        "cancel_velocity": round(cancel_velocity, 2),
        "is_data_outage": is_data_outage,
        "model": "Sniper V1 (Blind-Fight Capable)",
        "original_prediction": int(original_pred) if circuit_breaker_triggered else None,
        "weather_index_used": int(weather_idx),
        "modification_reason": reason if circuit_breaker_triggered else None
    }

if __name__ == "__main__":
    if len(sys.argv) < 2:
        # 【核心智能切换逻辑 (Smart Sensing)】：
        #不再依赖硬编码的时间 (10:00)，而是直接询问数据库最新数据到了哪一天。
        
        # [SMART GAP DETECTION LOGIC]
        # 目标：找到“有航班数据”但“还没有TSA数据”的那一天。
        # 逻辑：Target = Max(TSA_Date) + 1 Day.
        
        try:
            conn = sqlite3.connect(DB_PATH)
            
            # 1. Get latest TSA data (Ground Truth)
            # traffic_full contains the merged data with 'throughput'.
            # traffic_full's max date is the last day we KNOW the answer for.
            row_tsa = conn.execute("SELECT MAX(date) FROM traffic_full WHERE throughput IS NOT NULL").fetchone()
            max_tsa_date_str = row_tsa[0] if row_tsa else '2020-01-01'
            
            # 2. Get latest Flight data (OpenSky)
            row_flight = conn.execute("SELECT MAX(date) FROM flight_stats").fetchone()
            max_flight_date_str = row_flight[0] if row_flight else None
            
            conn.close()
            
            print(f"   [智能感知] TSA最新: {max_tsa_date_str} | OpenSky最新: {max_flight_date_str}")
            
            if not max_flight_date_str:
                # No flight data at all, fallback to yesterday
                target_date = datetime.now() - timedelta(days=1)
                print(f"   [目标设定] 无航班数据，默认 T-1: {target_date.strftime('%Y-%m-%d')}")
            else:
                # Calculate the Gap Date
                max_tsa_dt = datetime.strptime(max_tsa_date_str, "%Y-%m-%d")
                gap_date = max_tsa_dt + timedelta(days=1)
                gap_date_str = gap_date.strftime("%Y-%m-%d")
                
                # Check if we have flights for this gap date
                # Actually, even if we don't have flights (e.g. today), Sniper might want to run in JIT mode?
                # But user wants specific logic: "Use the day that HAS flights".
                # So if Gap Date <= Max Flight Date, use Gap Date.
                
                max_flight_dt = datetime.strptime(max_flight_date_str, "%Y-%m-%d")
                
                if gap_date <= max_flight_dt:
                    target_date = gap_date
                    print(f"   [目标设定] 锁定未公布的最近一天 (有航班数据): {gap_date_str}")
                else:
                    # Case: TSA is up to date with Flights (or ahead? unlikely). 
                    # Or Flights are lagging behind TSA (e.g. OpenSky is broken).
                    # In this case, we default to Max Flight Date? 
                    # If TSA=Jan23, Flights=Jan23. Target should be Jan24 (JIT)? 
                    # But user said "Use the day that HAS flights". 
                    # If Flights=Jan23, and TSA=Jan23, then we have NO day with "Flights but no TSA".
                    # In that specific 100% synced case, maybe default to Max Flight Date (Jan23) just to show something?
                    # BUT, predicting Jan 23 when we have Jan 23 actuals is redundant.
                    # Let's target Jan 24 (Gap) and let JIT try to fetch it.
                    target_date = gap_date
                    print(f"   [目标设定] 航班数据未领先 TSA，尝试预测下一天 (JIT模式): {gap_date_str}")

        except Exception as e:
            print(f"   [警告] 智能逻辑失败: {e}，回退至 T-1。")
            target_date = datetime.now() - timedelta(days=1)

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
