import pandas as pd
import sqlite3
import datetime
from dateutil.easter import easter
import numpy as np

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.config import DB_PATH

# 配置
# DB_PATH = 'tsa_data.db'
WEATHER_CSV = 'weather_features.csv'

def get_db_connection():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn

def run():
    print("=== 开始数据库合并工程 (Timeline Strategy) ===")
    
    conn = get_db_connection()
    
    # ... (rest of the code is inside run now) ...
    # Be careful not to delete the content, just the def line change and indentation if needed.
    # Actually, REPLACE renaming main to run is easier if I just match the def main(): line.
    
    # Wait, replace_file_content requires exact match.
    # I should just replace `def main():` with `def run():` and the `if __name__` block.
    
    # But wait, python indentation...
    # If the original code was inside `main()`, it is already indented.
    # So renaming `def main():` to `def run():` works perfectly.
    pass

# Actually I will just do the replace.

    df_traffic = pd.read_sql("SELECT date, throughput FROM traffic", conn)
    # 1. 读取原始数据...
    print("1. 读取原始数据...")
    df_traffic = pd.read_sql("SELECT date, throughput FROM traffic", conn)
    
    # [NEW] 使用统一的 Holiday Utils (Single Source of Truth)
    print("1.5 生成基础节日特征 (Holiday Utils)...")
    from src.utils.holiday_utils import get_holiday_features
    
    # Generate for a wide range to cover all potential data
    # (2018 to 2030 covers training and forecast)
    holiday_dates = pd.date_range(start='2018-01-01', end='2030-12-31')
    df_feat = get_holiday_features(holiday_dates)
    df_holiday = df_feat.copy()
    df_holiday['date'] = holiday_dates
    
    # Columns to merge: date, is_holiday, ...
    # Note: get_holiday_features returns the feature cols. We added 'date'.
    
    # (Old Aggregation logic is handled inside get_holiday_features implicitly by row-based generation)
    
 
    # 1.5 Load Weather from DB (Replacing CSV)

    # 1.5 Load Weather from DB (Replacing CSV)
    print("1. Loading Weather Data from DB...")
    try:
        # [NEW] Read from daily_weather_index table
        df_weather = pd.read_sql("SELECT date, weather_index FROM daily_weather_index", conn)
    except Exception as e:
        print(f"Error reading weather table: {e}")
        # Fallback empty df if table doesn't exist yet
        df_weather = pd.DataFrame(columns=['date', 'weather_index'])

    # 规范化日期格式
    for df in [df_traffic, df_holiday, df_weather]:
        df['date'] = pd.to_datetime(df['date'])

    # 2. 构建全量时间骨架 (Skeleton - Robust Union)
    print("2. 构建时间骨架 (Robust Union Mode)...")
    
    # 获取各数据源的日期范围
    dates_traffic = set(df_traffic['date'].dt.date) if 'date' in df_traffic else set()
    dates_weather = set(df_weather['date'].dt.date) if 'date' in df_weather else set()
    
    # 临时读取 flight_stats 以获取其日期范围 (前面没读)
    try:
         df_flights_tmp = pd.read_sql("SELECT date FROM flight_stats GROUP BY date", conn)
         df_flights_tmp['date'] = pd.to_datetime(df_flights_tmp['date'])
         dates_flights = set(df_flights_tmp['date'].dt.date)
    except:
         dates_flights = set()

    # 计算并集
    all_dates = dates_traffic.union(dates_weather).union(dates_flights)
    
    if not all_dates:
        # Fallback
        start_date = pd.Timestamp('2019-01-01')
        end_date = pd.Timestamp.now() + pd.Timedelta(days=15)
        full_range = pd.date_range(start=start_date, end=end_date)
    else:
        min_date = min(all_dates)
        max_date = max(all_dates)
        # 扩展到未来 15 天 (确保有预测空间)
        target_end = pd.Timestamp.now().date() + pd.Timedelta(days=15)
        if max_date < target_end:
            max_date = target_end
            
        full_range = pd.date_range(start=min_date, end=max_date)

    print(f"   时间范围: {full_range.min().date()} 到 {full_range.max().date()}")
    df_skeleton = pd.DataFrame({'date': full_range})
    
    # 3. 合并 (Left Join onto Skeleton)
    print("3. 执行合并 (Left Join)...")
    df_full = df_skeleton.merge(df_traffic, on='date', how='left')
    df_full = df_full.merge(df_holiday, on='date', how='left')
    # Merge weather (inner join logic effectively, but left to keep skeleton)
    df_full = df_full.merge(df_weather, on='date', how='left')

    # [NEW] Merge Flight Data (OpenSky)
    print("3.5 合并航班数据 (OpenSky)...")
    try:
        # Load flight stats (summing over airports per date)
        df_flights = pd.read_sql("SELECT date, SUM(arrival_count) as flight_volume FROM flight_stats GROUP BY date", conn)
        df_flights['date'] = pd.to_datetime(df_flights['date'])
        
        # [REMOVED] Thresholding logic. Even partial data should be shown in Raw Matrix.
        # df_flights = df_flights[df_flights['flight_volume'] > 2000]
        
        # Calculate 7-day Moving Average for flights (Baseload)
        df_flights = df_flights.sort_values('date')
        df_flights['flight_ma_7'] = df_flights['flight_volume'].rolling(window=7, min_periods=1).mean()
        df_flights['flight_lag_1'] = df_flights['flight_volume'].shift(1)
        
        df_full = df_full.merge(df_flights, on='date', how='left')
        df_full['flight_volume'] = df_full['flight_volume'].fillna(0).astype(int)
        df_full['flight_ma_7'] = df_full['flight_ma_7'].fillna(0).astype(int)
        df_full['flight_lag_1'] = df_full['flight_lag_1'].fillna(0).astype(int)
        
    except Exception as e:
        print(f"Warning: Could not merge flight data: {e}")
        df_full['flight_volume'] = 0
        df_full['flight_ma_7'] = 0
        df_full['flight_lag_1'] = 0
    
    # 4. 基础清洗
    # 填充 weather_index (NaN -> 0)
    df_full['weather_index'] = df_full['weather_index'].fillna(0).astype(int)
    # 填充 is_holiday (NaN -> 0), holiday_name (NaN -> '')
    df_full['is_holiday'] = df_full['is_holiday'].fillna(0).astype(int)
    df_full['holiday_name'] = df_full['holiday_name'].fillna('')
    # [NEW] Fill Advanced Holiday Features
    df_full['is_holiday_exact_day'] = df_full['is_holiday_exact_day'].fillna(0).astype(int)
    df_full['is_holiday_travel_window'] = df_full['is_holiday_travel_window'].fillna(0).astype(int)
    df_full['holiday_name'] = df_full['holiday_name'].fillna('')
    
    # 4. 注入超级碗补丁 (Super Bowl Patch)
    print("4. 注入超级碗 (Super Bowl Sunday & Monday)...")
    years = df_full['date'].dt.year.unique()
    sb_dates = []
    
    for year in years:
        try:
            # 规则: 2月的第2个周日
            # 1. 找到2月1日
            feb_first = pd.Timestamp(year=year, month=2, day=1)
            # 2. 找到第一个周日 (dayofweek: Mon=0, Sun=6)
            days_to_first_sunday = (6 - feb_first.dayofweek) % 7
            first_sunday = feb_first + pd.Timedelta(days=days_to_first_sunday)
            # 3. 第二个周日 = 第一个周日 + 7天
            sb_sunday = first_sunday + pd.Timedelta(days=7)
            sb_monday = sb_sunday + pd.Timedelta(days=1)
            
            sb_dates.extend([sb_sunday, sb_monday])
        except Exception:
            continue
        
    mask_sb = df_full['date'].isin(sb_dates)
    # 标记为节日
    df_full.loc[mask_sb, 'is_holiday'] = 1
    # 覆盖名称 (Super Bowl 优先级很高，值得覆盖)
    df_full.loc[mask_sb, 'holiday_name'] = 'Super Bowl Group'
    
    # 7. Lag Features (滞后特征)
    # 对于 Prophet 来说，Lag 特征需要 shift
    # 注意: throughput 在未来是 NaN，Shift 之后未来几天会有值(来自过去)，但再远就没有了
    # 这里先加上 lag_7, 仅供参考
    print("6. 生成滞后特征 (Lag-7)...")
    df_full['throughput_lag_7'] = df_full['throughput'].shift(7)
    
    # 8. 存入数据库
    print("7. 存入数据库表 traffic_full ...")
    # 清理临时列
    # 清理临时列
    cols_to_keep = ['date', 'throughput', 'weather_index', 'is_holiday', 'holiday_name', 
                    'is_holiday_exact_day', 'is_holiday_travel_window', 'is_spring_break', 
                    'throughput_lag_7', 'flight_volume', 'flight_ma_7', 'flight_lag_1']
    final_df = df_full[cols_to_keep].copy()
    
    # 转换 date 为 string 存入 sqlite
    final_df['date'] = final_df['date'].dt.strftime('%Y-%m-%d')
    
    final_df.to_sql('traffic_full', conn, if_exists='replace', index=False)
    
    # NEW: 停止导出 CSV (Removed export_table.py logic)
    print("7.5 [Migration] CSV Export Disabled. Data saved to traffic_full table.")
    # final_df.to_csv('TSA_Final_Analysis.csv', index=False, encoding='utf-8-sig')
    
    # 9. 验证
    print("\n=== 验证阶段 ===")
    print(f"Total Rows: {len(final_df)}")
    
    # 验证 2026-01-16 (未来)
    future_row = final_df[final_df['date'] == '2026-01-16']
    print("\n[Check Future 2026-01-16]:")
    print(future_row.to_string(index=False))
    
    # 验证 2022-12-22 (核弹)
    # [Validation Update] Show new cols
    cols_val = ['date', 'throughput', 'is_holiday', 'is_holiday_exact_day', 'is_holiday_travel_window', 'holiday_name']
    bomb_row = final_df[final_df['date'] == '2022-12-22']
    print("\n[Check Bomb Cyclone 2022-12-22]:")
    print(bomb_row[cols_val].to_string(index=False))
    
    # 验证 复活节优先级 (2024-03-31 Easter Sunday)
    easter_row = final_df[final_df['date'] == '2024-03-31']
    print("\n[Check Easter Priority 2024-03-31]:")
    print(easter_row.to_string(index=False))

    # 验证 超级碗 (2024-02-11 Super Bowl Sunday)
    sb_row = final_df[final_df['date'] == '2024-02-11']
    print("\n[Check Super Bowl 2024-02-11]:")
    print(sb_row.to_string(index=False))
    # 应该显示 holiday_name='Easter Group', is_spring_break=0 (尽管是3月周末)
    
    conn.close()
    print("\nDone.")

if __name__ == "__main__":
    run()
