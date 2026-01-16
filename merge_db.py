import pandas as pd
import sqlite3
import datetime
from dateutil.easter import easter
import numpy as np

# 配置
DB_PATH = 'tsa_data.db'
WEATHER_CSV = 'weather_features.csv'

def get_db_connection():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn

def main():
    print("=== 开始数据库合并工程 (Timeline Strategy) ===")
    
    conn = get_db_connection()
    
    # 1. 读取各路数据
    print("1. 读取原始数据...")
    df_traffic = pd.read_sql("SELECT date, throughput FROM traffic", conn)
    # 1. 读取原始数据...
    print("1. 读取原始数据...")
    df_traffic = pd.read_sql("SELECT date, throughput FROM traffic", conn)
    
    # [NEW] 在内存中实时生成基础节日特征 (不再依赖外部表)
    print("1.5 生成基础节日特征 (US Holidays)...")
    import holidays
    
    # [NEW] Advanced Holiday Logic (Refined)
    us_holidays = holidays.US(years=range(2018, 2029))
    
    # 1. Exact Day List (The "Stay at Home" days)
    major_holidays = ['Christmas Day', 'Thanksgiving', 'Independence Day']
    
    # 临时存储 expanded events
    expanded_events = [] 
    
    for date, name in us_holidays.items():
        date_obj = pd.Timestamp(date)
        
        # Check if it's a Major Holiday
        is_major = any(m in name for m in major_holidays)
        
        # 1. Mark Exact Day (Original Holiday)
        expanded_events.append({
            'date': date_obj,
            'is_holiday': 1,
            'is_holiday_exact_day': 1 if is_major else 0, # Only major ones get the "penalty" flag
            'is_holiday_travel_window': 0, 
            'holiday_name': name
        })
        
        # 2. Generate Window (Only for Major Holidays)
        if is_major:
            for offset in range(-7, 8):
                if offset == 0: continue # Skip exact day
                window_date = date_obj + pd.Timedelta(days=offset)
                
                expanded_events.append({
                    'date': window_date,
                    'is_holiday': 0, # Window itself is not a legal holiday (unless overlap)
                    'is_holiday_exact_day': 0,
                    'is_holiday_travel_window': 1,
                    'holiday_name': f"Travel Window ({name})"
                })

    # Convert to DataFrame
    df_h_temp = pd.DataFrame(expanded_events)
    
    # Aggregation rules: Take MAX for flags to handle overlaps
    df_holiday = df_h_temp.groupby('date').agg({
        'is_holiday': 'max',
        'is_holiday_exact_day': 'max',
        'is_holiday_travel_window': 'max',
        'holiday_name': lambda x: ', '.join(set([str(v) for v in x])) # Join names
    }).reset_index()

    try:
        df_weather = pd.read_csv(WEATHER_CSV)
    except Exception as e:
        print(f"Error reading weather CSV: {e}")
        return

    # 规范化日期格式
    for df in [df_traffic, df_holiday, df_weather]:
        df['date'] = pd.to_datetime(df['date'])

    # 2. 构建全量时间骨架 (Skeleton)
    print("2. 构建时间骨架 (2019-01-01 ~ Future)...")
    start_date = '2019-01-01'
    # 结束日期取天气的最后一天 (通常是未来15天)
    end_date = df_weather['date'].max()
    print(f"   时间范围: {start_date} 到 {end_date}")
    
    df_skeleton = pd.DataFrame({'date': pd.date_range(start=start_date, end=end_date)})
    
    # 3. 合并 (Left Join onto Skeleton)
    print("3. 执行合并 (Left Join)...")
    df_full = df_skeleton.merge(df_traffic, on='date', how='left')
    df_full = df_full.merge(df_holiday, on='date', how='left')
    df_full = df_full.merge(df_weather, on='date', how='left')
    
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
    
    # 5. 注入复活节补丁 (Easter Patch)
    print("4. 注入复活节补丁 (Good Fri ~ Easter Mon)...")
    years = df_full['date'].dt.year.unique()
    easter_dates = []
    
    for year in years:
        e_sun = pd.Timestamp(easter(year))
        # 窗口: Good Friday(-2), Holy Saturday(-1), Sunday(0), Monday(+1)
        window = [e_sun - pd.Timedelta(days=2), 
                  e_sun - pd.Timedelta(days=1),
                  e_sun, 
                  e_sun + pd.Timedelta(days=1)]
        easter_dates.extend(window)
        
    # 标记复活节
    # 注意: 如果原本已经是节日(比如和其他联邦假日重叠)，这里会覆盖，或者保留?
    # 通常 Easter 期间没别的联邦假日，直接覆盖比较安全
    mask_easter = df_full['date'].isin(easter_dates)
    df_full.loc[mask_easter, 'is_holiday'] = 1
    # 仅当原名为为空时才填 Easter，或者强行覆盖? 
    # 考虑到 Easter 优先级很高，且基本不重叠，我们做个追加或覆盖
    # 为简单起见，显示 "Easter Group"
    df_full.loc[mask_easter, 'holiday_name'] = 'Easter Group'
    
    # 5.5 注入超级碗补丁 (Super Bowl Patch)
    print("5. 注入超级碗 (Super Bowl Sunday & Monday)...")
    sb_dates = []
    
    for year in years:
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
        
    mask_sb = df_full['date'].isin(sb_dates)
    # 标记为节日
    df_full.loc[mask_sb, 'is_holiday'] = 1
    # 覆盖名称 (Super Bowl 优先级很高，值得覆盖)
    df_full.loc[mask_sb, 'holiday_name'] = 'Super Bowl Group'
    
    # 6. 注入春假补丁 (Spring Break)
    # 逻辑: 3/4月 + 周末 + (不是节日/复活节)
    print("5. 计算春假特征 (Spring Break)...")
    df_full['month'] = df_full['date'].dt.month
    df_full['day_of_week'] = df_full['date'].dt.dayofweek # 0=Mon, 6=Sun
    
    # 条件
    cond_month = df_full['month'].isin([3, 4])
    cond_weekend = df_full['day_of_week'].isin([5, 6]) # Sat, Sun
    cond_not_holiday = (df_full['is_holiday'] == 0)
    
    mask_sb = cond_month & cond_weekend & cond_not_holiday
    
    df_full['is_spring_break'] = 0
    df_full.loc[mask_sb, 'is_spring_break'] = 1
    
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
    cols_to_keep = ['date', 'throughput', 'weather_index', 'is_holiday', 'holiday_name', 'is_holiday_exact_day', 'is_holiday_travel_window', 'is_spring_break', 'throughput_lag_7']
    final_df = df_full[cols_to_keep].copy()
    
    # 转换 date 为 string 存入 sqlite
    final_df['date'] = final_df['date'].dt.strftime('%Y-%m-%d')
    
    final_df.to_sql('traffic_full', conn, if_exists='replace', index=False)
    
    # NEW: 导出 CSV 供分析使用 (替代 export_table.py)
    print("7.5 导出 CSV (TSA_Final_Analysis.csv) ...")
    final_df.to_csv('TSA_Final_Analysis.csv', index=False, encoding='utf-8-sig')
    
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
    main()
