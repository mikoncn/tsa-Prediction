import sqlite3
import pandas as pd
import holidays
import datetime

# 数据库配置
DB_NAME = "tsa_data.db"
TABLE_NAME = "traffic"
OUTPUT_TABLE = "traffic_features"
OUTPUT_CSV = "tsa_data_with_features.csv"

def get_holiday_priority(name):
    """
    根据节日名称返回 Tier 优先级和窗口大小
    Returns: (priority, window_size)
    """
    name_lower = name.lower()
    
    # Tier 1: 核弹级 (±7天)
    if any(k in name_lower for k in ["thanksgiving", "christmas"]):
        return 100, 7
        
    # Tier 2: 长周末级 (±3天)
    # New Year, Memorial, Independence, Labor, Martin Luther King
    if any(k in name_lower for k in ["new year", "memorial", "independence", "labor", "martin luther king"]):
        return 80, 3
        
    # Tier 3: 普通法定假日 (±1天)
    return 50, 1

def main():
    print("正在读取数据库...")
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME}", conn)
    
    # 转换日期格式
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    
    print("正在生成日历特征...")
    df['day_of_week'] = df['date'].dt.dayofweek # 0=Mon, 6=Sun
    df['month'] = df['date'].dt.month
    df['quarter'] = df['date'].dt.quarter
    df['year'] = df['date'].dt.year
    
    # 初始化特征列
    df['is_holiday'] = 0
    df['holiday_name'] = None
    df['is_pandemic'] = 0
    
    # 疫情标记 (2020-03-01 ~ 2021-12-31)
    mask_pandemic = (df['date'] >= '2020-03-01') & (df['date'] <= '2021-12-31')
    df.loc[mask_pandemic, 'is_pandemic'] = 1
    
    print("正在计算分级节日窗口...")
    # 获取 2019-2027 所有美国联邦节日
    us_holidays = holidays.US(years=range(2019, 2028))
    
    # 创建每日的 (priority, name) 映射
    # date_obj -> (priority, name)
    holiday_events = {}
    
    for date, name in us_holidays.items():
        priority, window = get_holiday_priority(name)
        
        # 标记窗口期
        # range: [-window, +window] inclusive
        for offset in range(-window, window + 1):
            target_date = date + datetime.timedelta(days=offset)
            
            # 生成名称
            if offset == 0:
                event_name = name
                # 当天优先级稍微提高一点点，确保同 Tier 下优先显示正日子 (可选)
                current_prio = priority + 1 
            elif offset < 0:
                event_name = f"Pre-{name} ({abs(offset)}d)"
                current_prio = priority
            else:
                event_name = f"Post-{name} ({offset}d)"
                current_prio = priority
                
            # 冲突解决：谁大听谁的
            if target_date in holiday_events:
                existing_prio, _ = holiday_events[target_date]
                if current_prio > existing_prio:
                    holiday_events[target_date] = (current_prio, event_name)
            else:
                holiday_events[target_date] = (current_prio, event_name)

    # 将 map 应用到 DataFrame
    # 为了效率，我们可以将 holiday_events 转为 DataFrame 然后 merge
    holiday_data = []
    for d, (p, n) in holiday_events.items():
        holiday_data.append({'date': pd.Timestamp(d), 'is_holiday': 1, 'holiday_name': n})
        
    df_holidays = pd.DataFrame(holiday_data)
    
    # 合并 (Left Join)
    # 注意：原始数据可能有缺失日期，我们只标记存在的日期
    print("正在合并特征...")
    df = pd.merge(df, df_holidays, on='date', how='left', suffixes=('', '_new'))
    
    # 填充合并后的空值
    if 'is_holiday_new' in df.columns:
        df['is_holiday'] = df['is_holiday_new'].fillna(0).astype(int)
        df['holiday_name'] = df['holiday_name_new']
        df.drop(columns=['is_holiday_new', 'holiday_name_new'], inplace=True)
    
    # 预览
    print("\n特征预览 (Christmas 附近):")
    xmas_mask = df['date'].astype(str).str.contains('12-25')
    preview_dates = df[xmas_mask].head(1)['date'].values
    if len(preview_dates) > 0:
        center_date = pd.Timestamp(preview_dates[0])
        print(df[(df['date'] >= center_date - pd.Timedelta(days=5)) & 
                 (df['date'] <= center_date + pd.Timedelta(days=5))]
              [['date', 'throughput', 'is_holiday', 'holiday_name']])

    print(f"\n保存结果到 {OUTPUT_CSV} ...")
    df.to_csv(OUTPUT_CSV, index=False)
    
    print(f"保存结果到数据库表 {OUTPUT_TABLE} ...")
    # Date 转回字符串以便存储
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    df.to_sql(OUTPUT_TABLE, conn, if_exists='replace', index=False)
    conn.close()
    
    print("完成！")

if __name__ == "__main__":
    main()
