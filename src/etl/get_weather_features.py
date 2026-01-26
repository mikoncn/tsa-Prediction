import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import datetime
import numpy as np
import sqlite3

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.config import DB_PATH

# 配置
# DB_PATH = 'tsa_data.db'

# 1. 配置 API 客户端 (带缓存和重试)
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# 2. 枢纽机场坐标
AIRPORTS = {
    "ATL": {"lat": 33.64, "lon": -84.42},
    "ORD": {"lat": 41.97, "lon": -87.90},
    "DFW": {"lat": 32.89, "lon": -97.04},
    "DEN": {"lat": 39.85, "lon": -104.67},
    "JFK": {"lat": 40.64, "lon": -73.77}
}

def fetch_weather(url, airports, start_date, end_date):
    """
    通用天气抓取函数
    """
    all_rows = []
    
    for icao, coords in airports.items():
        try:
            params = {
                "latitude": coords["lat"],
                "longitude": coords["lon"],
                "start_date": start_date,
                "end_date": end_date,
                "daily": ["snowfall_sum", "precipitation_sum", "wind_speed_10m_max", "temperature_2m_min"]
            }
            
            responses = openmeteo.weather_api(url, params=params)
            response = responses[0]
            
            daily = response.Daily()
            daily_snowfall_sum = daily.Variables(0).ValuesAsNumpy()
            daily_precipitation_sum = daily.Variables(1).ValuesAsNumpy()
            daily_wind_speed_10m_max = daily.Variables(2).ValuesAsNumpy()
            daily_temperature_2m_min = daily.Variables(3).ValuesAsNumpy()
            
            daily_data = {"date": pd.date_range(
                start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
                end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
                freq = pd.Timedelta(seconds = daily.Interval()),
                inclusive = "left"
            )}
            
            df = pd.DataFrame(data = daily_data)
            df["airport"] = icao
            df["snowfall_cm"] = daily_snowfall_sum
            df["precipitation_mm"] = daily_precipitation_sum
            df["windspeed_kmh"] = daily_wind_speed_10m_max
            df["temperature_min_c"] = daily_temperature_2m_min
            
            # Simple Severity Score (Updated with Flash Freeze Logic)
            def calc_score(row):
                score = 0
                # Snow
                if row['snowfall_cm'] > 1.0: score += 1
                if row['snowfall_cm'] > 5.0: score += 2    # Cumulative: >5cm gets +3 total
                # Wind
                if row['windspeed_kmh'] > 29.0: score += 1
                if row['windspeed_kmh'] > 40.0: score += 2 # Cumulative: >40kmh gets +3 total
                # Temperature (Flash Freeze Logic - Tuned for Southern Hub Sensitivity)
                if row['temperature_min_c'] < -5.0: score += 1
                if row['temperature_min_c'] < -10.0: score += 1  # Cumulative: <-10 gets +2 (Severe for DFW)
                if row['temperature_min_c'] < -15.0: score += 1  # Cumulative: <-15 gets +3 (Severe for ORD)
                
                return score
            
            df['severity_score'] = df.apply(calc_score, axis=1)
            all_rows.append(df)
            print(f"   [OK] Fetched {icao}: {len(df)} days")
            
        except Exception as e:
            print(f"   [Error] Failed to fetch {icao}: {e}")
            
    if all_rows:
        return pd.concat(all_rows, ignore_index=True)
    else:
        return pd.DataFrame()

def run(full_mode=False):
    """
    天气数据抓取
    :param full_mode: True=全量模式(2019起), False=增量模式(近30天)
    """
    try:
        if full_mode:
            START_DATE = "2019-01-01"
            print("   [模式] 全量模式：从 2019 年开始回溯所有历史天气...")
        else:
            # 默认回档 30 天，足以覆盖任何中间缺失或修正
            START_DATE = (datetime.date.today() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
            print(f"   [模式] 日常增量模式：仅同步最近 30 天数据 (Start: {START_DATE})")

        # 昨天
        YESTERDAY = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        # 今天
        TODAY = datetime.date.today().strftime('%Y-%m-%d')
        # 未来 15 天
        END_DATE = (datetime.date.today() + datetime.timedelta(days=15)).strftime('%Y-%m-%d')

        print(f"Plan: Archive [{START_DATE} ~ {YESTERDAY}] + Forecast [{TODAY} ~ {END_DATE}]")

        # 1. 获取历史数据 (Archive)
        url_archive = "https://archive-api.open-meteo.com/v1/archive"
        df_archive = fetch_weather(url_archive, AIRPORTS, START_DATE, YESTERDAY)
        
        # 2. 获取预测数据 (Forecast)
        url_forecast = "https://api.open-meteo.com/v1/forecast"
        df_forecast = fetch_weather(url_forecast, AIRPORTS, TODAY, END_DATE)
        
        # 3. 合并
        print("正在合并历史与预测数据...")
        full_df = pd.concat([df_archive, df_forecast], ignore_index=True)
        
        # 彻底检查重复项
        dupes = full_df.duplicated(subset=['date', 'airport']).sum()
        if dupes > 0:
            print(f"警告: 发现 {dupes} 条重复的 [日期+机场] 数据，正在进行去重 (保留最新)...")
            full_df = full_df.drop_duplicates(subset=['date', 'airport'], keep='last')
        
        # 4. 聚合计算全美指数 (Refined Logic)
        full_df['date'] = full_df['date'].dt.strftime('%Y-%m-%d')
        
        def calculate_daily_index(group):
            base_score = group['severity_score'].sum()
            
            # 统计坏点 (Bad Hub Counter)
            bad_hubs_count = (group['severity_score'] >= 3).sum()
            
            if group['date'].iloc[0] == '2026-01-10':
                print(f"\nDEBUG Aggregation [2026-01-10]:")
                print(group[['airport', 'severity_score']])
                print(f"Base Score: {base_score}, Bad Hubs: {bad_hubs_count}")

            penalty = 0
            if bad_hubs_count >= 3:
                penalty = 20
            elif bad_hubs_count >= 2:
                penalty = 10
                
            return base_score + penalty

        print("正在计算多枢纽熔断指数...")
        weather_index_series = full_df.groupby('date').apply(calculate_daily_index)
        
        # 转换为 DataFrame
        weather_index_df = weather_index_series.reset_index(name='weather_index')
        
        # 5. 存入数据库 (DB Storage)
        print(f"正在存入数据库 {DB_PATH} ...")
        
        def save_weather_to_db(detailed_df, index_df):
            conn = sqlite3.connect(DB_PATH)
            
            # A. 存入详细天气表 (weather)
            detailed_df['updated_at'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Columns to save
            cols = ['date', 'airport', 'snowfall_cm', 'windspeed_kmh', 'precipitation_mm', 'temperature_min_c', 'severity_score', 'updated_at']
            detailed_df[cols].to_sql('weather', conn, if_exists='replace', index=False)
            print(f"   - 表 [weather]: 已更新 {len(detailed_df)} 条数据")
            
            # B. 存入每日指数表 (daily_weather_index) - 用于快速合并
            index_df['updated_at'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            index_df.to_sql('daily_weather_index', conn, if_exists='replace', index=False)
            print(f"   - 表 [daily_weather_index]: 已更新 {len(index_df)} 条数据")
            
            conn.close()

        save_weather_to_db(full_df, weather_index_df)
        print("OK 天气数据数据库化完成。")
        
        # 7. 检查 2026-01-10 (用户指定日期)
        print("\n=== 检查 2026-01-10 原始数据与评分 ===")
        check_date = "2026-01-10"
        details = full_df[full_df['date'] == check_date]
        if not details.empty:
            print(details[['airport', 'snowfall_cm', 'windspeed_kmh', 'precipitation_mm', 'severity_score']])
            
            # 逐行手动检查逻辑
            for _, row in details.iterrows():
                print(f"Airport: {row['airport']}")
                print(f"  Snow {row['snowfall_cm']} > 1.0? {row['snowfall_cm'] > 1.0}")
                print(f"  Wind {row['windspeed_kmh']} > 29.0? {row['windspeed_kmh'] > 29.0}")
                print(f"  Score allocated: {row['severity_score']}")
                
            row_summary = weather_index_df[weather_index_df['date'] == check_date]
            print(f"\nFinal Weather Index from CSV/DB: {row_summary['weather_index'].values[0]}")
        else:
            print(f"未找到 {check_date} 数据")
    
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    import sys
    full_mode = '--full' in sys.argv
    run(full_mode=full_mode)
