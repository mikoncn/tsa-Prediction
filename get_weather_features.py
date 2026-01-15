import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import datetime
import numpy as np

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

# 3. 时间范围
START_DATE = "2019-01-01"
# 昨天
YESTERDAY = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
# 今天
TODAY = datetime.date.today().strftime('%Y-%m-%d')
# 未来 15 天
END_DATE = (datetime.date.today() + datetime.timedelta(days=15)).strftime('%Y-%m-%d')

print(f"Plan: Archive [{START_DATE} ~ {YESTERDAY}] + Forecast [{TODAY} ~ {END_DATE}]")

def fetch_weather(url, airports, start, end, is_forecast=False):
    """通用天气抓取函数"""
    print(f"正在从 {url} 获取数据 ({start} 到 {end})...")
    
    # 准备参数
    params = {
        "latitude": [v["lat"] for v in airports.values()],
        "longitude": [v["lon"] for v in airports.values()],
        "start_date": start,
        "end_date": end,
        "daily": ["snowfall_sum", "wind_speed_10m_max", "precipitation_sum"],
        "timezone": "America/New_York" # 统一时区方便对齐
    }
    
    responses = openmeteo.weather_api(url, params=params)
    
    all_data = []
    airport_codes = list(airports.keys())
    
    for i, response in enumerate(responses):
        code = airport_codes[i]
        
        # 处理 Daily 数据
        daily = response.Daily()
        daily_snowfall_sum = daily.Variables(0).ValuesAsNumpy()
        daily_wind_speed_10m_max = daily.Variables(1).ValuesAsNumpy()
        daily_precipitation_sum = daily.Variables(2).ValuesAsNumpy()
        
        # 生成日期索引
        daily_data = {"date": pd.date_range(
            start=pd.to_datetime(daily.Time(), unit="s", utc=True),
            end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=daily.Interval()),
            inclusive="left"
        )}
        
        # 创建 DataFrame
        df = pd.DataFrame(data=daily_data)
        df['airport'] = code
        df['snowfall_cm'] = daily_snowfall_sum
        df['windspeed_kmh'] = daily_wind_speed_10m_max
        df['precipitation_mm'] = daily_precipitation_sum
        
        # 简单评分逻辑
        # 暴雪 (High): Snow > 3.0 cm -> 5分
        # 狂风 (Med): Wind > 40 km/h -> 2分
        # 暴雨 (Low): Rain > 25 mm -> 1分
        
        conditions = [
            (df['snowfall_cm'] > 3.0),
            (df['windspeed_kmh'] > 40.0),
            (df['precipitation_mm'] > 25.0)
        ]
        scores = [5, 2, 1]
        
        # 向量化计算得分 (取最大匹配? 不, 应该是叠加)
        # 题目要求: "评分...然后求和"
        # 比如芝加哥大雪(5)+纽约大风(2)，这里是针对单个机场的得分
        # 解释：单个机场如果同时暴雪+大风，应该是叠加比较合理，或者取最大?
        # 通常恶劣天气是综合的。这里我们假设叠加: Snow(5) + Wind(2) = 7 (这是一个很烂的天气)
        
        df['severity_score'] = 0
        df.loc[df['snowfall_cm'] > 3.0, 'severity_score'] += 5
        df.loc[df['windspeed_kmh'] > 40.0, 'severity_score'] += 2
        df.loc[df['precipitation_mm'] > 25.0, 'severity_score'] += 1
        
        all_data.append(df)
        
    return pd.concat(all_data)

try:
    # 1. 获取历史数据 (Archive)
    url_archive = "https://archive-api.open-meteo.com/v1/archive"
    df_archive = fetch_weather(url_archive, AIRPORTS, START_DATE, YESTERDAY)
    
    # 2. 获取预测数据 (Forecast)
    url_forecast = "https://api.open-meteo.com/v1/forecast"
    df_forecast = fetch_weather(url_forecast, AIRPORTS, TODAY, END_DATE)
    
    # 3. 合并
    print("正在合并数据...")
    full_df = pd.concat([df_archive, df_forecast])
    
    # 4. 聚合计算全美指数
    # 这里的 weather_index = Sum(5个机场的 severity_score)
    # 按 Date 分组求和
    full_df['date'] = full_df['date'].dt.strftime('%Y-%m-%d')
    weather_index_df = full_df.groupby('date')['severity_score'].sum().reset_index()
    weather_index_df.rename(columns={'severity_score': 'weather_index'}, inplace=True)
    
    # 5. 输出
    output_file = "weather_features.csv"
    print(f"写入文件 {output_file} ...")
    weather_index_df.to_csv(output_file, index=False)
    
    # 6. 验证炸弹气旋 (2022-12-22)
    print("\n=== 验证炸弹气旋 (2022-12-22) ===")
    target_date = "2022-12-22"
    row = weather_index_df[weather_index_df['date'] == target_date]
    if not row.empty:
        print(f"Date: {target_date}, Weather Index: {row['weather_index'].values[0]}")
        
        # 打印当天各机场详情
        print("详情:")
        details = full_df[full_df['date'] == target_date]
        print(details[['airport', 'snowfall_cm', 'windspeed_kmh', 'precipitation_mm', 'severity_score']])
    else:
        print(f"未找到 {target_date} 数据")

except Exception as e:
    print(f"Error: {e}")
