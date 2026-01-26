# fetch_flightaware.py - FlightAware AeroAPI V4 数据抓取脚本
# 功能：抓取美国核心机场的历史抵达航班与未来计划航班数据，补充 OpenSky 的不足。

import requests
import sqlite3
import pandas as pd
import sys
import time
import json
import os
from datetime import datetime, timedelta, timezone

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.config import DB_PATH

# 配置
AIRPORTS = [
    'KATL', 'KORD', 'KDFW', 'KDEN', 'KLAX', 
    'KJFK', 'KMCO', 'KLAS', 'KCLT', 'KMIA'
]

def load_flightaware_key():
    """从 flightaware_key.json 加载 API Key"""
    try:
        root_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        path = os.path.join(root_path, "flightaware_key.json")
        if not os.path.exists(path):
            print("   [错误] 未找到 flightaware_key.json。")
            return None
            
        with open(path, "r", encoding='utf-8') as f:
            data = json.load(f)
            key = data.get("api_key")
            if not key or "PLEASE_ENTER" in key:
                print("   [错误] 请在 flightaware_key.json 中填写有效的 API Key。")
                return None
            return key
    except Exception as e:
        print(f"   [异常] 加载 API Key 失败: {e}")
        return None

def fetch_flights(airport, start_iso, end_iso, api_key, entry_type="arrivals"):
    """
    抓取指定机场、时间段的航班数据。
    entry_type: "arrivals" (历史) 或 "scheduled_arrivals" (未来)
    """
    # AeroAPI V4 Endpoint
    url = f"https://aeroapi.flightaware.com/aeroapi/airports/{airport}/flights"
    headers = {"x-apikey": api_key}
    params = {
        "start": start_iso,
        "end": end_iso,
        "max_pages": 5 # 限制翻页以节约额度，通常核心机场一页 15 条，翻 5 页涵盖大部分
    }
    
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            flights = data.get(entry_type, [])
            # 翻页逻辑 (如果需要精确总数)
            while "next_id" in data.get("links", {}) and params["max_pages"] > 1:
                # 简单实现：这里我们主要关注总量，如果接口支持更好的聚合则更优
                # 注意：AeroAPI 是按请求计费的
                break 
            
            return len(flights)
        else:
            print(f"   [错误] {airport} HTTP {resp.status_code}: {resp.text}")
            return None
    except Exception as e:
        print(f"   [异常] 请求失败: {e}")
        return None

def update_flight_stats(date_str, airport, count, source="flightaware"):
    """保存到数据库"""
    if count is None: return
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        # 我们甚至可以在这里记录来源，但目前为了兼容 merge_db，直接覆盖或插入
        conn.execute('''
            INSERT OR REPLACE INTO flight_stats (date, airport, arrival_count)
            VALUES (?, ?, ?)
        ''', (date_str, airport, count))
        conn.commit()
    except Exception as e:
        print(f"   [数据库错误] {e}")
    finally:
        conn.close()

def sync_recent(api_key):
    """
    [ECONOMY STRATEGY] 极致省钱模式
    - 不抓取历史 (由 OpenSky 免费提供)
    - 只抓取未来 3 天的计划航班 (由 FlightAware 提供，用于增强预测)
    """
    days_forward = 3 
    print(f"=== 启动 FlightAware 精准同步 (仅未来 {days_forward}天计划) ===")
    
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    # 只抓取未来 (计划)
    for i in range(0, days_forward + 1):
        target_date = today + timedelta(days=i)
        d_str = target_date.strftime("%Y-%m-%d")
        start_iso = target_date.isoformat()
        end_iso = (target_date + timedelta(days=1)).isoformat()
        
        for icao in AIRPORTS:
            print(f"   [目标] {icao} ({d_str})... ", end="")
            count = fetch_flights(icao, start_iso, end_iso, api_key, "scheduled_arrivals")
            if count:
                print(f"发现 {count} 架计划航班")
                update_flight_stats(d_str, icao, count)
            else:
                print("跳过/无数据")
            time.sleep(0.05) # 极短延迟

    print(f"=== 同步结束 (预计消耗 {len(AIRPORTS) * (days_forward + 1)} 次请求) ===")

if __name__ == "__main__":
    key = load_flightaware_key()
    if key:
        sync_recent(key)
