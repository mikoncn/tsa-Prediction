# fetch_opensky.py - OpenSky 网络数据抓取核心脚本
# 功能：从 OpenSky Network API 抓取美国核心枢纽机场的航班抵达数据，用于 TSA 客流模型训练。
# 注意事项：受限于 OpenSky 的 API 积分制（Basic 账户每日 4000 点），需谨慎处理抓取频率。

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

# 数据库与接口配置
# DB_PATH = 'tsa_data.db'
BASE_URL = "https://opensky-network.org/api/flights/arrival"
# 监控美国吞吐量最大的前 10 个枢纽机场，这些机场的变动对 TSA 总量具有极强的代表性
AIRPORTS = [
    'KATL', 'KORD', 'KDFW', 'KDEN', 'KLAX', 
    'KJFK', 'KMCO', 'KLAS', 'KCLT', 'KMIA'
]

# OAuth2 凭据管理：OpenSky 自 2025 年 3 月起强制要求新账号使用 OAuth2 认证
TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
current_token = None
token_expiry = 0

def load_credentials():
    """从本地 credentials.json 加载 API 身份信息"""
    try:
        if not os.path.exists("credentials.json"):
            print("   [错误] 未找到 credentials.json 凭据文件。")
            return None, None
            
        with open("credentials.json", "r") as f:
            creds = json.load(f)
            return creds.get("clientId"), creds.get("clientSecret")
    except Exception as e:
        print(f"   [异常] 加载凭据失败: {e}")
        return None, None

def get_oauth_token():
    """由于 OpenSky Token 有效期较短，该函数负责按需获取或更新 Bearer Token"""
    global current_token, token_expiry
    
    # 如果系统当前已有有效 Token 且未过期，直接复用
    if current_token and time.time() < token_expiry - 60:
        return current_token
        
    client_id, client_secret = load_credentials()
    if not client_id or not client_secret:
        return None

    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret
    }
    
    try:
        resp = requests.post(TOKEN_URL, data=data, timeout=10)
        if resp.status_code == 200:
            token_data = resp.json()
            current_token = token_data['access_token']
            # 设置过期缓冲期
            token_expiry = time.time() + token_data['expires_in']
            print("   [授权] OAuth Token 已刷新。")
            return current_token
        else:
            print(f"   [授权错误] 无法获取 Token: {resp.status_code}")
            return None
    except Exception as e:
        print(f"   [授权异常] 获取 Token 失败: {e}")
        return None

def fetch_arrival_count(date_str, icao):
    """
    抓取特定日期、特定机场的航班抵达总数。
    关键逻辑：OpenSky 接口接收 UTC 时间戳，脚本会将本地日期转换为该日 00:00 到 23:59 的 UTC 窗口。
    """
    token = get_oauth_token()
    if not token:
        return None
        
    try:
        # 将日期字符串转换为对应的 UTC 时间戳起始和结束点
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        begin = int(dt.replace(tzinfo=timezone.utc).timestamp())
        end = begin + 86400 # 覆盖全天 24 小时
        
        # 安全性逻辑：严禁查询未来数据，如果 end 时间超过当前系统时间，则截断至“现在”。
        now_ts = int(time.time())
        if end > now_ts:
            end = now_ts
        
        # 如果起始时间就已经在未来，直接跳过抓取
        if begin > now_ts:
             print(f"   [跳过] 日期 {date_str} 处于未来，无有效数据。")
             return 0

        params = {
            'airport': icao,
            'begin': begin,
            'end': end
        }
        
        headers = {
            'Authorization': f'Bearer {token}'
        }
        
        resp = requests.get(
            BASE_URL, 
            params=params, 
            headers=headers, 
            timeout=30
        )
        
        if resp.status_code == 200:
            flights = resp.json()
            if len(flights) > 0:
                print(f"   [成功] 抓取到 {icao} 的 {len(flights)} 架次航班。采样: {flights[0].get('callsign', 'N/A')}")
            else:
                print(f"   [提醒] {icao} 在 {date_str} 抓取结果为 0。")
            return len(flights)
        elif resp.status_code == 429:
            print("   [警告] 触发 429 访问受限（频率/额度）。")
            # [NEW] Failsafe for UI: If running from Dashboard, don't sleep forever
            if '--fail-fast' in sys.argv:
                print("   [UI模式] 快速失败，跳过等待。")
                return None
                
            print("   正在进入 60 秒强制冷却期...")
            time.sleep(60)
            return None 
        elif resp.status_code == 401:
             print("   [错误] 401 认证失效，Token 可能已过期。")
             return None
        else:
            print(f"   [错误] {icao} 在 {date_str} 抓取失败: {resp.status_code}")
            return None
            
    except Exception as e:
        print(f"   [异常] {icao} 在 {date_str} 抓取时发生故障: {e}")
        return None

def save_to_db(data_list):
    """批量持久化航班数据到 SQLite 数据库 flight_stats 表"""
    if not data_list: return
    
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        # INSERT OR REPLACE 确保了如果重复运行脚本，数据会被最新修正的结果覆盖（如时区修正后的数据）
        conn.executemany('''
            INSERT OR REPLACE INTO flight_stats (date, airport, arrival_count)
            VALUES (?, ?, ?)
        ''', data_list)
        conn.commit()
        print(f"   [数据库] 成功保存 {len(data_list)} 条记录。")
    except Exception as e:
        print(f"   [数据库错误] 写入失败: {e}")
    finally:
        conn.close()

def get_db_connection():
    """获取数据库连接"""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn

def backfill(days_to_backfill=45):
    """
    数据回溯策略：分析历史缺口并进行自动填补。
    """
    print(f"=== 启动历史数据回溯任务 (目标范围: 过去 {days_to_backfill} 天) ===")
    
    today = datetime.now().date()
    dates_to_check = []
    for i in range(1, days_to_backfill + 1):
        d = today - timedelta(days=i)
        dates_to_check.append(d.strftime("%Y-%m-%d"))
        
    conn = get_db_connection()
    # 若表结构不存在则创建（date, airport 构成联合主键）
    conn.execute('''
        CREATE TABLE IF NOT EXISTS flight_stats (
            date TEXT,
            airport TEXT,
            arrival_count INTEGER,
            PRIMARY KEY (date, airport)
        )
    ''')
    existing = pd.read_sql("SELECT date, airport FROM flight_stats", conn)
    conn.close()
    
    existing_set = set(zip(existing['date'], existing['airport']))
    
    tasks = []
    for d_str in dates_to_check:
        # 特殊逻辑：1月14日和15日曾受旧版时区 Bug 影响（数据腰斩），必须强行重新下载
        is_suspect_date = (d_str == '2026-01-14' or d_str == '2026-01-15')
        
        for icao in AIRPORTS:
            if is_suspect_date or (d_str, icao) not in existing_set:
                tasks.append((d_str, icao))
    
    print(f"=== 待处理任务总数: {len(tasks)} ===")
    
    batch = []
    for i, (d_str, icao) in enumerate(tasks):
        print(f"进度: [{i+1}/{len(tasks)}] 正在请求 {icao} ({d_str})...")
        
        count = fetch_arrival_count(d_str, icao)
        
        if count is not None:
            batch.append((d_str, icao, count))
        
        # 批量保存以提升 IO 性能（每 10 次请求执行一次 Commit）
        if len(batch) >= 10:
            save_to_db(batch)
            batch = []
            
        # 必要的礼貌延迟，防止极速请求导致的 429 封禁
        time.sleep(0.5)
        
    if batch:
        save_to_db(batch)
        
    print("=== 历史回溯任务结束 ===")

if __name__ == "__main__":
    # 命令行参数逻辑优化
    # 模式 1: python fetch_opensky.py 60  -> 深度回溯过去 60 天 (Daemon 模式)
    # 模式 2: python fetch_opensky.py --recent -> 仅确保最近 3 天数据完整 (UI 模式)
    
    import sys
    
    if '--recent' in sys.argv:
        print("=== [UI模式] 快速同步最近 3 天数据 ===")
        # 强制检查过去 3 天 (今天, 昨天, 前天)
        # 确保 T-1 和 T-2 有数据，这是 Sniper 模型最需要的
        backfill(days_to_backfill=3)
        
    elif len(sys.argv) > 1:
        # 数字模式，回溯指定天数
        try:
            days = int(sys.argv[1])
            backfill(days)
        except:
            backfill(45) # Default
    else:
        # 默认行为
        backfill(45)
