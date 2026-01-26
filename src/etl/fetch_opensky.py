# fetch_opensky.py - OpenSky 网络数据抓取核心脚本
# 功能：从 OpenSky Network API 抓取美国核心枢纽机场的航班抵达数据，用于 TSA 客流模型训练。
# 更新：支持多账号自动切换 (Rotation) 以应对 Rate Limit。

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
BASE_URL = "https://opensky-network.org/api/flights/arrival"
AIRPORTS = [
    'KATL', 'KORD', 'KDFW', 'KDEN', 'KLAX', 
    'KJFK', 'KMCO', 'KLAS', 'KCLT', 'KMIA'
]

# OAuth2 凭据管理
TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

# [NEW] Multi-Account State
CREDENTIALS_LIST = []
CURRENT_ACCOUNT_INDEX = 0
TOKEN_CACHE = {} # Map index -> {token, expiry}

def load_credentials_list():
    """从本地 credentials.json 加载 API 身份信息列表"""
    global CREDENTIALS_LIST
    try:
        # 尝试寻找 credentials.json
        # 1. 当前目录
        paths = ["credentials.json"]
        # 2. 项目根目录 (假设脚本在 src/etl/ 中)
        root_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        paths.append(os.path.join(root_path, "credentials.json"))
        
        target_path = None
        for p in paths:
            if os.path.exists(p):
                target_path = p
                break
        
        if not target_path:
            print("   [错误] 未找到 credentials.json 凭据文件。")
            return []
            
        with open(target_path, "r", encoding='utf-8') as f:
            data = json.load(f)
            
        # 兼容旧格式（单个对象）和新格式（列表）
        if isinstance(data, dict):
            CREDENTIALS_LIST = [data]
        elif isinstance(data, list):
            CREDENTIALS_LIST = data
        else:
            CREDENTIALS_LIST = []
            
        print(f"   [系统] 加载了 {len(CREDENTIALS_LIST)} 个 API 账号。")
        return CREDENTIALS_LIST
        
    except Exception as e:
        print(f"   [异常] 加载凭据失败: {e}")
        return []

def get_oauth_token(force_refresh=False):
    """获取当前活跃账号的 Token，如果失效则刷新"""
    global CURRENT_ACCOUNT_INDEX, TOKEN_CACHE, CREDENTIALS_LIST
    
    if not CREDENTIALS_LIST:
        load_credentials_list()
        if not CREDENTIALS_LIST:
            return None

    # 获取当前账号信息
    account_idx = CURRENT_ACCOUNT_INDEX
    account = CREDENTIALS_LIST[account_idx]
    
    # 检查缓存
    cache = TOKEN_CACHE.get(account_idx)
    if not force_refresh and cache and time.time() < cache['expiry'] - 60:
        return cache['token']
        
    # 刷新 Token
    client_id = account.get("clientId")
    client_secret = account.get("clientSecret")
    
    if not client_id or not client_secret or "PLEASE_ENTER" in client_secret:
        print(f"   [跳过] 账号 {client_id} 配置不完整。")
        return None

    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret
    }
    
    try:
        # print(f"   [授权] 正在尝试使用账号: {client_id} ...")
        resp = requests.post(TOKEN_URL, data=data, timeout=10)
        if resp.status_code == 200:
            token_data = resp.json()
            token = token_data['access_token']
            expiry = time.time() + token_data['expires_in']
            
            TOKEN_CACHE[account_idx] = {'token': token, 'expiry': expiry}
            # print(f"   [授权] Token 获取成功 ({client_id})。")
            return token
        else:
            print(f"   [授权错误] {client_id} 获取 Token 失败: {resp.status_code}")
            return None
    except Exception as e:
        print(f"   [授权异常] {client_id} 连接失败: {e}")
        return None

def rotate_account():
    """切换到下一个可用账号"""
    global CURRENT_ACCOUNT_INDEX, CREDENTIALS_LIST
    if not CREDENTIALS_LIST: return False
    
    old_idx = CURRENT_ACCOUNT_INDEX
    CURRENT_ACCOUNT_INDEX = (CURRENT_ACCOUNT_INDEX + 1) % len(CREDENTIALS_LIST)
    
    print(f"   [切换] 触发账号切换: #{old_idx} -> #{CURRENT_ACCOUNT_INDEX}")
    
    # 清除旧 Token 缓存（可选，为了安全）
    # TOKEN_CACHE.pop(old_idx, None) 
    return True

def fetch_arrival_count(date_str, icao, retry_count=0):
    """
    抓取特定日期、特定机场的航班抵达总数。
    支持自动轮换账号重试。
    """
    if retry_count > len(CREDENTIALS_LIST) + 1:
        print(f"   [失败] 已尝试所有账号，无法获取数据 ({date_str}, {icao})。")
        return None

    token = get_oauth_token()
    if not token:
        # 当前 Token 获取失败，尝试切换账号并重试
        if rotate_account():
            return fetch_arrival_count(date_str, icao, retry_count + 1)
        else:
            return None
        
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        begin = int(dt.replace(tzinfo=timezone.utc).timestamp())
        end = begin + 86400
        
        now_ts = int(time.time())
        if end > now_ts: end = now_ts
        if begin > now_ts: return 0

        params = {'airport': icao, 'begin': begin, 'end': end}
        headers = {'Authorization': f'Bearer {token}'}
        
        resp = requests.get(BASE_URL, params=params, headers=headers, timeout=30)
        
        if resp.status_code == 200:
            flights = resp.json()
            return len(flights)
            
        elif resp.status_code == 429:
            print(f"   [限制] 当前账号 #{CURRENT_ACCOUNT_INDEX} 触发 429 限流。")
            if rotate_account():
                time.sleep(1) # 短暂冷却
                return fetch_arrival_count(date_str, icao, retry_count + 1)
            else:
                return None
                
        elif resp.status_code == 401:
            print(f"   [认证] 当前账号 #{CURRENT_ACCOUNT_INDEX} Token 失效。")
            # 强制刷新当前账号（可能只是过期），或者也可以选择 Rotate
            # 这里选择 Rotate 更稳妥
            if rotate_account():
                return fetch_arrival_count(date_str, icao, retry_count + 1)
            else:
                return None
        else:
            print(f"   [错误] {icao} {date_str} HTTP {resp.status_code}")
            return None
            
    except Exception as e:
        print(f"   [异常] 抓取故障: {e}")
        return None

def save_to_db(data_list):
    """批量持久化航班数据到 SQLite 数据库 flight_stats 表"""
    if not data_list: return
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
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
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn

def check_cooldown(scope="all", interval_minutes=60):
    """
    检查抓取冷却时间，防止高频重复请求。
    """
    try:
        conn = get_db_connection()
        conn.execute("CREATE TABLE IF NOT EXISTS fetch_metadata (key TEXT PRIMARY KEY, last_run TEXT)")
        row = conn.execute("SELECT last_run FROM fetch_metadata WHERE key = ?", (f"opensky_{scope}",)).fetchone()
        
        if row:
            last_run = datetime.strptime(row['last_run'], '%Y-%m-%d %H:%M:%S')
            elapsed = (datetime.now() - last_run).total_seconds() / 60
            if elapsed < interval_minutes:
                conn.close()
                return False, interval_minutes - int(elapsed)
        
        # 更新时间戳（先不提交，如果后面真正运行了再由调用者提交或此处直接提交）
        conn.execute("INSERT OR REPLACE INTO fetch_metadata (key, last_run) VALUES (?, ?)", 
                     (f"opensky_{scope}", datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        conn.commit()
        conn.close()
        return True, 0
    except Exception as e:
        print(f"   [冷却检查异常] {e}")
        return True, 0 # 异常时保守运行

def backfill(days_to_backfill=45, force=False):
    print(f"=== 启动历史数据回溯任务 (目标范围: 过去 {days_to_backfill} 天) ===")
    
    # [NEW] 冷却检查逻辑 (仅在非 force 模式下生效)
    if not force:
        scope = "recent" if days_to_backfill <= 7 else "full"
        can_run, wait_mins = check_cooldown(scope)
        if not can_run:
            print(f"   [跳过] OpenSky 抓取处于冷却期，请在 {wait_mins} 分钟后重试。")
            return
    
    # 确保加载凭据
    load_credentials_list()
    if not CREDENTIALS_LIST:
        print("无可用凭据，退出。")
        return

    today = datetime.now().date()
    dates_to_check = []
    for i in range(1, days_to_backfill + 1):
        d = today - timedelta(days=i)
        dates_to_check.append(d.strftime("%Y-%m-%d"))
        
    conn = get_db_connection()
    conn.execute('''
        CREATE TABLE IF NOT EXISTS flight_stats (
            date TEXT,
            airport TEXT,
            arrival_count INTEGER,
            PRIMARY KEY (date, airport)
        )
    ''')
    existing = pd.read_sql("SELECT date, airport, arrival_count FROM flight_stats", conn)
    conn.close()
    
    existing_map = { (row['date'], row['airport']): row['arrival_count'] for _, row in existing.iterrows() }
    
    tasks = []
    QUALITY_THRESHOLD = 50 
    
    for d_str in dates_to_check:
        dt_obj = datetime.strptime(d_str, "%Y-%m-%d").date()
        # [逻辑优化] 最近 3 天不再强制刷新，除非数据量确实过低（判定为未就绪）
        is_recent_window = (today - dt_obj).days <= 3
        
        for icao in AIRPORTS:
            count = existing_map.get((d_str, icao))
            is_dirty = count is not None and count < QUALITY_THRESHOLD
            
            should_fetch = False
            if count is None: 
                should_fetch = True
            elif is_dirty: 
                should_fetch = True
            # [REMOVED] elif is_recent_window: should_fetch = True 
            # 删除了无条件强制刷新最近3天的逻辑，改为只有数据不足时才刷新。
            
            if should_fetch:
                tasks.append((d_str, icao))
    
    # 去重
    tasks = list(dict.fromkeys(tasks))
    print(f"=== 待处理任务总数: {len(tasks)} (含脏数据重刷与前向扫频) ===")
    
    batch = []
    for i, (d_str, icao) in enumerate(tasks):
        print(f"进度: [{i+1}/{len(tasks)}] 正在请求 {icao} ({d_str})...")
        
        count = fetch_arrival_count(d_str, icao)
        
        if count is not None:
            if count >= 10:
                batch.append((d_str, icao, count))
            else:
                print(f"   [丢弃] {icao} 在 {d_str} 的数据仅为 {count}，判定为未就绪。")
        
        if len(batch) >= 10:
            save_to_db(batch)
            batch = []
            
        time.sleep(0.5)
        
    if batch:
        save_to_db(batch)
        
    print("=== 历史回溯任务结束 ===")

def run(recent=False, force=False):
    if recent:
        print("=== [UI模式] 快速同步最近 3 天数据 ===")
        backfill(days_to_backfill=3, force=force)
    else:
        backfill(45, force=force)

if __name__ == "__main__":
    import sys
    force_run = '--force' in sys.argv
    if '--recent' in sys.argv:
        run(recent=True, force=force_run)
    elif len(sys.argv) > 1:
        try:
            # Check if arg 1 is days or --force
            if sys.argv[1].isdigit():
                days = int(sys.argv[1])
                backfill(days, force=force_run)
            else:
                run(force=force_run)
        except:
            run(force=force_run)
    else:
        run(force=force_run)
