import requests
import time
import csv
import os
import json
from datetime import datetime

# ==========================================
# 配置区域 (Configuration)
# ==========================================

# 目标机场列表 (过滤只记录这些机场的事件)
TARGET_AIRPORTS = [
    'ORD', 'JFK', 'EWR', 'LGA', 'ATL', 'DFW', 
    'DEN', 'SFO', 'LAX', 'SEA', 'MCO', 'LAS'
]

# FAA OIS API 地址
API_URL = "https://nasstatus.faa.gov/api/airport-events"

# 日志文件名称
LOG_FILE = "faa_ois_log.csv"

# 检查间隔 (秒) - 15分钟
INTERVAL_SECONDS = 900

# CSV 表头
CSV_HEADERS = [
    "Timestamp_UTC", 
    "Airport", 
    "Event_Type", 
    "Avg_Delay_Mins", 
    "Reason", 
    "Raw_Start_Time"
]

# 请求头 (伪装成浏览器)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json"
}

# ==========================================
# 核心逻辑 (Core Logic)
# ==========================================

def init_csv():
    """初始化 CSV 文件，如果不存在则写入表头"""
    if not os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(CSV_HEADERS)
            print(f"[系统] 已创建日志文件: {LOG_FILE}")
        except Exception as e:
            print(f"[错误] 初始化 CSV 失败: {e}")

def parse_avg_delay(delay_str):
    """
    解析平均延误时间字符串。
    示例: "49 mins" -> 49
    如果无效或未提供，返回 0
    """
    if not delay_str:
        return 0
    try:
        # 尝试提取数字
        # 简单处理：分割字符串取第一部分，如果是纯数字则转换
        parts = str(delay_str).split()
        if parts and parts[0].isdigit():
            return int(parts[0])
        return 0
    except:
        return 0

def fetch_and_log():
    """执行一次 API 抓取并记录日志"""
    current_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        # 1. 发送请求
        response = requests.get(API_URL, headers=HEADERS, timeout=30)
        response.raise_for_status() # 检查 HTTP 错误
        
        # 2. 解析 JSON
        data = response.json()
        
        # FAA API 有时返回列表，有时可能是嵌套结构
        # 假设根对象是列表 (常见情况) 或包含在某个 key 中
        # 根据观察，nasstatus api 通常直接返回 list 或 {'data': [...]}
        # 这里做适应性处理
        events_list = []
        if isinstance(data, list):
            events_list = data
        elif isinstance(data, dict):
            # 尝试寻找常见的列表键
            for key in ['values', 'data', 'airportEvents']:
                if key in data and isinstance(data[key], list):
                    events_list = data[key]
                    break
        
        # 3. 过滤与提取
        active_events_count = 0
        rows_to_save = []
        
        for item in events_list:
            airport = item.get('airportCode', '').upper()
            
            # 仅处理白名单内的机场
            if airport in TARGET_AIRPORTS:
                event_type = item.get('event', 'UNKNOWN')
                reason = item.get('reason', '')
                start_time = item.get('startTime', '')
                
                # 提取平均延误
                # 字段名可能是 avgDelay, delay, departureDelay 等
                avg_delay_raw = item.get('avgDelay', 0)
                avg_delay_mins = parse_avg_delay(avg_delay_raw)
                
                # 记录该行
                row = [
                    current_utc,
                    airport,
                    event_type,
                    avg_delay_mins,
                    reason,
                    start_time
                ]
                rows_to_save.append(row)
                active_events_count += 1
        
        # 4. 写入文件 (Append Mode)
        if rows_to_save:
            with open(LOG_FILE, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(rows_to_save)
        
        # 5. 控制台输出
        local_time = datetime.now().strftime("%Y-%m-%d %H:%M")
        print(f"[{local_time}] FAA 状态检查完毕。发现 {active_events_count} 个活跃事件 (目标机场)。")
        
    except Exception as e:
        local_time = datetime.now().strftime("%Y-%m-%d %H:%M")
        print(f"[{local_time}] [警告] API 请求或处理失败: {e}")

# ==========================================
# 主程序入口 (Entry Point)
# ==========================================

if __name__ == "__main__":
    print("=== FAA 机场延误监控器已启动 ===")
    print(f"监控目标: {', '.join(TARGET_AIRPORTS)}")
    print(f"刷新频率: 每 {INTERVAL_SECONDS} 秒")
    print(f"日志文件: {os.path.abspath(LOG_FILE)}")
    
    # 确保文件存在
    init_csv()
    
    # 无限循环
    while True:
        fetch_and_log()
        
        # 等待下一次周期
        try:
            time.sleep(INTERVAL_SECONDS)
        except KeyboardInterrupt:
            print("\n[系统] 用户停止了脚本。再见！")
            break
