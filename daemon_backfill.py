# daemon_backfill.py - OpenSky 自动回溯守护进程
# 功能：全天候无人值守运行，负责将过去 60 天的数据一点点“啃”下来。
# 特性：
# 1. 自动休眠：遇到 429 自动睡 1 小时，不头铁。
# 2. 自动唤醒：次日 08:00 (OpenSky 刷新时间) 准时开工。
# 3. 进度记忆：断点续传，不重复抓取。

import subprocess
import time
import datetime
import sys
import os

# 配置
SCRIPT_NAME = "fetch_opensky.py"
BACKFILL_DAYS = 60
MAX_RETRIES = 5  # 连续失败次数阈值

def log(msg):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}")
    with open("daemon.log", "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {msg}\n")

def run_backfill():
    """运行一次回溯任务"""
    log("启动子任务: fetch_opensky.py ...")
    try:
        # 调用核心脚本
        result = subprocess.run(
            [sys.executable, SCRIPT_NAME, str(BACKFILL_DAYS)],
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace'
        )
        
        # 记录输出摘要
        output_tail = "\n".join(result.stdout.strip().split('\n')[-3:])
        if result.returncode == 0:
            log(f"任务完成:\n{output_tail}")
            return "SUCCESS"
        else:
            log(f"任务异常 (Code {result.returncode}):\n{output_tail}")
            if "429" in result.stdout or "429" in result.stderr:
                return "RATE_LIMIT"
            return "ERROR"
            
    except Exception as e:
        log(f"启动失败: {e}")
        return "CRASH"

def main():
    log("=== OpenSky 守护进程已启动 (按 Ctrl+C 停止) ===")
    
    consecutive_errors = 0
    
    while True:
        status = run_backfill()
        
        if status == "SUCCESS":
            log("本轮任务全部成功完成。休眠 4 小时后重新检查 (防止过度请求)...")
            time.sleep(4 * 3600)
            consecutive_errors = 0
            
        elif status == "RATE_LIMIT":
            log("检测到 API 429 限流。")
            log("策略: 深度休眠直到明天北京时间 08:05 (UTC 00:05) 额度刷新...")
            
            # 计算需要睡多久
            now = datetime.datetime.now()
            # 目标是明天的 08:05
            target = now.replace(hour=8, minute=5, second=0, microsecond=0)
            if now >= target:
                target += datetime.timedelta(days=1)
                
            sleep_seconds = (target - now).total_seconds()
            hours = sleep_seconds / 3600
            log(f"当前时间: {now}, 目标唤醒: {target}, 需休眠 {hours:.2f} 小时。")
            
            time.sleep(sleep_seconds)
            consecutive_errors = 0 # 睡醒后重置错误计数
            
        elif status == "ERROR" or status == "CRASH":
            consecutive_errors += 1
            log(f"发生未知错误 (连续第 {consecutive_errors} 次)。")
            
            if consecutive_errors >= MAX_RETRIES:
                log("连续错误过多，守护进程自动终止，请检查日志。")
                break
                
            log("等待 10 分钟后重试...")
            time.sleep(600)

if __name__ == "__main__":
    main()
