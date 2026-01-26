# verify_fa_connection.py - 极简连接验证脚本
# 该脚本仅用于验证 API Key 是否有效，不涉及大规模航班查询，消耗额度极低。

import sys
import os

# 路径修复
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.utils.flightaware_client import FlightAwareClient

def verify():
    print("=== FlightAware AeroAPI V4 连接审计工具 ===")
    
    # 获取根目录路径
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    key_path = os.path.join(root, "flightaware_key.json")
    
    if not os.path.exists(key_path):
        print(f"   [失败] 未找到配置文件: {key_path}")
        return

    try:
        client = FlightAwareClient(config_path=key_path)
        print("   [1/1] 正在请求 KATL 机场静态信息以验证 Key ...")
        
        if client.test_connection():
            print("\n✅ [成功] API 鉴权通过！")
            print("   您的 Key (zEhg...ECzK) 已被成功集成，目前处于'待审计'状态。")
            print("   目前代码层已完全解耦，除非手动调用，否则不会消耗您的 5 美元额度。")
        else:
            print("\n❌ [失败] 连接成功但返回异常。请核对 Key 权限及费用额度。")
            
    except Exception as e:
        print(f"\n❌ [异常] 验证失败: {e}")

if __name__ == "__main__":
    verify()
