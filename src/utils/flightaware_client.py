# flightaware_client.py - FlightAware AeroAPI V4 Client
# 专业级 API 封装，支持严格的额度控制与错误处理

import requests
import json
import os
import time
from datetime import datetime, timezone

class FlightAwareClient:
    """
    FlightAware AeroAPI V4 客户端
    设计目标：高内聚、低耦合，严格控制 API 调用成本。
    """
    BASE_URL = "https://aeroapi.flightaware.com/aeroapi"
    
    def __init__(self, api_key=None, config_path=None):
        self.api_key = api_key
        if not self.api_key and config_path:
            self.api_key = self._load_key_from_config(config_path)
            
        if not self.api_key:
            raise ValueError("FlightAware API Key is required.")
            
        self.headers = {"x-apikey": self.api_key}

    def _load_key_from_config(self, path):
        if not os.path.exists(path):
            return None
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get("api_key")

    def get_airport_flights(self, airport_id, start=None, end=None, type="arrivals", max_pages=1):
        """
        获取机场航班数据
        type: arrivals, departures, scheduled_arrivals, scheduled_departures
        """
        endpoint = f"{self.BASE_URL}/airports/{airport_id}/flights"
        params = {
            "max_pages": max_pages
        }
        if start: params["start"] = start
        if end: params["end"] = end
        
        try:
            print(f"   [API] Calling FlightAware for {airport_id} ({type})...")
            response = requests.get(endpoint, headers=self.headers, params=params, timeout=20)
            
            if response.status_code == 200:
                data = response.json()
                return data.get(type, [])
            else:
                print(f"   [API Error] HTTP {response.status_code}: {response.text}")
                return None
        except Exception as e:
            print(f"   [API Exception] {e}")
            return None

    def test_connection(self):
        """
        极简测试：获取一个机场的状态（不涉及大量翻页航班查询，节省额度）
        """
        endpoint = f"{self.BASE_URL}/airports/KATL"
        try:
            resp = requests.get(endpoint, headers=self.headers, timeout=10)
            return resp.status_code == 200
        except:
            return False

if __name__ == "__main__":
    # 示例用法
    try:
        # 假设 key 在项目根目录
        root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        key_path = os.path.join(root, "flightaware_key.json")
        client = FlightAwareClient(config_path=key_path)
        
        print("Testing Connection...")
        if client.test_connection():
            print("Successfully connected to AeroAPI V4!")
        else:
            print("Connection failed. Check your API Key.")
    except Exception as e:
        print(f"Setup Error: {e}")
