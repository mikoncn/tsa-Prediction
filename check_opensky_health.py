import requests
import json
import time
import os
from datetime import datetime, timezone, timedelta

# Configuration
CREDENTIALS_FILE = "credentials.json"
TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
API_URL = "https://opensky-network.org/api/flights/arrival"

def load_credentials():
    if not os.path.exists(CREDENTIALS_FILE):
        print(f"❌ Error: {CREDENTIALS_FILE} not found.")
        return None, None
    with open(CREDENTIALS_FILE, "r") as f:
        creds = json.load(f)
        return creds.get("clientId"), creds.get("clientSecret")

def get_token(client_id, client_secret):
    print("🔑 Authenticating with OpenSky...")
    try:
        data = {
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret
        }
        resp = requests.post(TOKEN_URL, data=data, timeout=10)
        
        if resp.status_code == 200:
            print("   ✅ Auth Success: Token acquired.")
            return resp.json().get('access_token')
        else:
            print(f"   🚫 Auth Failed: Status {resp.status_code}")
            print(f"   Response: {resp.text}")
            return None
    except Exception as e:
        print(f"   ❌ Auth Error: {e}")
        return None

def check_api_health(token):
    print("\n📡 Sending Probe Request (1 Airport, 1 Hour window)...")
    
    # Construct a minimal request (KATL, 1 hour window yesterday)
    # Using yesterday ensures data exists
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    begin = int(yesterday.timestamp())
    end = begin + 3600 # 1 hour
    
    params = {
        'airport': 'KATL',
        'begin': begin,
        'end': end
    }
    
    headers = {
        'Authorization': f'Bearer {token}',
        'User-Agent': 'MikonAI-HealthCheck/1.0'
    }
    
    try:
        start_time = time.time()
        resp = requests.get(API_URL, params=params, headers=headers, timeout=15)
        duration = time.time() - start_time
        
        print(f"   ⏱️ Latency: {duration:.2f}s")
        print(f"   🏷️ Status Code: {resp.status_code}")
        
        # Check Headers for Rate Limits
        print("\n📊 API Response Headers (Rate Limit Info):")
        rate_headers = [h for h in resp.headers if 'rate' in h.lower() or 'limit' in h.lower() or 'remaining' in h.lower()]
        
        if rate_headers:
            for h in rate_headers:
                print(f"   {h}: {resp.headers[h]}")
        else:
            print("   (No explicit X-Rate-Limit headers found in response)")
            
        print("\n🩺 Diagnosis:")
        if resp.status_code == 200:
            count = len(resp.json())
            print(f"   ✅ API IS ONLINE. Retrieved {count} flights successfully.")
            print("   Your account is active and working.")
        elif resp.status_code == 429:
            print("   ❌ RATE LIMITED (429).")
            print("   You have exceeded your request quota or credit limit.")
            print("   Action: Wait for reset (usually daily) or check OpenSky account dashboard.")
        elif resp.status_code == 403:
            print("   🚫 FORBIDDEN (403).")
            print("   Creds valid but permission denied. Check subscription plan.")
        else:
            print(f"   ⚠️ UNKNOWN ISSUE: {resp.status_code}")
            print(f"   Response: {resp.text}")
            
    except Exception as e:
        print(f"   ❌ Connection Error: {e}")

if __name__ == "__main__":
    print("=== OpenSky API Health Check Tool ===\n")
    cid, sec = load_credentials()
    if cid and sec:
        token = get_token(cid, sec)
        if token:
            check_api_health(token)
    else:
        print("Cannot proceed without credentials.")
    
    print("\n=====================================")
