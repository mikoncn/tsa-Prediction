import requests
import sqlite3
import pandas as pd
import sys
import time
import json
import os
from datetime import datetime, timedelta, timezone

# Configuration
DB_PATH = 'tsa_data.db'
BASE_URL = "https://opensky-network.org/api/flights/arrival"
# Top 10 Busiest US Airports
AIRPORTS = [
    'KATL', 'KORD', 'KDFW', 'KDEN', 'KLAX', 
    'KJFK', 'KMCO', 'KLAS', 'KCLT', 'KMIA'
]

# OAuth2 Token Management
TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
current_token = None
token_expiry = 0

def load_credentials():
    try:
        if not os.path.exists("credentials.json"):
            print("   [ERROR] credentials.json not found.")
            return None, None
            
        with open("credentials.json", "r") as f:
            creds = json.load(f)
            return creds.get("clientId"), creds.get("clientSecret")
    except Exception as e:
        print(f"Error loading credentials: {e}")
        return None, None

USERNAME, PASSWORD = load_credentials()

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def get_oauth_token():
    global current_token, token_expiry
    now = time.time()
    
    # Reuse token if valid (buffer 60s)
    if current_token and now < token_expiry - 60:
        return current_token
    
    if not USERNAME or not PASSWORD:
        print("   [ERROR] No credentials loaded.")
        return None
        
    try:
        data = {
            'grant_type': 'client_credentials',
            'client_id': USERNAME, 
            'client_secret': PASSWORD 
        }
        resp = requests.post(TOKEN_URL, data=data, timeout=10)
        
        if resp.status_code == 200:
            token_data = resp.json()
            current_token = token_data.get('access_token')
            expires_in = token_data.get('expires_in', 3600)
            token_expiry = now + expires_in
            print("   [AUTH] OAuth Token Refreshed.")
            return current_token
        else:
            print(f"   [AUTH] Token Fetch Failed: {resp.status_code}")
            print(resp.text)
            return None
    except Exception as e:
        print(f"   [AUTH] Token Exception: {e}")
        return None

def fetch_arrival_count(date_str, icao):
    """
    Fetch arrival count for a specific airport and date (00:00 - 24:00 UTC)
    OpenSky uses UTC timestamps.
    """
    try:
        token = get_oauth_token()
        if not token:
            print("   [ERROR] No valid token. Skipping fetch.")
            return None
            
        # FORCE UTC TIMEZONE
        # date_str is assumed to be YYYY-MM-DD
        dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        begin = int(dt.timestamp())
        end_dt = dt + timedelta(days=1)
        end = int(end_dt.timestamp())
        
        # CLAMP TO NOW
        # If the requested end time is in the future relative to system time, clamp it.
        # This prevents requesting future data which might cause API to return 0/error.
        now_ts = int(time.time())
        if end > now_ts:
            # print(f"   [DEBUG_TIME] Clamping end {end} to now {now_ts}")
            end = now_ts
        
        # If begin > now, we are asking for fully future data -> Return 0
        if begin > now_ts:
             print(f"   [DEBUG] Date {date_str} is in the future. Skipping.")
             return 0

        # print(f"   [DEBUG_TIME] {date_str} -> {begin} to {end}")
        
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
            headers=headers, # Use Bearer Token
            timeout=30
        )
        
        if resp.status_code == 200:
            flights = resp.json()
            # DEBUG: Print first item to verify structure
            if len(flights) > 0:
                print(f"   [DEBUG] Fetched {len(flights)} flights for {icao}. Sample: {flights[0].get('callsign', 'N/A')}")
            else:
                print(f"   [DEBUG] Fetched 0 flights for {icao}.")
                
            return len(flights)
        elif resp.status_code == 404:
            return 0 # No flights?
        elif resp.status_code == 429:
            print("   [429] Rate Limit Exceeded. Waiting 60s...")
            time.sleep(60)
            return None # Retry needed
        elif resp.status_code == 403:
            print("   [403] Forbidden. Check credits/scope.")
            return None
        elif resp.status_code == 401:
             print("   [401] Unauthorized. Token invalid?")
             return None
        else:
            print(f"   Error fetching {icao} on {date_str}: {resp.status_code}")
            return None
            
    except Exception as e:
        print(f"   Exception for {icao} on {date_str}: {e}")
        return None

def save_to_db(data_list):
    """
    Save list of (date, airport, count) to DB
    """
    if not data_list:
        return
        
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.executemany('''
            INSERT OR REPLACE INTO flight_stats (date, airport, arrival_count)
            VALUES (?, ?, ?)
        ''', data_list)
        conn.commit()
        print(f"   Saved {len(data_list)} records to DB.")
    except Exception as e:
        print(f"   DB Error: {e}")
    finally:
        conn.close()

def backfill(days_to_backfill=45):
    """
    Check missing data for past X days and fill it.
    """
    print(f"Checking data for the last {days_to_backfill} days...")
    
    # We want to check relative to 'now' but using local date definition for iterations is fine
    # the fetch_arrival_count converts that date string to UTC window.
    today = datetime.now().date()
    
    dates_to_check = []
    # Check yesterday and before
    for i in range(1, days_to_backfill + 1):
        d = today - timedelta(days=i)
        dates_to_check.append(d.strftime("%Y-%m-%d"))
        
    conn = get_db_connection()
    # Ensure table exists
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
    
    # Reset existing for the purpose of RE-FETCHING specific days if we think they are wrong?
    # Actually, user wants to FIX the data.
    # So we should probably force delete Jan 14 and Jan 15 from DB to force re-fetch?
    # Or just rely on INSERT OR REPLACE.
    # But logic below skips if present.
    
    existing_set = set(zip(existing['date'], existing['airport']))
    
    tasks = []
    for d_str in dates_to_check:
        # Force re-fetch for Jan 14 and Jan 15 since we know they are wrong/incomplete
        is_suspect_date = (d_str == '2026-01-14' or d_str == '2026-01-15')
        
        for icao in AIRPORTS:
            if is_suspect_date or (d_str, icao) not in existing_set:
                tasks.append((d_str, icao))
    
    print(f"Found {len(tasks)} records to fetch/refetch.")
    
    # Process batch
    batch = []
    for i, (d_str, icao) in enumerate(tasks):
        print(f"[{i+1}/{len(tasks)}] Fetching {icao} for {d_str}...")
        
        count = fetch_arrival_count(d_str, icao)
        
        if count is not None:
            batch.append((d_str, icao, count))
        
        # Save every 10 or if last
        if len(batch) >= 10:
            save_to_db(batch)
            batch = []
            
        time.sleep(0.5)
        
    if batch:
        save_to_db(batch)
        
    print("Backfill complete.")

if __name__ == "__main__":
    days = 60
    if len(sys.argv) > 1:
        days = int(sys.argv[1])
        
    backfill(days)
