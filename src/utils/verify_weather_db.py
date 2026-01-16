import sqlite3
import pandas as pd

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.config import DB_PATH

# DB_PATH = 'tsa_data.db'

def verify():
    print(f"=== Verifying Database: {DB_PATH} ===")
    conn = sqlite3.connect(DB_PATH)
    
    # Check 'weather' table
    try:
        df_weather = pd.read_sql("SELECT * FROM weather LIMIT 5", conn)
        print("\nTable [weather] sample:")
        print(df_weather.to_string())
        
        count_w = pd.read_sql("SELECT count(*) as cnt FROM weather", conn)['cnt'][0]
        print(f"Total rows in [weather]: {count_w}")
        
    except Exception as e:
        print(f"Error checking [weather]: {e}")
        
    # Check 'daily_weather_index'
    try:
        df_idx = pd.read_sql("SELECT * FROM daily_weather_index LIMIT 5", conn)
        print("\nTable [daily_weather_index] sample:")
        print(df_idx.to_string())
        
        count_idx = pd.read_sql("SELECT count(*) as cnt FROM daily_weather_index", conn)['cnt'][0]
        print(f"Total rows in [daily_weather_index]: {count_idx}")
        
    except Exception as e:
        print(f"Error checking [daily_weather_index]: {e}")
        
    conn.close()

if __name__ == "__main__":
    verify()
