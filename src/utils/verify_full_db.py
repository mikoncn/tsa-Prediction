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
    
    # Check 'traffic_full' table
    try:
        df = pd.read_sql("SELECT * FROM traffic_full LIMIT 5", conn)
        print("\nTable [traffic_full] sample:")
        print(df.to_string())
        
        count = pd.read_sql("SELECT count(*) as cnt FROM traffic_full", conn)['cnt'][0]
        print(f"Total rows in [traffic_full]: {count}")
        
    except Exception as e:
        print(f"Error checking [traffic_full]: {e}")
        
    conn.close()

if __name__ == "__main__":
    verify()
