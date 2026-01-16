import sqlite3
import pandas as pd
import os

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.config import DB_PATH

# DB_PATH = 'tsa_data.db'
CSV_PATH = 'weather_features.csv'

def get_schema():
    print(f"=== Database Schema: {DB_PATH} ===")
    if not os.path.exists(DB_PATH):
        print("Database not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get list of tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    for table_name in tables:
        t = table_name[0]
        print(f"\nTable: [{t}]")
        cursor.execute(f"PRAGMA table_info({t})")
        columns = cursor.fetchall()
        # id, name, type, notnull, dflt_value, pk
        print(f"{'cid':<3} {'name':<20} {'type':<10} {'pk':<3}")
        print("-" * 40)
        for col in columns:
            print(f"{col[0]:<3} {col[1]:<20} {col[2]:<10} {col[5]:<3}")

    conn.close()

def check_csv():
    print(f"\n=== CSV Structure: {CSV_PATH} ===")
    if not os.path.exists(CSV_PATH):
        print("CSV not found.")
        return
        
    df = pd.read_csv(CSV_PATH, nrows=5)
    print("Columns:", list(df.columns))
    print(df.head().to_string())

if __name__ == "__main__":
    import sys
    with open('schema_dump.txt', 'w', encoding='utf-8') as f:
        sys.stdout = f
        get_schema()
        check_csv()
        sys.stdout = sys.__stdout__
    
    with open('schema_dump.txt', 'r', encoding='utf-8') as f:
        print(f.read())
