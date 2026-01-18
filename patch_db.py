import sqlite3
import pandas as pd

try:
    conn = sqlite3.connect('tsa_data.db')
    c = conn.cursor()
    
    # 1. Update Holiday Name
    print("Updating holiday_name for 2026-01-19...")
    c.execute("UPDATE prediction_history SET holiday_name = 'Martin Luther King Jr. Day' WHERE target_date = '2026-01-19'")
    print(f"Rows updated: {c.rowcount}")
    
    # 2. Verify
    rows = c.execute("SELECT target_date, holiday_name FROM prediction_history WHERE target_date = '2026-01-19'").fetchall()
    print("Verification:", rows)
    
    conn.commit()
    conn.close()
except Exception as e:
    print(f"Error: {e}")
