import sqlite3
import pandas as pd
import os

DB_PATH = os.path.join(os.getcwd(), 'tsa_data.db')
conn = sqlite3.connect(DB_PATH)

try:
    df = pd.read_sql("SELECT date, is_holiday, is_holiday_travel_window, holiday_name FROM traffic_full WHERE date BETWEEN '2026-01-15' AND '2026-01-25'", conn)
    print(df.to_string())
except Exception as e:
    print(e)
finally:
    conn.close()
