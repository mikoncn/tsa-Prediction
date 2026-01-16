import sqlite3
import pandas as pd

conn = sqlite3.connect('tsa_data.db')
cursor = conn.cursor()

print("--- Tables ---")
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
print(cursor.fetchall())

print("\n--- traffic_full First 3 Rows ---")
try:
    df = pd.read_sql("SELECT * FROM traffic_full LIMIT 3", conn)
    print(df)
    print(df.columns)
except Exception as e:
    print(e)
    
print("\n--- flight_stats First 3 Rows ---")
try:
    df = pd.read_sql("SELECT * FROM flight_stats LIMIT 3", conn)
    print(df)
    print(df.columns)
except Exception as e:
    print(e)

conn.close()
