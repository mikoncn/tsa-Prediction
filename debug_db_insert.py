import sqlite3
import pandas as pd
import numpy as np

DB_NAME = "tsa_data.db"

def test_insert():
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        # Test Data mimicking the problematic row
        # derived from "2026-01-15", 2255026, "2026-01-16"
        
        records = [
            {'target_date': '2026-01-15', 'predicted_throughput': 2255026, 'model_run_date': '2026-01-16'},
            {'target_date': '2026-01-16', 'predicted_throughput': 2526667, 'model_run_date': '2026-01-16'}
        ]
        
        print("Records to insert:", records)
        
        cursor.executemany('''
            INSERT INTO prediction_history (target_date, predicted_throughput, model_run_date)
            VALUES (:target_date, :predicted_throughput, :model_run_date)
        ''', records)
        
        conn.commit()
        conn.close()
        print("Success!")
        
    except Exception as e:
        print(f"FAILED: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_insert()
