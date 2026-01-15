import sqlite3
import pandas as pd
import os

DB_NAME = "tsa_data.db"
CSV_FILE = "prediction_history.csv"

def migrate_csv():
    if not os.path.exists(CSV_FILE):
        print(f"File {CSV_FILE} not found. Skipping.")
        return

    print(f"Reading {CSV_FILE}...")
    try:
        df = pd.read_csv(CSV_FILE)
        
        # Standardize dates
        df['target_date'] = pd.to_datetime(df['target_date'], format='mixed').dt.strftime('%Y-%m-%d')
        # If model_run_date is missing, default to today? Or specific date?
        # Assuming model_run_date exists as per previous code analysis
        if 'model_run_date' in df.columns:
            df['model_run_date'] = pd.to_datetime(df['model_run_date'], format='mixed').dt.strftime('%Y-%m-%d')
        else:
            print("Warning: model_run_date missing, referencing today")
            df['model_run_date'] = pd.Timestamp.now().strftime('%Y-%m-%d')

        records = df[['target_date', 'predicted_throughput', 'model_run_date']].to_dict(orient='records')
        
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        print(f"Migrating {len(records)} records...")
        
        cursor.executemany('''
            INSERT INTO prediction_history (target_date, predicted_throughput, model_run_date)
            VALUES (:target_date, :predicted_throughput, :model_run_date)
        ''', records)
        
        conn.commit()
        conn.close()
        print("Migration complete.")
        
    except Exception as e:
        print(f"Migration failed: {e}")

if __name__ == "__main__":
    migrate_csv()
