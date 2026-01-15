import sqlite3

DB_NAME = "tsa_data.db"

def create_table():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Create prediction_history table
    # Columns:
    #   id: INTEGER PRIMARY KEY AUTOINCREMENT
    #   target_date: TEXT (YYYY-MM-DD) - The date being predicted
    #   predicted_throughput: INTEGER - The predicted value
    #   model_run_date: TEXT (YYYY-MM-DD) - When the prediction was made
    #   created_at: TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS prediction_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            target_date TEXT NOT NULL,
            predicted_throughput INTEGER NOT NULL,
            model_run_date TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Index for fast lookup
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_target_date ON prediction_history(target_date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_model_run_date ON prediction_history(model_run_date)')
    
    conn.commit()
    conn.close()
    print(f"Table prediction_history created in {DB_NAME}")

if __name__ == "__main__":
    create_table()
