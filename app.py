from flask import Flask, render_template, jsonify
import sqlite3
import os

app = Flask(__name__)
DB_NAME = "tsa_data.db"

def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data')
def get_data():
    try:
        conn = get_db_connection()
        # 按日期升序排列，方便图表显示
        rows = conn.execute('SELECT date, throughput FROM traffic ORDER BY date ASC').fetchall()
        conn.close()
        
        data = [{'date': row['date'], 'throughput': row['throughput']} for row in rows]
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # 确保数据库存在
    if not os.path.exists(DB_NAME):
        print(f"Error: {DB_NAME} not found. Please run build_tsa_db.py first.")
    else:
        app.run(debug=True, port=5000)
