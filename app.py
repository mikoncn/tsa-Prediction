from flask import Flask, render_template, jsonify
import sqlite3
import pandas as pd

app = Flask(__name__)
DB_PATH = 'tsa_data.db'

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data')
def get_data():
    conn = get_db_connection()
    # 查询全量宽表 (包含天气和节日特征)
    # 限制为当前时间之前的数据，或者全部数据
    query = """
        SELECT date, throughput, weather_index, is_holiday, holiday_name 
        FROM traffic_full 
        WHERE date <= date('now') 
        ORDER BY date ASC
    """
    try:
        rows = conn.execute(query).fetchall()
    except sqlite3.OperationalError:
        # Fallback if traffic_full doesn't exist yet
        rows = conn.execute('SELECT date, throughput FROM traffic ORDER BY date ASC').fetchall()
        
    conn.close()
    
    data = []
    for row in rows:
        item = {
            'date': row['date'],
            'throughput': row['throughput']
        }
        # Add features if they exist
        if 'weather_index' in row.keys():
            item['weather_index'] = row['weather_index']
            item['is_holiday'] = row['is_holiday']
            item['holiday_name'] = row['holiday_name']
        
        data.append(item)
        
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True)
