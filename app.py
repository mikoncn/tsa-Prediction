from flask import Flask, render_template, jsonify
import sqlite3
import pandas as pd
import os

app = Flask(__name__)

# [NEW] Import config
import sys
# Ensure src can be imported if app.py is run directly
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from src.config import DB_PATH

# 获取数据库连接的助手函数
def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # 允许通过列名访问结果
    return conn

# 主页路由：返回仪表盘 HTML
@app.route('/')
def index():
    return render_template('index.html')

# API: 获取历史流量数据 (用于绘制主图表)
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
# API: 获取生数据 (Raw Data) - 支持分页
@app.route('/api/raw_data')
def get_raw_data():
    try:
        from flask import request
        limit = int(request.args.get('limit', 15))
        offset = int(request.args.get('offset', 0))
        
        conn = get_db_connection()
        
        # 动态查询所有字段
        # 我们先查一下列名，确保全量因子都能获取
        # 核心因子: date, throughput, weather_index, is_holiday, holiday_name, 
        #           flight_volume, days_to_nearest_holiday, is_off_peak_workday, 
        #           is_spring_break, throughput_lag_7
        
        # 检查表是否存在
        check_query = "SELECT name FROM sqlite_master WHERE type='table' AND name='traffic_full'"
        if not conn.execute(check_query).fetchone():
            return jsonify({'error': 'Table traffic_full not ready'}), 404

        # 获取所有列名
        cursor = conn.execute("PRAGMA table_info(traffic_full)")
        columns = [row['name'] for row in cursor.fetchall()]
        
        # 构建查询
        col_str = ", ".join(columns)
        # [FIX] User requested to limit future data to T+3 days to avoid empty rows
        query = f"SELECT {col_str} FROM traffic_full WHERE date <= date('now', '+3 days') ORDER BY date DESC LIMIT ? OFFSET ?"
        
        rows = conn.execute(query, (limit, offset)).fetchall()
        conn.close()
        
        data = []
        for row in rows:
            # 将 sqlite.Row 转为普通 dict
            item = dict(row)
            data.append(item)
            
        return jsonify({
            'status': 'success',
            'data': data,
            'pagination': {'limit': limit, 'offset': offset}
        })
        
    except Exception as e:
        print(f"Error in get_raw_data: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

# API: 获取预测结果和历史验证数据
@app.route('/api/predictions')
def get_predictions():
    result = {}
    
    try:
        conn = sqlite3.connect('tsa_data.db')
        conn.row_factory = sqlite3.Row
        
        from datetime import datetime
        now_str = datetime.now().strftime('%Y-%m-%d')
        
        # 1. 加载未来预测 (Forecast) - From SQLite 'prediction_history'
        # Logic: Get predictions for Date >= Today
        # Logic: Get predictions for Date >= Today
        query_forecast = """
            SELECT target_date, predicted_throughput, model_run_date, 
                   weather_index, is_holiday, flight_volume, holiday_name 
            FROM prediction_history 
            WHERE target_date >= ?
        """
        df_preds = pd.read_sql(query_forecast, conn, params=(now_str,))
        
        if not df_preds.empty:
            # Dedupe: keep latest model_run_date for each target_date
            df_preds['target_date'] = pd.to_datetime(df_preds['target_date']).dt.strftime('%Y-%m-%d')
            # Sort by run_date DESC, keep first
            df_forecast = df_preds.sort_values('model_run_date', ascending=False).drop_duplicates('target_date')
            # Sort by date ASC for chart
            df_forecast = df_forecast.sort_values('target_date')
            # Fill NaNs for display
            df_forecast[['weather_index', 'is_holiday', 'flight_volume']] = df_forecast[['weather_index', 'is_holiday', 'flight_volume']].fillna(0)
            
            result['forecast'] = df_forecast[['target_date', 'predicted_throughput', 'weather_index', 'is_holiday', 'flight_volume', 'holiday_name']].rename(columns={
                'target_date': 'ds',
                'predicted_throughput': 'predicted_throughput'
            }).to_dict(orient='records')
        else:
            result['forecast'] = []

        # 2. 加载历史验证 (Validation) - From SQLite 'prediction_history' & 'traffic_full'
        # Query History (Past predictions)
        query_hist = """
            SELECT target_date, predicted_throughput, model_run_date, 
                   weather_index, is_holiday, flight_volume
            FROM prediction_history 
            WHERE target_date < ?
        """
        df_hist = pd.read_sql(query_hist, conn, params=(now_str,))
        
        # [FIX] Generate 'History' (Orange Line) separately from Validation
        # History should show ALL past predictions, even if we don't have actuals yet.
        if not df_hist.empty:
            # 1. Standardize formatting
            df_hist['target_date'] = pd.to_datetime(df_hist['target_date']).dt.strftime('%Y-%m-%d')
            # 2. Keep latest prediction per date
            df_hist_clean = df_hist.sort_values('model_run_date', ascending=False).drop_duplicates('target_date')
            # 3. Sort for chart
            df_hist_clean = df_hist_clean.sort_values('target_date')
            # Fill NaNs
            df_hist_clean[['weather_index', 'is_holiday', 'flight_volume']] = df_hist_clean[['weather_index', 'is_holiday', 'flight_volume']].fillna(0)
            
            result['history'] = df_hist_clean[['target_date', 'predicted_throughput', 'weather_index', 'is_holiday', 'flight_volume']].rename(columns={
                'target_date': 'date',
                'predicted_throughput': 'predicted'
            }).to_dict(orient='records')
        else:
            result['history'] = []
            
        # Query Actuals (From traffic_full)
        query_actual = "SELECT date, throughput FROM traffic_full WHERE throughput IS NOT NULL"
        df_actual = pd.read_sql(query_actual, conn)
        
        conn.close()
        
        # Validation Table (Only where we have BOTH Prediction AND Actuals)
        if not df_hist.empty and not df_actual.empty:
            # Standardization
            # df_hist['target_date'] is already standardized above
            df_actual['date'] = pd.to_datetime(df_actual['date']).dt.strftime('%Y-%m-%d')
            
            # Merge
            merged = pd.merge(df_hist, df_actual, left_on='target_date', right_on='date', how='inner')
            
            # Keep latest prediction per target date (using target_date for dedupe logic works, or date)
            merged = merged.sort_values('model_run_date', ascending=False).drop_duplicates('target_date')
            
            # Calculate Error
            merged['difference'] = merged['predicted_throughput'] - merged['throughput']
            merged['error_rate'] = (merged['difference'].abs() / merged['throughput']) * 100
            
            # Formatting
            # We already have 'date' from df_actual. We don't need to rename target_date to date.
            # But we might need to ensure target_date is dropped or just ignored.
            merged = merged.rename(columns={
                'throughput': 'actual',
                'predicted_throughput': 'predicted'
            })
            
            # Select columns explicitly
            merged = merged[['date', 'actual', 'predicted', 'difference', 'error_rate']]
            
            merged = merged.sort_values('date', ascending=True)
            merged = merged.fillna(0)
            
            result['validation'] = merged[['date', 'actual', 'predicted', 'difference', 'error_rate']].to_dict(orient='records')
        else:
            result['validation'] = []
            
    except Exception as e:
        print(f"Error in get_predictions (DB Mode): {e}")
        import traceback
        traceback.print_exc()
        result['forecast'] = []
        result['validation'] = []
        result['history'] = []
        
    return jsonify(result)

# API: 点击页面按钮时手动触发模型重新训练和预测
@app.route('/api/run_prediction', methods=['POST'])
def run_prediction():
    try:
        import subprocess
        import sys
        print("🚀 正在触发模型运行 (train_xgb.py)...")
        print(f"   Python executable: {sys.executable}")
        print(f"   Working directory: {os.getcwd()}")
        
        # 运行子进程执行训练脚本
        # 注意：此处处理了 Windows 环境下的 GBK 编码问题
        result = subprocess.run(
            [sys.executable, '-m', 'src.models.train_xgb'], 
            capture_output=True, 
            text=True,
            encoding='utf-8',
            errors='replace',  # 解码失败时替换字符而非报错
            cwd=os.getcwd(),
            timeout=60  # 60秒超时保护
        )
        
        # 打印完整输出用于调试
        if result.stdout:
            print(f"📝 STDOUT:\n{result.stdout}")
        if result.stderr:
            print(f"⚠️ STDERR:\n{result.stderr}")
        
        if result.returncode == 0:
            print("✅ Model Run Success")
            # 提取最后几行输出作为摘要
            output_lines = result.stdout.strip().split('\n')
            summary = '\n'.join(output_lines[-5:]) if len(output_lines) > 5 else result.stdout
            return jsonify({
                'status': 'success', 
                'message': '预测完成!数据已更新',
                'summary': summary
            })
        else:
            error_msg = result.stderr if result.stderr else result.stdout
            print(f"❌ Model Run Failed (returncode={result.returncode})")
            return jsonify({
                'status': 'error', 
                'message': f'模型运行失败: {error_msg}'
            }), 500
            
    except subprocess.TimeoutExpired:
        print(f"❌ Timeout: 模型运行超过60秒")
        return jsonify({'status': 'error', 'message': '模型运行超时(>60秒)'}), 500
    except Exception as e:
        print(f"❌ Execution Error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500

# API: 一键更新数据(抓取TSA+天气+合并)
@app.route('/api/update_data', methods=['POST'])
def update_data():
    try:
        import subprocess
        import sys
        print("🔄 开始数据更新流程...")
        
        steps = [
            {'name': '抓取最新TSA数据', 'cmd': [sys.executable, '-m', 'src.etl.build_tsa_db', '--latest'], 'timeout': 30},
            {'name': '同步天气特征', 'cmd': [sys.executable, '-m', 'src.etl.get_weather_features'], 'timeout': 45},
            {'name': '合并数据库', 'cmd': [sys.executable, '-m', 'src.etl.merge_db'], 'timeout': 30},
            {'name': '全量模型重训(Persistence)', 'cmd': [sys.executable, '-m', 'src.models.train_xgb'], 'timeout': 120}
        ]
        
        results = []
        for step in steps:
            print(f"\n[步骤] {step['name']}...")
            result = subprocess.run(
                step['cmd'], capture_output=True, text=True,
                encoding='utf-8', errors='replace',
                cwd=os.getcwd(), timeout=step['timeout']
            )
            
            if result.returncode == 0:
                print(f"✅ {step['name']} 完成")
                output_lines = result.stdout.strip().split('\n')
                summary = '\n'.join(output_lines[-3:]) if len(output_lines) > 3 else result.stdout
                results.append({'step': step['name'], 'status': 'success', 'summary': summary})
            else:
                error_msg = result.stderr if result.stderr else result.stdout
                print(f"❌ {step['name']} 失败: {error_msg}")
                return jsonify({'status': 'error', 'message': f'{step["name"]}失败', 'error': error_msg}), 500
        
        print("\n✅ 数据更新流程全部完成")
        return jsonify({'status': 'success', 'message': '数据更新成功!', 'results': results})
        
    except subprocess.TimeoutExpired as e:
        print(f"❌ 超时: {e}")
        return jsonify({'status': 'error', 'message': f'操作超时: {e}'}), 500
    except Exception as e:
        print(f"❌ 错误: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500

# API: 狙击模型 (T+0 Nowcasting)
@app.route('/api/predict_sniper', methods=['POST'])
def predict_sniper():
    try:
        import subprocess
        import sys
        import json
        
        # Determine target date? For now default to script default (Today/Tomorrow)
        # Or accept from JSON body if needed
        
        print("🎯 启动狙击模型 (Sniper Mode)...")
        
        # Run script
        result = subprocess.run(
            [sys.executable, '-m', 'src.models.predict_sniper'],
            capture_output=True,
            text=True,
            encoding='utf-8', 
            errors='replace',
            cwd=os.getcwd(),
            timeout=30 # Fast timeout
        )
        
        if result.returncode == 0:
            # Parse JSON from stdout
            try:
                # Script might print other things, find the JSON line
                lines = result.stdout.strip().split('\n')
                # Assume last line is JSON
                json_str = lines[-1]
                data = json.loads(json_str)
                
                # [FIX] Check for internal script error
                if "error" in data:
                     print(f"❌ Sniper Internal Error: {data['error']}")
                     return jsonify({'status': 'error', 'message': data['error']}), 500
                     
                print(f"✅ Sniper Hit: {data}")
                return jsonify({'status': 'success', 'data': data})
            except Exception as parse_err:
                print(f"⚠️ JSON Parse Error: {parse_err}. Stdout: {result.stdout}")
                return jsonify({'status': 'error', 'message': '无法解析模型输出', 'raw': result.stdout}), 500
        else:
            print(f"❌ Sniper Missed: {result.stderr}")
            return jsonify({'status': 'error', 'message': '模型运行失败', 'error': result.stderr}), 500
            
    except Exception as e:
        print(f"❌ Sniper Error: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/run_challenger', methods=['POST'])
def run_challenger():
    """触发 FLAML 深度分析 (Challenger Model)"""
    try:
        import subprocess
        import sys
        import json
        
        print("🟣 启动 FLAML 挑战者训练任务...")
        
        # 运行训练脚本
        result = subprocess.run(
            [sys.executable, '-m', 'src.models.train_challenger'],
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            timeout=600 # 10分钟超时
        )
        
        if result.returncode != 0:
            return jsonify({
                'status': 'error', 
                'message': f"Training failed: {result.stderr}"
            }), 500
            
        # 读取生成的摘要
        if os.path.exists("challenger_summary.json"):
            with open("challenger_summary.json", 'r') as f:
                summary = json.load(f)
            return jsonify({
                'status': 'success',
                'data': summary
            })
        else:
             return jsonify({
                'status': 'error', 
                'message': "Model trained but no summary file found."
            }), 500
            
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/market_sentiment')
def get_market_sentiment():
    """获取 Polymarket 市场情绪 (含 6H 涨跌幅)"""
    try:
        conn = get_db_connection()
        
        # 1. 获取每个 (date, outcome) 的最新价格
        # 使用窗口函数或 Group By Max ID (SQLite简单处理)
        # 这里我们需要针对每个 target_date + outcome_label 分组
        
        query_latest = """
            SELECT target_date, outcome_label, price as current_price, fetched_at, market_slug
            FROM market_sentiment_snapshots 
            WHERE id IN (
                SELECT MAX(id) 
                FROM market_sentiment_snapshots 
                GROUP BY target_date, outcome_label
            )
            ORDER BY target_date ASC, outcome_label ASC
        """
        
        rows_latest = conn.execute(query_latest).fetchall()
        
        # 2. 获取 ~6小时前的价格 (或最接近的旧数据)
        # 简单策略：查找 fetched_at <= now - 6h 的最新一条
        # 但这种对于批量查询很慢。
        # 优化策略：Load full recent history in memory (volume is low enough) OR single complex query.
        # 鉴于数据量每天仅几百条，载入内存处理最快。
        
        # Let's use Python to compute diffs from raw rows for simplicity and robustness
        query_all_recent = """
            SELECT target_date, outcome_label, price, fetched_at
            FROM market_sentiment_snapshots
            WHERE fetched_at >= datetime('now', '-24 hours')
            ORDER BY fetched_at ASC
        """
        all_rows = conn.execute(query_all_recent).fetchall()
        conn.close()
        
        from datetime import datetime
        import pandas as pd
        
        # Group by key
        history_map = {} # Key: "date|outcome" -> List of (dt, price)
        
        for r in all_rows:
            key = f"{r['target_date']}|{r['outcome_label']}"
            fetched_dt = datetime.strptime(r['fetched_at'], '%Y-%m-%d %H:%M:%S')
            if key not in history_map:
                history_map[key] = []
            history_map[key].append((fetched_dt, r['price']))
            
        results = []
        
        # Process latest rows
        now = datetime.utcnow() # SQLite usually UTC
        # If SQLite fetched_at is local, might need adjustment. defaulted to CURRENT_TIMESTAMP (UTC).
        
        # Re-iterate latest rows from SQL (or compute from history map latest)
        # Using SQL latest is safer
        for r in rows_latest:
            key = f"{r['target_date']}|{r['outcome_label']}"
            curr_price = r['current_price']
            slug = r['market_slug']
            
            # Find 6h ago price
            # Ideal: Price at (Now - 6h)
            # Logic: Find closest snapshot that is older than 5.5h? Or just finding the one closest to 6h mark?
            # Let's try to find a data point between 5h and 7h ago.
            # If not found, fallback to oldest available within 24h?
            
            change_6h = 0.0
            
            if key in history_map:
                points = history_map[key]
                # Points are sorted ASC by time
                # We want point closest to (latest_time - 6h)
                
                # Assume latest fetch was just now-ish
                latest_ts = points[-1][0]
                target_ts = latest_ts - pd.Timedelta(hours=6)
                
                closest_price = None
                min_diff_seconds = 999999
                
                for (ts, p) in points:
                    # check difference
                    diff = abs((ts - target_ts).total_seconds())
                    # We only care if the point is ACTUALLY in the past relative to latest
                    # and roughly around the 6h mark (e.g., within 3h to 9h window?)
                    # Simplification: Just find the record closest to target_ts
                    
                    if diff < min_diff_seconds:
                        min_diff_seconds = diff
                        closest_price = p
                
                # Calculate change
                if closest_price is not None:
                     change_6h = curr_price - closest_price
                     
            results.append({
                'target_date': r['target_date'],
                'market_slug': slug,
                'outcome': r['outcome_label'],
                'price': curr_price,
                'change_6h': round(change_6h, 3),
                'fetched_at': r['fetched_at']
            })
            
        # Group by Date for frontend convenience
        grouped = {}
        for item in results:
            d = item['target_date']
            if d not in grouped: grouped[d] = []
            grouped[d].append(item)
            
        return jsonify(grouped)
        
    except Exception as e:
        print(f"Error in market_sentiment: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5001)
