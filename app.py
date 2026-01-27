from flask import Flask, render_template, jsonify
import sqlite3
import pandas as pd
import os
import io
import threading

app = Flask(__name__)

# [NEW] Import config
import sys
# Ensure src can be imported if app.py is run directly
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from src.config import DB_PATH
from src.etl import build_tsa_db, fetch_polymarket, get_weather_features, merge_db
from src.models import train_xgb

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

        conn = get_db_connection()
        # conn.row_factory = sqlite3.Row  # get_db_connection already sets this
        
        # [IMPROVED] Identify the boundary: the latest actual TSA throughput date
        # This ensures Jan 16-19 (if TSA is lagging) are treated as "Future/Forecast" in UI
        query_max_actual = "SELECT max(date) FROM traffic WHERE throughput IS NOT NULL"
        max_actual_row = conn.execute(query_max_actual).fetchone()
        boundary_date = max_actual_row[0] if (max_actual_row and max_actual_row[0]) else None
        
        if not boundary_date:
            from datetime import datetime
            boundary_date = datetime.now().strftime('%Y-%m-%d')
            
        print(f"   [API] Detection Boundary for Forecast: > {boundary_date}")
        
        # 1. 加载未来预测 (Forecast) - From SQLite 'prediction_history'
        # Logic: Get predictions for Date > Latest Actual Date
        query_forecast = """
            SELECT target_date, predicted_throughput, model_run_date, 
                   weather_index, is_holiday, flight_volume, holiday_name 
            FROM prediction_history 
            WHERE target_date > ?
        """
        df_preds = pd.read_sql(query_forecast, conn, params=(boundary_date,))
        
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

        # [NEW] Load latest Sniper prediction (Persistence)
        # Find latest entry in sniper_predictions
        try:
            row_sn = conn.execute("""
                SELECT target_date, predicted_value, flights_volume, is_fallback 
                FROM sniper_predictions 
                ORDER BY created_at DESC 
                LIMIT 1
            """).fetchone()
            
            if row_sn:
                result['sniper_latest'] = {
                    'date': row_sn['target_date'],
                    'predicted_throughput': row_sn['predicted_value'],
                    'flight_volume': row_sn['flights_volume'],
                    'is_fallback': bool(row_sn['is_fallback'])
                }
            else:
                result['sniper_latest'] = None
        except Exception as e:
            print(f"Failed to load saved sniper data: {e}")
            result['sniper_latest'] = None

        # 2. 加载历史验证 (Validation) - From SQLite 'prediction_history' & 'traffic_full'
        # Query History (Past predictions)
        query_hist = """
            SELECT target_date, predicted_throughput, model_run_date, 
                   weather_index, is_holiday, flight_volume
            FROM prediction_history 
            WHERE target_date <= ?
        """
        df_hist = pd.read_sql(query_hist, conn, params=(boundary_date,))
        
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
 
# API V2: 强控协议标头导出 (兼容所有 Flask 版本)
@app.route('/api/v2/secure_export')
def secure_export():
    try:
        conn = get_db_connection()
        query = """
            SELECT p.target_date, p.predicted_throughput
            FROM prediction_history p
            LEFT JOIN traffic t ON p.target_date = t.date
            WHERE (t.throughput IS NULL OR t.throughput = 0)
            AND p.id IN (SELECT MAX(id) FROM prediction_history GROUP BY target_date)
            ORDER BY p.target_date ASC
        """
        df = pd.read_sql(query, conn)
        conn.close()

        if df.empty:
            return "NO_DATA", 200

        # 构建文本内容
        lines = [f"{row['target_date']}: {int(row['predicted_throughput'])}" for _, row in df.iterrows()]
        txt_content = "=== TSA FORECAST LIST ===\n" + "\n".join(lines)
        
        from flask import make_response
        # 手动构建响应对象，避开 send_file 的版本兼容性坑
        response = make_response(txt_content.encode('utf-8-sig'))
        response.headers['Content-Type'] = 'text/plain; charset=utf-8'
        # [核心锁定] 手动注入标准下载标头
        response.headers['Content-Disposition'] = 'attachment; filename=tsa_forecast.txt'
        response.headers['Cache-Control'] = 'no-cache'
        
        return response
    except Exception as e:
        return str(e), 500

@app.route('/api/run_prediction', methods=['POST'])
def run_prediction():
    try:
        print("🚀 正在触发模型运行 (train_xgb.run)...")
        
        # 直接调用函数
        train_xgb.run()
        
        print("✅ Model Run Success")
        return jsonify({
            'status': 'success', 
            'message': '预测完成!数据已更新',
            'summary': 'Executed via Direct Call'
        })
            
    except Exception as e:
        print(f"❌ Execution Error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500

# [NEW] Import Refactored Modules
from src.etl import build_tsa_db, fetch_polymarket, get_weather_features, merge_db
from src.models import train_xgb

# ...

@app.route('/api/update_data', methods=['POST'])
def update_data():
    """
    [实时双规平衡模式]
    1. 同步进行 Sniper 预测 + Polymarket 同步 (保证返回给 n8n 的是最新数据)。
    2. 维持原始返回结构，无缝兼容。
    3. 将耗时较长的全量 ETL 流程放入后台异步处理。
    """
    
    # --- 1. 定位目标日期 ---
    latest_unresolved = None
    try:
        conn = get_db_connection()
        query_max_actual = "SELECT max(date) FROM traffic WHERE throughput IS NOT NULL AND throughput > 0"
        max_row = conn.execute(query_max_actual).fetchone()
        max_actual_date = max_row[0] if (max_row and max_row[0]) else '1970-01-01'
        
        query_pred = """
            SELECT target_date, predicted_throughput, holiday_name, model_run_date
            FROM prediction_history
            WHERE target_date > ?
            ORDER BY target_date ASC, model_run_date DESC
            LIMIT 1
        """
        pred_row = conn.execute(query_pred, (max_actual_date,)).fetchone()
        if pred_row:
            latest_unresolved = dict(pred_row)
        conn.close()
    except Exception as e:
        print(f"⚠️ 预检索未结盘数据失败: {e}")

    # --- 2. 同步执行：实时抓取 Polymarket 赔率 & 快速狙击预测 ---
    sniper_result = None
    market_consensus = None
    
    if latest_unresolved:
        target_date = latest_unresolved['target_date']
        
        # A. [SYNC] 实时同步 Polymarket (较快)
        print(f"🎯 [Sync] 正在实时抓取 Polymarket 最新赔率...")
        from src.etl import fetch_polymarket
        try:
            fetch_polymarket.run(recent=True)
        except Exception as fe:
            print(f"⚠️ Polymarket 同步失败: {fe}")

        # B. [SYNC] 实时运行快速狙击预测 (skip_jit=True，不等待 OpenSky)
        print(f"🎯 [Sync] 正在为 {target_date} 启动快速狙击预测...")
        from src.models import predict_sniper
        try:
            # 使用 skip_jit=True 确保不会因为 OpenSky 429 或耗时而阻塞
            sniper_result = predict_sniper.train_and_predict(target_date, skip_jit=True)
            if sniper_result and "error" in sniper_result: sniper_result = None
        except Exception as se:
            print(f"⚠️ Sniper 快速预测失败: {se}")

        # C. 提取最新的市场共识 (从刚刚同步完成的数据库中读取)
        try:
            conn = get_db_connection()
            query_market = """
                SELECT outcome_label, price 
                FROM market_sentiment_snapshots 
                WHERE target_date = ?
                AND id IN (SELECT MAX(id) FROM market_sentiment_snapshots WHERE target_date = ? GROUP BY outcome_label)
                ORDER BY price DESC LIMIT 1
            """
            market_row = conn.execute(query_market, (target_date, target_date)).fetchone()
            if market_row:
                market_consensus = {
                    "outcome": market_row['outcome_label'],
                    "probability": f"{round(market_row['price'] * 100, 1)}%",
                    "raw_price": market_row['price']
                }
            conn.close()
        except: pass

    # --- 3. 异步启动：耗时/限流任务 (OpenSky & 全量 ETL) ---
    def run_async_pipeline():
        try:
            print(f"\n🚀 [Async] 后台长耗时任务启动 (Target: {target_date if latest_unresolved else 'None'})...")
            
            # A. [Async] OpenSky Removed
            # print("🚀 [Async] OpenSky Skipped (Deprecated)...")

            # B. [ASYNC] 重新运行深度狙击预测 (允许 JIT，补全数据)
            if latest_unresolved:
                try: 
                    print(f"🎯 [Async] 正在为 {target_date} 重新运行深度狙击预测 (允许 JIT)...")
                    predict_sniper.train_and_predict(target_date, skip_jit=False)
                except: pass

            # C. [ASYNC] 全量 ETL 流水线
            print("🚀 [Async] 正在执行全量 ETL 合并与模型重训...")
            build_tsa_db.run(latest=True)
            get_weather_features.run()
            merge_db.run()
            train_xgb.run()
            print("✅ [Async] 后台流程全部完成")
        except Exception as e:
            print(f"❌ [Async] 后台任务崩溃: {e}")

    thread = threading.Thread(target=run_async_pipeline)
    thread.daemon = True
    thread.start()

    # --- 4. 返回包含实时赔率的结果 ---
    return jsonify({
        'status': 'success',
        'message': '数据已实时同步并返回，全量更新已在后台触发。',
        'prediction_sources': {
            'long_term_forecast': latest_unresolved,
            'short_term_sniper': sniper_result,
            'market_sentiment': market_consensus
        },
        'timestamp': pd.Timestamp.now().isoformat()
    })

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
                
                if "error" in data:
                     print(f"❌ Sniper Internal Error: {data['error']}")
                     return jsonify({'status': 'error', 'message': data['error']}), 500
                     
                print(f"✅ Sniper Hit: {data}")
                
                # [NEW] Save to DB (Persistence)
                try:
                    conn = get_db_connection()
                    conn.execute("""
                        INSERT INTO sniper_predictions 
                        (target_date, predicted_value, flights_volume, model_version, is_fallback)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        data.get('date'), 
                        data.get('predicted_throughput'), 
                        data.get('flight_volume'), 
                        data.get('model'), 
                        1 if data.get('is_fallback') else 0
                    ))
                    conn.commit()
                    conn.close()
                    print(f"💾 Sniper Result Saved to DB: {data.get('date')}")
                except Exception as db_err:
                    print(f"⚠️ Failed to save Sniper result: {db_err}")
                
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
    """获取 Polymarket 市场情绪 (仅显示 TSA 官网尚未出分/未结盘的市场)"""
    try:
        conn = get_db_connection()
        
        # 1. 识别已结盘日期 (TSA 官网已出分)
        # 即使日期过了，只要 TSA 没出分，市场就不该下架
        query_resolved = "SELECT date FROM traffic WHERE throughput IS NOT NULL"
        resolved_dates = [row['date'] for row in conn.execute(query_resolved).fetchall()]
        
        # 2. 获取每个 (date, outcome) 的最新快照
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
        
        # 3. 过滤逻辑：剔除已结盘日期
        # [NEW] 允许保留最近 1 个已结盘日期作为参考，其他的全部剔除
        filtered_latest = [r for r in rows_latest if r['target_date'] not in resolved_dates]
        
        # 4. 获取 ~6小时前的价格用于对标增量
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
        
        history_map = {}
        for r in all_rows:
            key = f"{r['target_date']}|{r['outcome_label']}"
            fetched_dt = datetime.strptime(r['fetched_at'], '%Y-%m-%d %H:%M:%S')
            if key not in history_map:
                history_map[key] = []
            history_map[key].append((fetched_dt, r['price']))
            
        results = []
        for r in filtered_latest:
            key = f"{r['target_date']}|{r['outcome_label']}"
            curr_price = r['current_price']
            
            change_6h = 0.0
            if key in history_map:
                points = history_map[key]
                latest_ts = points[-1][0]
                target_ts = latest_ts - pd.Timedelta(hours=6)
                
                closest_price = None
                min_diff_seconds = 999999
                for (ts, p) in points:
                    diff = abs((ts - target_ts).total_seconds())
                    if diff < min_diff_seconds:
                        min_diff_seconds = diff
                        closest_price = p
                
                if closest_price is not None:
                     change_6h = curr_price - closest_price
                     
            results.append({
                'target_date': r['target_date'],
                'market_slug': r['market_slug'],
                'outcome': r['outcome_label'],
                'price': curr_price,
                'change_6h': round(change_6h, 3),
                'fetched_at': r['fetched_at']
            })
            
        grouped = {}
        for item in results:
            d = item['target_date']
            if d not in grouped: grouped[d] = []
            grouped[d].append(item)
            
        return jsonify(grouped)
        
    except Exception as e:
        print(f"Error in market_sentiment: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/sync_market_sentiment', methods=['POST'])
def sync_market_sentiment():
    """实时突击同步：仅抓取当前未结盘的活跃市场赔率"""
    try:
        import subprocess
        import sys
        
        # 运行爬虫脚本，并传入 --active 参数（待实现）
        # 如果爬虫暂不支持 --active，先全量同步 T-1 到 T+7
        print("⚡ 正在执行实时赔率同步 (Targeted Sync)...")
        result = subprocess.run(
            [sys.executable, '-m', 'src.etl.fetch_polymarket', '--recent'], 
            capture_output=True, text=True, encoding='utf-8', errors='replace',
            timeout=30
        )
        
        if result.returncode == 0:
            return jsonify({'status': 'success', 'message': '赔率已实时同步'})
        else:
            return jsonify({'status': 'error', 'message': result.stderr}), 500
            
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
