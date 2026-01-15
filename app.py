from flask import Flask, render_template, jsonify
import sqlite3
import pandas as pd
import os

app = Flask(__name__)
DB_PATH = 'tsa_data.db'

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
# API: 获取预测结果和历史验证数据
@app.route('/api/predictions')
def get_predictions():
    result = {}
    
    # 1. 加载未来 7 天的预测结果 (从 train_xgb.py 生成的 CSV)
    try:
        df_forecast = pd.read_csv("xgb_forecast.csv")
        result['forecast'] = df_forecast.to_dict(orient='records')
    except Exception as e:
        result['forecast'] = []
        print(f"Error loading forecast: {e}")

    # 2. 加载历史预测记录并与真实流量合并 (用于模型回测表格)
    try:
        print(f"DEBUG: CWD = {os.getcwd()}")
        if os.path.exists("prediction_history.csv"):
            print("DEBUG: History file found.")
            df_hist = pd.read_csv("prediction_history.csv")
            df_actual = pd.read_csv("TSA_Final_Analysis.csv")
            
            print(f"DEBUG: Hist Len={len(df_hist)}, Actual Len={len(df_actual)}")
            
            # Merge Actuals into History
            # df_hist: target_date, predicted_throughput, model_run_date
            # df_actual: date, throughput
            
            # Ensure dates are strings YYYY-MM-DD
            df_hist['target_date'] = pd.to_datetime(df_hist['target_date']).dt.strftime('%Y-%m-%d')
            df_actual['date'] = pd.to_datetime(df_actual['date']).dt.strftime('%Y-%m-%d')
            
            merged = pd.merge(df_hist, df_actual, left_on='target_date', right_on='date', how='inner')
            print(f"DEBUG: Merged Len={len(merged)}")
            
            # Logic: For each target_date, find the prediction made 1 day before (or latest available)
            # Simple approach: Sort by model_run_date desc, drop duplicates on target_date
            merged = merged.sort_values('model_run_date', ascending=False).drop_duplicates('target_date')
            
            # Calculate Error
            merged['difference'] = merged['predicted_throughput'] - merged['throughput']
            merged['error_rate'] = (merged['difference'].abs() / merged['throughput']) * 100
            
            # [FIXES] Drop redundant 'date' from actuals before rename to avoid collision
            if 'date' in merged.columns:
                merged = merged.drop(columns=['date'])

            # Rename for frontend
            merged = merged.rename(columns={
                'target_date': 'date',
                'throughput': 'actual',
                'predicted_throughput': 'predicted'
            })
            
            # Sort by date asc (dashboard.js expects old->new to slice last 15)
            merged = merged.sort_values('date', ascending=True)
            
            count = len(merged)
            print(f"DEBUG: Final Validation Count = {count}")
            
            # [CRITICAL FIX] Handle NaN/Inf for JSON compliance
            # Replace Inf with 0, NaN with 0 (or None)
            merged = merged.fillna(0)
            
            # [关键逻辑] 只返回前端需要的字段
            result['validation'] = merged[['date', 'actual', 'predicted', 'difference', 'error_rate']].to_dict(orient='records')
        else:
            print("DEBUG: prediction_history.csv NOT found.")
            result['validation'] = []
    except Exception as e:
        result['validation'] = []
        print(f"Error loading validation log: {e}")
        
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
            [sys.executable, 'train_xgb.py'], 
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
            {'name': '抓取最新TSA数据', 'cmd': [sys.executable, 'build_tsa_db.py', '--latest'], 'timeout': 30},
            {'name': '同步天气特征', 'cmd': [sys.executable, 'get_weather_features.py'], 'timeout': 45},
            {'name': '合并数据库', 'cmd': [sys.executable, 'merge_db.py'], 'timeout': 30}
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

if __name__ == '__main__':
    app.run(debug=True)
