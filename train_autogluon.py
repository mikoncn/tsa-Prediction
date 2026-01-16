# train_autogluon.py - 重装挑战者模型
# 功能：使用 AutoGluon 自动机器学习框架进行地毯式模型搜索（Ensemble）。
# 目的：作为 XGBoost Sniper 的对照组（Benchmark），验证现有模型的极限。

import pandas as pd
import numpy as np
import os
import shutil
from autogluon.tabular import TabularPredictor
from datetime import timedelta

# 配置
TRAIN_FILE = 'TSA_Final_Analysis.csv'
MODEL_DIR = 'autogluon_model' # 训练好的模型存放目录
OUTPUT_FILE = 'autogluon_forecast.csv'
TIME_LIMIT = 600 # 训练限时 (秒)，设为 10 分钟。AutoGluon 会在这个时间内尽力而为。

def load_and_prep_data():
    print("Loading data for AutoGluon...")
    df = pd.read_csv(TRAIN_FILE)
    df['date'] = pd.to_datetime(df['date'])
    
    # 特征工程 (保持与 XGBoost 一致，但 AutoGluon 其实能自动处理很多)
    # 我们直接丢给它最原始的尽量多的特征，看它能不能挖掘出新东西
    # 必须把 'date' 这种 timestamp 转换成数字或丢弃，否则 AutoGluon 可能会把它当类别
    # 但时序预测最好保留时间特征。AutoGluon Tabular 模式不擅长处理纯时间列，需手动拆解。
    
    df['day_of_week'] = df['date'].dt.dayofweek
    df['month'] = df['date'].dt.month
    df['year'] = df['date'].dt.year
    df['day'] = df['date'].dt.day
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    
    # 移除疫情期间的异常数据 (2020-03 ~ 2021-12)
    # 挑战者也应该遵守基本规则，否则只会学坏
    mask_pandemic = (df['date'] >= '2020-03-01') & (df['date'] <= '2021-12-31')
    train_df = df[~mask_pandemic].copy()
    
    # 删除无关列 (date本身不入模，只用拆解后的特征)
    # throughput 是 target
    train_data = train_df.drop(columns=['date', 'holiday_name']) # holiday_name 是文本，AutoGluon可以处理，但可能太稀疏，先去掉
    
    # 简单的 NaN 处理
    train_data = train_data.fillna(0)
    
    return train_data, df # 返回全量df用于生成未来特征

def train(train_data):
    print(f"Starting AutoGluon training (Limit: {TIME_LIMIT}s)...")
    
    # 如果存在旧模型，先清理? AutoGluon 支持增量，但为了干净，建议清理
    if os.path.exists(MODEL_DIR):
        shutil.rmtree(MODEL_DIR, ignore_errors=True)
        
    predictor = TabularPredictor(label='throughput', path=MODEL_DIR, eval_metric='mean_absolute_percentage_error').fit(
        train_data,
        time_limit=TIME_LIMIT,
        presets='medium_quality' # fast_training, medium_quality, best_quality
    )
    
    print("Training complete.")
    results = predictor.fit_summary(show_plot=False)
    print(results)
    return predictor

def predict_future(predictor, full_df):
    print("Generating forecast...")
    
    # 构造未来 7 天的特征
    last_date = full_df['date'].max()
    future_dates = [last_date + timedelta(days=i) for i in range(1, 8)]
    
    future_data = []
    
    # 这里有个难点：滞后特征 (Lag) 和 天气 在未来怎么获取？
    # 为简单起见，作为挑战者，我们先只用“日历特征”和“已有静态特征”裸跑，
    # 或者尝试沿用最后一天的天气/Lag。
    # 为了公平对比，Ideally 应该用与 predict_sniper 相同的逻辑填充未来特征。
    # 这里做个简化：假设未来特征已经部分存在于 full_df (如果 build_db 只有历史，那这里就没有)
    # 实际上 traffic_full 只有历史。
    
    # 简易版：只构造时间特征
    for d in future_dates:
        row = {
            'day_of_week': d.dayofweek,
            'month': d.month,
            'year': d.year,
            'day': d.day,
            'is_weekend': 1 if d.dayofweek >= 5 else 0,
            # [TODO] 这些关键特征缺失会导致效果大打折扣
            'weather_index': 0, 
            'is_holiday': 0,
            # 'throughput_lag_7': ... 
        }
        future_data.append(row)
        
    future_df = pd.DataFrame(future_data)
    
    preds = predictor.predict(future_df)
    
    # 保存结果
    output_df = pd.DataFrame({
        'date': future_dates,
        'forecast': preds
    })
    
    output_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Forecast saved to {OUTPUT_FILE}")

def main():
    train_data, full_df = load_and_prep_data()
    predictor = train(train_data)
    predict_future(predictor, full_df)

if __name__ == "__main__":
    main()
