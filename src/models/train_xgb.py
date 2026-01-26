import pandas as pd
import numpy as np
import holidays
import os
import sqlite3
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_percentage_error
import warnings
import sys

# Add src to path if run directly
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.config import DB_PATH, FORECAST_MODEL_PATH

warnings.filterwarnings('ignore')

def run():
    # 1. 加载数据 (From DB)
    print("Loading data from SQLite (traffic_full)...")
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql("SELECT * FROM traffic_full", conn)
    except Exception as e:
        print(f"Error reading traffic_full: {e}")
        conn.close()
        return
        
    conn.close()
    
    if df.empty:
        print("Traffic data is empty. Aborting training.")
        return

    df['ds'] = pd.to_datetime(df['date'])
    df['y'] = df['throughput']
    df = df.sort_values('ds').reset_index(drop=True)

    # [NEW] Load Flight Stats (OpenSky) & Weather
    try:
        print("Loading Weather & Flight Stats from SQLite...")
        conn_extra = sqlite3.connect(DB_PATH, timeout=30)
        
        # 1. Flights
        try:
            df_flights = pd.read_sql("SELECT date, SUM(arrival_count) as total_flights FROM flight_stats GROUP BY date", conn_extra)
            df_flights['date'] = pd.to_datetime(df_flights['date'])
        except:
            df_flights = pd.DataFrame(columns=['date', 'total_flights'])
        
        # 2. Weather
        try:
            df_weather = pd.read_sql("SELECT date, index_value as weather_index FROM daily_weather_index", conn_extra)
            df_weather['date'] = pd.to_datetime(df_weather['date'])
        except:
            df_weather = pd.DataFrame(columns=['date', 'weather_index'])
        
        conn_extra.close()
        
        # Merge Flights
        df = df.merge(df_flights, left_on='ds', right_on='date', how='left')
        df.drop(columns=['date_y'], inplace=True, errors='ignore') 
        df.rename(columns={'date_x': 'date'}, inplace=True, errors='ignore')
        
        # Merge Weather
        if 'weather_index' not in df.columns:
            print("   Merging weather_index from daily_weather_index...")
            df = df.merge(df_weather, left_on='ds', right_on='date', how='left')
            df.drop(columns=['date_y'], inplace=True, errors='ignore')
            df.rename(columns={'date_x': 'date'}, inplace=True, errors='ignore')
        
        if 'weather_index' in df.columns:
            df['weather_index'] = df['weather_index'].fillna(0).astype(int)
        else:
            print("   WARNING: weather_index missing after merge! Forcing 0.")
            df['weather_index'] = 0

        # [OPTIMIZATION] Forward Fill
        # Create total_flights if missing
        if 'total_flights' not in df.columns:
            df['total_flights'] = 0
            
        df['total_flights'] = df['total_flights'].fillna(method='ffill').fillna(0)
        
        print(f"   Merged {len(df_flights)} flight records.")
    except Exception as e:
        print(f"   WARNING: Could not load flight stats/weather: {e}")
        if 'total_flights' not in df.columns: df['total_flights'] = 0
        if 'weather_index' not in df.columns: df['weather_index'] = 0

    print("Generating features for XGBoost...")

    # A. 时间特征 (Time Components)
    df['day_of_week'] = df['ds'].dt.dayofweek
    df['month'] = df['ds'].dt.month
    df['year'] = df['ds'].dt.year
    df['day_of_year'] = df['ds'].dt.dayofyear
    df['week_of_year'] = df['ds'].dt.isocalendar().week.astype(int)
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)

    # B. 滞后特征 (Lag Features)
    # df['throughput_lag_7'] might come from DB, else calculate
    if 'throughput_lag_7' not in df.columns:
        df['throughput_lag_7'] = df['y'].shift(7)
        
    df['lag_7'] = df['throughput_lag_7']
    df['lag_364'] = df['y'].shift(364)

    # C. 业务特征 (Business Logic)
    match_month = df['ds'].dt.month.isin([1, 2, 9, 10])
    match_day = df['ds'].dt.dayofweek.isin([1, 2]) # Tue, Wed
    df['is_off_peak_workday'] = (match_month & match_day).astype(int)

    # History Lags
    df['y_lag_7'] = df['y'].shift(7).fillna(method='bfill')

    # [NEW] Hybrid Lag Strategy (Aligned Seasonality)
    df['lag_364'] = df['y'].shift(364).fillna(method='bfill')

    # Vectorized implementation using numpy
    import numpy as np

    # Create a 'lag_365' for fixed comparison
    df['lag_365'] = df['y'].shift(365).fillna(method='bfill')

    fixed_date_holidays = [
        "New Year's Day", 
        "Independence Day", 
        "Veterans Day", 
        "Christmas Day"
    ]

    df['is_fixed_holiday'] = 0
    mask_fixed = df['ds'].apply(lambda d: 
        (d.month == 1 and d.day == 1) or
        (d.month == 7 and d.day == 4) or
        (d.month == 11 and d.day == 11) or
        (d.month == 12 and d.day == 25)
    )
    df.loc[mask_fixed, 'is_fixed_holiday'] = 1

    # Apply Vectorized Swap
    df['lag_364'] = np.where(df['is_fixed_holiday'] == 1, df['lag_365'], df['lag_364'])

    # Clean up temp cols
    df.drop(columns=['lag_365', 'is_fixed_holiday'], inplace=True)

    df['throughput_lag_7'] = df['y_lag_7'] # Alias for consistency
    df['flight_lag_1'] = df['total_flights'].shift(1).fillna(method='bfill')
    df['flight_ma_7'] = df['total_flights'].rolling(window=7).mean().shift(1).fillna(method='bfill')

    # [NEW] Whitelist & Clamping Logic for Historical Data
    print("   Calculating holiday distances for training data (Using Unified Tier 1/2 List)...")
    from src.utils.holiday_utils import TARGET_HOLIDAYS, get_us_holidays
    
    # Load raw holidays to get dates
    us_holidays = get_us_holidays(2019, 2030)

    major_holiday_dates = []

    # 1. Add Standard Federal Holidays (Filtered by Whitelist)
    for date, name in us_holidays.items():
        if any(target in name for target in TARGET_HOLIDAYS):
            major_holiday_dates.append(pd.Timestamp(date))

    # 2. Add Good Friday (Easter - 2 days)
    from dateutil.easter import easter
    for y in range(2019, 2030):
        try:
            easter_date = easter(y)
            good_friday = pd.Timestamp(easter_date) - pd.Timedelta(days=2)
            major_holiday_dates.append(good_friday)
        except: pass

    # Remove duplicates and sort
    major_holiday_dates = sorted(list(set(major_holiday_dates)))

    df['days_to_nearest_holiday'] = 15 # Default
    
    # Optimize loop? For now keep iterative as it works
    for idx, row in df.iterrows():
        d = row['ds']
        min_dist = 999 
        best_dist = 15
        for h_date in major_holiday_dates:
            diff_days = (d - h_date).days
            if abs(diff_days) < abs(min_dist):
                min_dist = diff_days
                best_dist = diff_days
        
        # Clamp logic: +/- 14 window
        if best_dist > 14: best_dist = 15
        elif best_dist < -14: best_dist = -15
        df.at[idx, 'days_to_nearest_holiday'] = best_dist

    # [NEW] Weather Rebound Logic (Revenge Travel Index)
    # Logic: Higher past weather indices = Higher current pent-up demand
    # revenge_index = 0.5*w_lag_1 + 0.3*w_lag_2 + 0.2*w_lag_3
    df['w_lag_1'] = df['weather_index'].shift(1).fillna(0)
    df['w_lag_2'] = df['weather_index'].shift(2).fillna(0)
    df['w_lag_3'] = df['weather_index'].shift(3).fillna(0)
    df['revenge_index'] = (df['w_lag_1'] * 0.5) + (df['w_lag_2'] * 0.3) + (df['w_lag_3'] * 0.2)
    
    # [NEW] Long Weekend Logic
    df['is_long_weekend'] = 0
    mask_long = (df['is_holiday'] == 1) & (df['day_of_week'].isin([0, 4]))
    df.loc[mask_long, 'is_long_weekend'] = 1

    # D. 填充缺失值
    features = [
        'day_of_week', 'month', 'year', 'day_of_year', 'week_of_year', 'is_weekend',
        'weather_index', 'is_holiday', 'is_spring_break', 'is_off_peak_workday',
        'is_holiday_exact_day', 'days_to_nearest_holiday',
        'revenge_index', 'is_long_weekend',
        'lag_7', 'lag_364', 'flight_lag_1', 'flight_ma_7'
    ]

    # Ensure cols exist
    for col in features:
        if col not in df.columns:
            df[col] = 0

    # 丢弃无法计算 lag_364 的早期数据
    df_model = df.dropna(subset=['lag_364']).copy()
    for col in features:
        df_model[col] = df_model[col].fillna(0)

    # 3. 划分训练集与测试集 (Backtest Strategy)
    pandemic_start = pd.Timestamp('2020-03-01')
    pandemic_end = pd.Timestamp('2021-12-31')
    train_cutoff = pd.Timestamp('2025-12-31')

    mask_train_period = (df_model['ds'] <= train_cutoff)
    mask_pandemic = (df_model['ds'] >= pandemic_start) & (df_model['ds'] <= pandemic_end)

    train_df = df_model[mask_train_period & (~mask_pandemic)]

    # 测试集: 2026-01-01 ~ 2026-01-13 (或最近)
    test_start = pd.Timestamp('2026-01-01')
    test_end = pd.Timestamp('2026-01-13') # Fixed range for backtest
    
    # Dynamic test end?
    if df_model['ds'].max() > test_end:
        test_end = df_model['ds'].max()

    test_df = df_model[(df_model['ds'] >= test_start) & (df_model['ds'] <= test_end)].copy()

    X_train = train_df[features]
    y_train = train_df['y']

    X_test = test_df[features]
    y_test = test_df['y']

    print(f"Training XGBoost on {len(X_train)} rows...")
    print(f"Testing on {len(X_test)} rows ({test_start.date()} ~ {test_end.date()})")

    # 4. 训练模型
    model = XGBRegressor(
        n_estimators=1000,
        learning_rate=0.05,
        max_depth=5,
        subsample=0.8,
        colsample_bytree=0.8,
        n_jobs=-1,
        random_state=42
    )

    model.fit(X_train, y_train)

    # 5. 预测与评估
    if not X_test.empty:
        y_pred = model.predict(X_test)
        test_df['yhat_xgb'] = y_pred
        test_df['diff'] = test_df['yhat_xgb'] - test_df['y']
        test_df['error_pct'] = (test_df['diff'].abs() / test_df['y']) * 100
        mape = test_df['error_pct'].mean()
        print(f"XGBoost finished. MAPE: {mape:.2f}%")
        
        # Save Validation Results
        validation_df = test_df[['ds', 'y', 'yhat_xgb', 'diff', 'error_pct']].rename(columns={
            'ds': 'date', 
            'y': 'actual', 
            'yhat_xgb': 'predicted',
            'diff': 'difference',
            'error_pct': 'error_rate'
        })
        validation_df.to_csv("xgb_validation.csv", index=False)
        print("Validation results saved to xgb_validation.csv")

    # ==========================================
    # 7. 部署模式: 预测未来 5 天 (Production Forecast)
    # ==========================================
    print("\n[FORECAST] Generating Future Forecast (Next 7 Days)...")

    # [CRITICAL UPDATE] Retrain on FULL DATA
    print("   [RETRAIN] Retraining model on ALL available history (2019-Present)...")
    mask_full_train = (~mask_pandemic) & (df_model['y'].notnull())
    full_train_df = df_model[mask_full_train]

    X_full = full_train_df[features]
    y_full = full_train_df['y']

    model_full = XGBRegressor(
        n_estimators=1200, 
        learning_rate=0.05,
        max_depth=5,
        subsample=0.8,
        colsample_bytree=0.8,
        n_jobs=-1,
        random_state=42
    )
    model_full.fit(X_full, y_full)
    print(f"   Full Model Trained on {len(full_train_df)} rows.")
        
    print(f"   [PERSISTENCE] Saving forecast model to {FORECAST_MODEL_PATH}...")
    model_full.save_model(FORECAST_MODEL_PATH)
    print("   [PERSISTENCE] Model saved successfully.")

    # 找到最后一条"真实有数据"的日期
    last_actual_row = df[df['y'].notnull()].iloc[-1]
    last_actual_date = last_actual_row['ds']
    print(f"Last Actual Data Date: {last_actual_date.date()}")

    # 从"有数据"的后一天开始预测
    print("   [DEBUG] STARTING FORECAST GENERATION PHASE")
    try:
        print("   [DEBUG] Generating future_dates...")
        future_dates = pd.date_range(start=last_actual_date + pd.Timedelta(days=1), periods=14)
        print(f"   [DEBUG] Generated {len(future_dates)} days: {future_dates[0]} to {future_dates[-1]}")

        # 构建未来特征 DataFrame
        future_df = pd.DataFrame({'ds': future_dates})

        # A. 时间特征
        future_df['day_of_week'] = future_df['ds'].dt.dayofweek
        future_df['month'] = future_df['ds'].dt.month
        future_df['year'] = future_df['ds'].dt.year
        future_df['day_of_year'] = future_df['ds'].dt.dayofyear
        future_df['week_of_year'] = future_df['ds'].dt.isocalendar().week.astype(int)
        future_df['is_weekend'] = future_df['day_of_week'].isin([5, 6]).astype(int)

        # B. 滞后特征 (Lags)
        def get_lag_value(target_date, lag_days):
            past_date = target_date - pd.Timedelta(days=lag_days)
            row = df[df['ds'] == past_date]
            if not row.empty:
                return row.iloc[0]['y']
            else:
                return 0 # Fallback

        future_df['lag_7'] = future_df['ds'].apply(lambda x: get_lag_value(x, 7))
        future_df['lag_364'] = future_df['ds'].apply(lambda x: get_lag_value(x, 364))

        # C. 业务特征
        match_month = future_df['ds'].dt.month.isin([1, 2, 9, 10])
        match_day = future_df['ds'].dt.dayofweek.isin([1, 2])
        future_df['is_off_peak_workday'] = (match_month & match_day).astype(int)

        # D. 外部特征 (Real Holiday Logic)
        print("   Generating Future Holiday Features (Unified Utils)...")
        from src.utils.holiday_utils import get_holiday_features, TARGET_HOLIDAYS, get_us_holidays
        
        # 1. Generate Flags (is_holiday, etc)
        h_feats = get_holiday_features(future_df['ds'])
        for c in h_feats.columns:
            future_df[c] = h_feats[c].values
            
        # 2. Generate Distance (days_to_nearest_holiday)
        # Using unified TARGET_HOLIDAYS
        us_holidays = get_us_holidays(2025, 2030) # Future range
        major_holiday_dates = []
        for date, name in us_holidays.items():
            if any(target in name for target in TARGET_HOLIDAYS):
                major_holiday_dates.append(pd.Timestamp(date))
                
        from dateutil.easter import easter
        unique_years = future_df['ds'].dt.year.unique()
        for y in unique_years:
            try:
                easter_date = easter(y)
                good_friday = pd.Timestamp(easter_date) - pd.Timedelta(days=2)
                major_holiday_dates.append(good_friday)
            except: pass
            
        major_holiday_dates = sorted(list(set(major_holiday_dates)))
        
        future_df['days_to_nearest_holiday'] = 15 
        for idx, row in future_df.iterrows():
            d = row['ds']
            min_dist = 999 
            best_dist = 15 
            for h_date in major_holiday_dates:
                diff_days = (d - h_date).days
                if abs(diff_days) < abs(min_dist):
                    min_dist = diff_days
                    best_dist = diff_days
            
            if best_dist > 14: best_dist = 15
            elif best_dist < -14: best_dist = -15
            future_df.at[idx, 'days_to_nearest_holiday'] = best_dist
            
        # [NEW] Long Weekend Logic (Vectorized)
        future_df['is_long_weekend'] = 0
        mask_long = (future_df['is_holiday'] == 1) & (future_df['ds'].dt.dayofweek.isin([0, 4]))
        future_df.loc[mask_long, 'is_long_weekend'] = 1

        # [FIX] Load Real Weather Forecast
        print("   Merging Real Weather Forecast (from DB)...")
        try:
            conn_w = sqlite3.connect(DB_PATH)
            df_weather = pd.read_sql("SELECT date, weather_index FROM daily_weather_index", conn_w)
            conn_w.close()
            
            df_weather['date'] = pd.to_datetime(df_weather['date'])
            future_df = future_df.merge(df_weather[['date', 'weather_index']], left_on='ds', right_on='date', how='left')
            future_df['weather_index'] = future_df['weather_index'].fillna(0).astype(int)
            if 'date' in future_df.columns:
                future_df.drop(columns=['date'], inplace=True)
        except Exception as e:
            print(f"   WARNING: Failed to load weather features ({e}). Defaulting to 0.")
            future_df['weather_index'] = 0

        future_df['weather_lag_1'] = future_df['weather_index'].shift(1).fillna(0)
        # Patch first row weather lag
        try:
            if len(df) > 0:
                future_df.at[0, 'weather_lag_1'] = df.iloc[-1]['weather_index']
        except: pass

        # [NEW] Flight Stats Integration
        print("   Merging OpenSky Flight Stats...")
        # Since future flights are unknown, we forward fill last known
        last_known_ma7 = df.iloc[-1]['flight_ma_7'] if 'flight_ma_7' in df.columns else 0
        last_known_lag1 = df.iloc[-1]['total_flights'] if 'total_flights' in df.columns else 0

        future_df['flight_ma_7'] = last_known_ma7
        future_df['flight_lag_1'] = last_known_lag1
        
        # [NEW] Calculate Future Revenge Index
        # Access daily_weather_index for lags
        try:
            conn_rev = sqlite3.connect(DB_PATH)
            # Pre-fetch weather history dict for fast lookup
            w_history_df = pd.read_sql("SELECT date, index_value as weather_index FROM daily_weather_index", conn_rev)
            conn_rev.close()
            w_history_df['date'] = pd.to_datetime(w_history_df['date'])
            w_map = w_history_df.set_index('date')['weather_index'].to_dict()
            
            def get_w(d): return w_map.get(d, 0)
            
            # vectorized apply is dangerous with dict lookup if huge, but here fine
            future_df['w_lag_1'] = future_df['ds'].apply(lambda d: get_w(d - pd.Timedelta(days=1)))
            future_df['w_lag_2'] = future_df['ds'].apply(lambda d: get_w(d - pd.Timedelta(days=2)))
            future_df['w_lag_3'] = future_df['ds'].apply(lambda d: get_w(d - pd.Timedelta(days=3)))
            
            future_df['revenge_index'] = (future_df['w_lag_1'] * 0.5) + \
                                         (future_df['w_lag_2'] * 0.3) + \
                                         (future_df['w_lag_3'] * 0.2)
        except Exception as e:
             print(f"Failed to calc future revenge index: {e}")
             future_df['revenge_index'] = 0

        # Real Spring Break
        future_df['is_spring_break'] = 0
        mask_sb = (future_df['ds'].dt.month.isin([3, 4])) & \
                  (future_df['ds'].dt.dayofweek.isin([5, 6])) & \
                  (future_df['is_holiday'] == 0)
        future_df.loc[mask_sb, 'is_spring_break'] = 1

        # E. 预测
        X_future = future_df[features]
        # Ensure all columns exist
        for col in features:
             if col not in X_future.columns: X_future[col] = 0

        y_future_pred = model_full.predict(X_future)
        future_df['predicted_throughput'] = y_future_pred.astype(int)

        # [NEW] Weather Circuit Breaker for Long-Term Forecast
        # Logic matches Sniper: >35 (-30%), >20 (-15%)
        print("   [POST-PROCESS] Applying Weather Circuit Breaker...")
        
        def apply_circuit_breaker(row):
            val = row['predicted_throughput']
            w_idx = row.get('weather_index', 0)
            
            if w_idx >= 35:
                return int(val * 0.70)
            elif w_idx >= 20:
                return int(val * 0.85)
            else:
                return val

        future_df['predicted_throughput'] = future_df.apply(apply_circuit_breaker, axis=1)

        # 保存预测结果
        future_df[['ds', 'predicted_throughput']].to_csv("xgb_forecast.csv", index=False)

        print("\n[FORECAST RESULTS] Future Forecast:")
        print(future_df[['ds', 'predicted_throughput']].to_string(index=False))

        # [NEW] Save to Persistent History Log (SQLite)
        today_str = pd.Timestamp.now().strftime('%Y-%m-%d')
        
        new_log = future_df[['ds', 'predicted_throughput', 'weather_index', 'is_holiday', 'flight_lag_1', 'is_weekend', 'holiday_name']].copy()
        new_log.columns = ['target_date', 'predicted_throughput', 'weather_index', 'is_holiday', 'flight_volume', 'is_weekend', 'holiday_name']
        new_log['model_run_date'] = today_str
        new_log['target_date'] = new_log['target_date'].dt.strftime('%Y-%m-%d')

        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            
            # Delete dupes for same run date
            dt_list = new_log['target_date'].tolist()
            cursor.executemany("DELETE FROM prediction_history WHERE target_date = ? AND model_run_date = ?", 
                               [(d, today_str) for d in dt_list])
            
            records = []
            for _, row in new_log.iterrows():
                records.append({
                    'target_date': str(row['target_date']),
                    'predicted_throughput': int(row['predicted_throughput']),
                    'model_run_date': str(row['model_run_date']),
                    'weather_index': int(row.get('weather_index', 0)),
                    'is_holiday': int(row.get('is_holiday', 0)),
                    'flight_volume': int(row.get('flight_volume', 0)), 
                    'is_weekend': int(row.get('is_weekend', 0))
                })
            
            cursor.executemany('''
                INSERT INTO prediction_history (
                    target_date, predicted_throughput, model_run_date, 
                    weather_index, is_holiday, flight_volume, is_weekend
                )
                VALUES (
                    :target_date, :predicted_throughput, :model_run_date,
                    :weather_index, :is_holiday, :flight_volume, :is_weekend
                )
            ''', records)
            
            conn.commit()
            conn.close()
            print(f"Forecast logged to {DB_PATH} for verification.")
            
        except Exception as e:
            print(f"ERROR logging to database: {e}")

    except Exception as e:
        print(f"   [CRITICAL ERROR] Forecast Generation Failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run()
