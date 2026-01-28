import pandas as pd
import numpy as np
import sqlite3
import os
import sys
from xgboost import XGBRegressor

sys.path.append(os.getcwd())
from src.config import DB_PATH

def run_comparison():
    print("Starting Model Comparison...")
    
    # 1. Load Data
    conn = sqlite3.connect(DB_PATH)
    try:
        print("  Loading data from traffic_full...")
        df = pd.read_sql("SELECT * FROM traffic_full", conn)
    except Exception as e:
        print(f"  ERROR loading data: {e}")
        return
    finally:
        conn.close()

    print(f"  Data loaded. Rows: {len(df)}")
    if len(df) == 0:
        print("  ERROR: No data found.")
        return

    # 2. Preprocessing
    print("  Preprocessing dates...")
    df['ds'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['ds']).sort_values('ds')
    df['y'] = df['throughput']
    
    # 3. Features
    print("  Generating Common Features...")
    df['day_of_week'] = df['ds'].dt.dayofweek
    df['month'] = df['ds'].dt.month
    df['year'] = df['ds'].dt.year
    df['day_of_year'] = df['ds'].dt.dayofyear
    df['week_of_year'] = df['ds'].dt.isocalendar().week.astype(int)
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    
    # Lags (Standard)
    # Reverting Multi-Year Trend (Experiment Failed: 4.65% error vs 3.42% baseline)
    # Keeping strict lag_364 is better during growth periods.
    df['lag_7'] = df['y'].shift(7).fillna(method='bfill')
    df['lag_364'] = df['y'].shift(364).fillna(method='bfill')
    
    # Holiday Features (Advanced)
    print("  Generating Advanced Holiday Features...")
    try:
        from src.utils.holiday_utils import get_holiday_features, TARGET_HOLIDAYS, get_us_holidays
        from dateutil.easter import easter
        
        # 1. Base Flags
        ds_series = df['ds'] if isinstance(df['ds'], pd.Series) else pd.Series(df['ds'])
        h_feats = get_holiday_features(ds_series)
        for c in h_feats.columns:
            df[c] = h_feats[c].values
            
        # 2. Distance to Holiday (Critical for Ramp-up)
        us_holidays = get_us_holidays(2019, 2030)
        major_holiday_dates = []
        for date, name in us_holidays.items():
            if any(target in name for target in TARGET_HOLIDAYS):
                major_holiday_dates.append(pd.Timestamp(date))
        
        # Add Good Friday
        for y in df['ds'].dt.year.unique():
            try:
                ed = pd.Timestamp(easter(y))
                major_holiday_dates.append(ed - pd.Timedelta(days=2))
            except: pass
        major_holiday_dates = sorted(list(set(major_holiday_dates)))
        
        def get_dist(d):
            best_dist = 15
            d_ts = pd.Timestamp(d)
            min_diff = 999
            for h in major_holiday_dates:
                diff = (d_ts - h).days
                if abs(diff) < abs(min_diff):
                    min_diff = diff
            if min_diff > 14: return 15
            if min_diff < -14: return -15
            return min_diff

        df['days_to_nearest_holiday'] = df['ds'].apply(get_dist)

        # 3. Off-Peak Workday (Tue/Wed in Jan/Feb/Sep/Oct)
        match_month = df['ds'].dt.month.isin([1, 2, 9, 10])
        match_day = df['ds'].dt.dayofweek.isin([1, 2]) # Tue, Wed
        df['is_off_peak_workday'] = (match_month & match_day).astype(int)

        # 4. Long Weekend
        df['is_long_weekend'] = 0
        mask_long = (df['is_holiday'] == 1) & (df['day_of_week'].isin([0, 4]))
        df.loc[mask_long, 'is_long_weekend'] = 1
        
        # 5. Spring Break (already in get_holiday_features usually, but ensure)
        if 'is_spring_break' not in df.columns:
             mask_sb = (df['ds'].dt.month.isin([3, 4])) & \
                       (df['ds'].dt.dayofweek.isin([5, 6])) & \
                       (df['is_holiday'] == 0)
             df['is_spring_break'] = mask_sb.astype(int)

        print("  Advanced features generated.")
        
    except Exception as e:
        print(f"  ERROR generating advanced holiday features: {e}")
        import traceback
        traceback.print_exc()
        df['is_holiday'] = 0
        df['days_to_nearest_holiday'] = 15
        df['is_off_peak_workday'] = 0
        df['is_long_weekend'] = 0
        df['is_spring_break'] = 0

    # 4. Model Setup
    # --- Classic Model Setup ---
    # Features: STRICTLY NAIVE + BUSINESS LOGIC + WEATHER (User Confirmed)
    # Flight Data REMOVED.
    feat_classic = [
        'day_of_week', 'month', 'year', 'day_of_year', 'week_of_year', 'is_weekend',
        'is_holiday', 'is_holiday_exact_day', 'is_holiday_travel_window', 
        'lag_7', 'lag_364', 'weather_index',
        'is_off_peak_workday', 'days_to_nearest_holiday', 'is_long_weekend', 'is_spring_break'
    ]
    
    # New Model features
    if 'weather_index' not in df.columns: df['weather_index'] = 0
    df['weather_index'] = df['weather_index'].fillna(0)
    
    # Revenge Index
    df['w_lag_1'] = df['weather_index'].shift(1).fillna(0)
    df['w_lag_2'] = df['weather_index'].shift(2).fillna(0)
    df['w_lag_3'] = df['weather_index'].shift(3).fillna(0)
    df['revenge_index'] = (df['w_lag_1'] * 0.5) + (df['w_lag_2'] * 0.3) + (df['w_lag_3'] * 0.2)
    
    # Shadow Model Injection
    print("  Injecting Shadow Model...")
    df['predicted_cancel_rate'] = 0.0 # Default
    try:
        import pickle
        from src.models.model_utils import get_aggregated_weather_features
        conn_shadow = sqlite3.connect(DB_PATH)
        df_weather_agg = get_aggregated_weather_features(conn_shadow)
        conn_shadow.close()
        
        shadow_path = os.path.join('src', 'models', 'shadow_weather_model.pkl')
        if os.path.exists(shadow_path):
            with open(shadow_path, 'rb') as f:
                shadow_model = pickle.load(f)
            
            shadow_features = [
                'max_snow', 'mean_snow', 'max_snow_sq', 'mean_snow_sq',
                'max_wind', 'mean_wind', 'max_precip', 'mean_precip', 
                'min_temp', 'mean_temp', 'national_severity', 'month', 'day_of_year'
            ]
            X_shadow = df_weather_agg[shadow_features].fillna(0)
            df_weather_agg['predicted_cancel_rate'] = shadow_model.predict(X_shadow)
            
            # Merge
            df['ds_date'] = df['ds'].dt.floor('D')
            df_weather_agg['date'] = pd.to_datetime(df_weather_agg['date'])
            
            df = df.merge(df_weather_agg[['date', 'predicted_cancel_rate']], 
                         left_on='ds_date', right_on='date', how='left')
            # Fix merge suffixes if any
            if 'predicted_cancel_rate_y' in df.columns:
                df['predicted_cancel_rate'] = df['predicted_cancel_rate_y'].fillna(0)
            else:
                df['predicted_cancel_rate'] = df['predicted_cancel_rate'].fillna(0)
                
            print(f"  Shadow model applied. Max Rate: {df['predicted_cancel_rate'].max():.4f}")
    except Exception as e:
        print(f"  WARNING: Shadow model injection failed: {e}")

    # Soft Logic
    df['lag_7_adjusted'] = df['lag_7'] * (1 - df['predicted_cancel_rate'])
    
    # New Features Set (weather_index and is_holiday are in feat_classic)
    feat_new = feat_classic + ['revenge_index', 
                              'predicted_cancel_rate', 'lag_7_adjusted']

    # 5. Backtest
    print("  Starting Backtest Loop...")
    train_mask = df['ds'].dt.year < 2025
    test_mask = df['ds'].dt.year >= 2025
    
    df_train = df[train_mask].dropna(subset=['lag_364'])
    df_test = df[test_mask].dropna(subset=['lag_364'])
    
    print(f"  Train: {len(df_train)}, Test: {len(df_test)}")
    
    # Train Classic
    print("  Training Classic Model...")
    model_c = XGBRegressor(n_estimators=1000, learning_rate=0.05, max_depth=5, n_jobs=-1, random_state=42)
    model_c.fit(df_train[feat_classic], df_train['y'])
    pred_c = model_c.predict(df_test[feat_classic])
    
    # Train New
    print("  Training New Model...")
    model_n = XGBRegressor(n_estimators=1000, learning_rate=0.05, max_depth=5, n_jobs=-1, random_state=42)
    model_n.fit(df_train[feat_new], df_train['y'])
    pred_n_raw = model_n.predict(df_test[feat_new])
    
    # Circuit Breakers
    final_pred_n = []
    print("  Applying Circuit Breakers...")
    # Need efficient access
    df_test_reset = df_test.reset_index(drop=True)
    for i, p in enumerate(pred_n_raw):
        row = df_test_reset.iloc[i]
        w_idx = row['weather_index']
        w_lag_1 = row['w_lag_1']
        
        multiplier = 1.0
        if w_idx >= 30: multiplier = 0.75
        elif w_idx >= 20: multiplier = 0.85
        elif w_idx >= 15: multiplier = 0.95
        
        if w_lag_1 >= 30: multiplier *= 0.90
        
        # Look ahead
        if i < len(pred_n_raw) - 1:
            lead_1 = df_test_reset.iloc[i+1]['predicted_cancel_rate']
            if lead_1 > 0.20: multiplier *= 0.90
            
        final_pred_n.append(p * multiplier)
        
    df_test['pred_classic'] = pred_c
    df_test['pred_new'] = final_pred_n
    
    # 6. Reporting
    print("  Reporting Results...")
    df_test['err_classic'] = (abs(df_test['pred_classic'] - df_test['y']) / df_test['y']) * 100
    df_test['err_new'] = (abs(df_test['pred_new'] - df_test['y']) / df_test['y']) * 100
    
    # Strict Normal Def
    df_test['day_type'] = 'Normal'
    df_test.loc[df_test['weather_index'] >= 15, 'day_type'] = 'Extreme Weather'
    df_test.loc[df_test['is_holiday'] == 1, 'day_type'] = 'Holiday'
    
    res_table = df_test[['ds', 'day_type', 'day_of_week', 'weather_index', 'y', 
                         'pred_classic', 'err_classic', 
                         'pred_new', 'err_new']].copy()
    
    res_table['improvement'] = res_table['err_classic'] - res_table['err_new']
    
    output_csv = "comparison_detail_2026.csv"
    res_table.to_csv(output_csv, index=False)
    print(f"  Saved {output_csv}")
    
    print("\n=== SUMMARY METRICS ===")
    groups = res_table.groupby('day_type')
    print(groups[['err_classic', 'err_new']].mean())
    
    print("\n=== TOP 10 BAD NORMAL DAYS (OLD MODEL) ===")
    bad_old = res_table[(res_table['day_type'] == 'Normal') & (res_table['err_classic'] > 3.0)].sort_values('err_classic', ascending=False)
    if not bad_old.empty:
        print(bad_old.head(10).to_string(index=False))
    
    print("\nComparison Run Completed.")

if __name__ == "__main__":
    run_comparison()



if __name__ == "__main__":
    run_comparison()
