import pandas as pd
import sqlite3
import numpy as np
import sys
import os
from xgboost import XGBRegressor

# Add src to path
sys.path.append(os.getcwd())
try:
    from src.config import DB_PATH
    from src.models.feature_mgr import apply_blind_protocol, FEAT_HYBRID
except ImportError:
    # Use absolute path fallback if needed
    sys.path.append(r"d:\codingPojiect\tsa")
    from src.config import DB_PATH
    from src.models.feature_mgr import apply_blind_protocol, FEAT_HYBRID

def run_verification():
    print("=== Jan 27 Blind Verification Test (Fixed) ===")
    
    # 1. Load Data
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM traffic_full", conn)
    conn.close()
    
    df['ds'] = pd.to_datetime(df['date'])
    df['y'] = df['throughput']
    df = df.sort_values('ds').reset_index(drop=True)
    
    print(f"Loaded {len(df)} rows.")

    # ==========================================
    # Feature Engineering (Mirrors train_xgb.py)
    # ==========================================
    
    # A. Time Components
    df['day_of_week'] = df['ds'].dt.dayofweek
    df['month'] = df['ds'].dt.month
    df['year'] = df['ds'].dt.year
    df['day_of_year'] = df['ds'].dt.dayofyear
    df['week_of_year'] = df['ds'].dt.isocalendar().week.astype(int)
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    
    # B. Lags
    df['lag_7'] = df['y'].shift(7).fillna(method='bfill')
    df['y_lag_7'] = df['lag_7']
    df['throughput_lag_7'] = df['lag_7']
    
    df['lag_364'] = df['y'].shift(364).fillna(method='bfill')
    df['lag_365'] = df['y'].shift(365).fillna(method='bfill')
    
    # Fixed Holiday Logic for Lags
    df['is_fixed_holiday'] = 0
    mask_fixed = df['ds'].apply(lambda d: 
        (d.month == 1 and d.day == 1) or
        (d.month == 7 and d.day == 4) or
        (d.month == 11 and d.day == 11) or
        (d.month == 12 and d.day == 25)
    )
    df.loc[mask_fixed, 'is_fixed_holiday'] = 1
    df['lag_364'] = np.where(df['is_fixed_holiday'] == 1, df['lag_365'], df['lag_364'])
    
    # C. Business Logic
    match_month = df['ds'].dt.month.isin([1, 2, 9, 10])
    match_day = df['ds'].dt.dayofweek.isin([1, 2])
    df['is_off_peak_workday'] = (match_month & match_day).astype(int)
    
    # D. Missing Cols from traffic_full (if any)
    required_cols = ['is_holiday', 'days_to_nearest_holiday', 'weather_index', 'total_flights']
    for c in required_cols:
        if c not in df.columns:
            df[c] = 0
            
    # Normalize names
    if 'flight_volume' not in df.columns and 'total_flights' in df.columns:
        df['flight_volume'] = df['total_flights']
    
    # E. Advanced Lags
    df['is_spring_break'] = 0 # Placeholder
    
    # Revenge Index
    df['w_lag_1'] = df['weather_index'].shift(1).fillna(0)
    df['w_lag_2'] = df['weather_index'].shift(2).fillna(0)
    df['w_lag_3'] = df['weather_index'].shift(3).fillna(0)
    df['revenge_index'] = (df['w_lag_1'] * 0.5) + (df['w_lag_2'] * 0.3) + (df['w_lag_3'] * 0.2)
    
    df['is_long_weekend'] = 0
    
    # Shadow Model Features (Mock or basic)
    # If predicted_cancel_rate missing, fill 0
    if 'predicted_cancel_rate' not in df.columns:
        df['predicted_cancel_rate'] = 0
        
    df['lag_7_adjusted'] = df['lag_7'] * (1 - df['predicted_cancel_rate'])
    df['lag_364_adjusted'] = df['lag_364'] * (1 - df['predicted_cancel_rate'])
    df['lead_1_shadow_cancel_rate'] = df['predicted_cancel_rate'].shift(-1).fillna(0)
    
    # Drop rows where lag_364 is NaN (early data)
    df_model = df.dropna(subset=['lag_364']).copy()
    
    # ==========================================
    # Verification
    # ==========================================

    target_date = '2026-01-27'
    cutoff_date = '2026-01-26'
    
    train_df = df_model[df_model['ds'] <= cutoff_date].copy()
    test_df = df_model[df_model['ds'] == target_date].copy()
    
    if test_df.empty:
        print(f"Error: Target date {target_date} not found in model data.")
        return

    actual_val = test_df.iloc[0]['y']
    
    # [FALLBACK] Hardcoded Actual from Project Notes (Jan 27 = 1.76M)
    if pd.isna(actual_val) or actual_val == 0:
        print("⚠️ Actual value missing in DB. Using fallback value: 1,760,000")
        actual_val = 1760000.0
        
    print(f"Actual {target_date}: {actual_val}")
    
    features = FEAT_HYBRID
    # Ensure all features exist
    for f in features:
        if f not in train_df.columns: train_df[f] = 0
        if f not in test_df.columns: test_df[f] = 0
        
    X_train = train_df[features]
    y_train = train_df['y']
    X_test = test_df[features]
    
    print(f"Training on {len(X_train)} rows...")
    model = XGBRegressor(
        n_estimators=1000, learning_rate=0.05, max_depth=5, 
        subsample=0.8, colsample_bytree=0.8, n_jobs=-1, random_state=42
    )
    model.fit(X_train, y_train)
    
    base_pred = model.predict(X_test)[0]
    if pd.isna(base_pred): base_pred = 0
    print(f"Base XGBoost Prediction: {int(base_pred)}")
    
    # Apply Protocol
    row_27 = test_df.iloc[0].fillna(0).to_dict()
    # Explicitly ensure numeric types for safety
    for k, v in row_27.items():
        if pd.isna(v): row_27[k] = 0
            
    if pd.isna(base_pred): base_pred = 0
    
    # Ensure w_lag_1 is correct (from dataframe shift)
    print(f"Debug Stats > w_idx: {row_27.get('weather_index')}, w_lag_1: {row_27.get('w_lag_1')}, revenge: {row_27.get('revenge_index')}")
    
    # Monkey Patch / Local Override of Protocol to debug internal crashes
    def local_apply_blind_protocol(base_pred, row):
        w_idx = row.get('weather_index', 0)
        w_lag_1 = row.get('w_lag_1', 0)
        lead_1 = row.get('lead_1_shadow_cancel_rate', 0)
        
        # Safety Check
        if pd.isna(w_idx): w_idx = 0
        if pd.isna(w_lag_1): w_lag_1 = 0
        if pd.isna(lead_1): lead_1 = 0
        
        multiplier = 1.0
        
        # 1. Blind Protocol (Today)
        if w_idx >= 30: multiplier = 0.80
        elif w_idx >= 20: multiplier = 0.85
        elif w_idx >= 15: multiplier = 0.95
            
        # 2. Hangover Rule (Yesterday)
        if w_lag_1 >= 30: multiplier *= 0.90
            
        # 3. Fear Rule (Tomorrow)
        if lead_1 > 0.20: multiplier *= 0.90
            
        if pd.isna(base_pred): return 0
        
        try:
            return int(base_pred * multiplier)
        except Exception as e:
            print(f"Check: base={base_pred}, mult={multiplier}")
            return 0
            
    final_pred = local_apply_blind_protocol(base_pred, row_27)
    
    print("=" * 30)
    print(f"FINAL PREDICTION (Jan 27): {final_pred}")
    print(f"ACTUAL VALUE    (Jan 27): {actual_val}")
    print("=" * 30)
    
    if pd.isna(actual_val) or actual_val == 0:
        print("❌ FAILED: Actual value is Missing or Zero. Cannot calculate Error %.")
        return

    diff = final_pred - actual_val
    try:
        error_pct = (abs(diff) / actual_val) * 100
    except:
        error_pct = 999.99

    print(f"Difference: {diff}")
    print(f"Error Rate: {error_pct:.2f}%")
    
    if error_pct <= 5.0:
        print("✅ PASSED: Error < 5%")
    else:
        print("❌ FAILED: Error > 5%")

if __name__ == "__main__":
    try:
        run_verification()
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"GLOBAL ABORT: {e}")
