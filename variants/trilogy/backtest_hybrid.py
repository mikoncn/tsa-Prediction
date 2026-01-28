import pandas as pd
import numpy as np
import sqlite3
import sys
import os
from xgboost import XGBRegressor

# Add src to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.config import DB_PATH
from src.models.model_utils import get_aggregated_weather_features

def run_backtest():
    print("🚀 Starting Rolling Backtest (2023, 2024, 2025, 2026)...")
    
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM traffic_full ORDER BY date", conn)
    
    # Load Weather for Shadow Model
    df_weather = pd.read_sql("SELECT date, weather_index FROM daily_weather_index", conn)
    conn.close()
    
    df['ds'] = pd.to_datetime(df['date'])
    df['y'] = df['throughput']
    df_weather['date'] = pd.to_datetime(df_weather['date'])
    
    # Merge Weather if needed (though traffic_full usually has it)
    if 'weather_index' not in df.columns:
        df = df.merge(df_weather, left_on='ds', right_on='date', how='left')
        df['weather_index'] = df['weather_index'].fillna(0)
    
    # ---------------------------------------------------------
    # 1. Feature Engineering (Same as train_xgb.py)
    # ---------------------------------------------------------
    
    # Shadow Model Injection
    print("   [Shadow Model] Injecting Cancellation Rates...")
    try:
        import pickle
        # [FIX] Point to src/models/shadow_weather_model.pkl
        shadow_model_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'src', 'models', 'shadow_weather_model.pkl')
        if os.path.exists(shadow_model_path):
            with open(shadow_model_path, 'rb') as f:
                shadow_model = pickle.load(f)
            
            conn_shad = sqlite3.connect(DB_PATH)
            df_w_agg = get_aggregated_weather_features(conn_shad)
            conn_shad.close()
            
            shadow_features = [
                'max_snow', 'mean_snow', 
                'max_snow_sq', 'mean_snow_sq', # <--- [NEW]
                'max_wind', 'mean_wind', 
                'max_precip', 'mean_precip', 'min_temp', 'mean_temp', 
                'national_severity', 'month', 'day_of_year'
            ]
            
            X_shad = df_w_agg[shadow_features].fillna(0)
            df_w_agg['predicted_cancel_rate'] = shadow_model.predict(X_shad)
            
            df = df.merge(df_w_agg[['date', 'predicted_cancel_rate']], left_on='ds', right_on='date', how='left')
            df['predicted_cancel_rate'] = df['predicted_cancel_rate'].fillna(0)
        else:
             df['predicted_cancel_rate'] = 0
    except Exception as e:
        print(f"Shadow model error: {e}")
        df['predicted_cancel_rate'] = 0

    # Basic Features
    df['day_of_week'] = df['ds'].dt.dayofweek
    df['month'] = df['ds'].dt.month
    df['year'] = df['ds'].dt.year
    df['day_of_year'] = df['ds'].dt.dayofyear
    df['week_of_year'] = df['ds'].dt.isocalendar().week.astype(int)
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    
    # Lags
    df['y_lag_7'] = df['y'].shift(7).fillna(method='bfill')
    df['lag_7'] = df['y_lag_7']
    
    # Lag 364/365
    df['lag_365'] = df['y'].shift(365).fillna(method='bfill')
    df['lag_364'] = df['y'].shift(364).fillna(method='bfill')
    
    fixed_date_holidays = ["New Year's Day", "Independence Day", "Veterans Day", "Christmas Day"]
    df['is_fixed_holiday'] = 0
    mask_fixed = df['ds'].apply(lambda d: (d.month==1 and d.day==1) or (d.month==7 and d.day==4) or (d.month==11 and d.day==11) or (d.month==12 and d.day==25))
    df.loc[mask_fixed, 'is_fixed_holiday'] = 1
    df['lag_364'] = np.where(df['is_fixed_holiday'] == 1, df['lag_365'], df['lag_364'])
    
    # Revenge Index
    df['w_lag_1'] = df['weather_index'].shift(1).fillna(0)
    df['w_lag_2'] = df['weather_index'].shift(2).fillna(0)
    df['w_lag_3'] = df['weather_index'].shift(3).fillna(0)
    df['revenge_index'] = (df['w_lag_1'] * 0.5) + (df['w_lag_2'] * 0.3) + (df['w_lag_3'] * 0.2)
    
    # Long Weekend
    if 'is_holiday' not in df.columns: df['is_holiday'] = 0
    df['is_long_weekend'] = 0
    mask_long = (df['is_holiday'] == 1) & (df['day_of_week'].isin([0, 4]))
    df.loc[mask_long, 'is_long_weekend'] = 1
    
    # Business Logic
    match_month = df['ds'].dt.month.isin([1, 2, 9, 10])
    match_day = df['ds'].dt.dayofweek.isin([1, 2])
    df['is_off_peak_workday'] = (match_month & match_day).astype(int)

    # [NEW] Multiplicative Interaction Features
    df['lag_7_adjusted'] = df['lag_7'] * (1 - df['predicted_cancel_rate'])
    df['lag_364_adjusted'] = df['lag_364'] * (1 - df['predicted_cancel_rate'])
    
    # [NEW] Fear Feature (Look-Ahead - Anticipation)
    # If tomorrow is a meltdown, airlines pre-cancel today.
    df['lead_1_shadow_cancel_rate'] = df['predicted_cancel_rate'].shift(-1).fillna(0)

    # Clean
    features = [
        'day_of_week', 'month', 'year', 'day_of_year', 'week_of_year', 'is_weekend',
        'weather_index', 'is_holiday',  'is_off_peak_workday', # 'is_spring_break',
        # 'is_holiday_exact_day', 'days_to_nearest_holiday', # Simplify for backtest if cols missing
        'predicted_cancel_rate', 
        'revenge_index', 'is_long_weekend',
        'lag_7', 'lag_364',
        'lag_7_adjusted', 'lag_364_adjusted',
        'lead_1_shadow_cancel_rate' # <--- [NEW] Fear Feature
    ]
    
    # Add optional cols if exist
    for c in ['is_spring_break', 'days_to_nearest_holiday']:
        if c in df.columns: features.append(c)
        else: df[c] = 0; features.append(c) # Dummy

    df = df.dropna(subset=['y', 'lag_364'])
    
    # ---------------------------------------------------------
    # 2. Rolling Backtest Loop
    # ---------------------------------------------------------
    years_to_test = [2023, 2024, 2025, 2026]
    results = []
    
    for test_year in years_to_test:
        print(f"\nEvaluating Year: {test_year}...")
        
        # Train: All data BEFORE this year
        # Test: This year
        train_mask = (df['ds'].dt.year < test_year) & (df['ds'].dt.year >= 2019)
        test_mask = (df['ds'].dt.year == test_year)
        
        if test_year == 2026:
            # For 2026, test only what we have (Jan)
            pass
            
        train_df = df[train_mask]
        test_df = df[test_mask]
        
        if train_df.empty: 
            print(f"   Skipping {test_year} (No training data)")
            continue
        if test_df.empty:
            print(f"   Skipping {test_year} (No test data yet)")
            continue
            
        X_train = train_df[features]
        y_train = train_df['y']
        X_test = test_df[features]
        y_test = test_df['y']
        
        model = XGBRegressor(n_estimators=1000, learning_rate=0.05, max_depth=5, n_jobs=-1, random_state=42)
        model.fit(X_train, y_train)
        
        y_pred = model.predict(X_test)
        
        # [NEW] Blind Flight Protocol (Hybrid Tuning + Hangover Rule + Fear Rule)
        def apply_blind_protocol_vec(val, w_idx, w_lag, lead_1):
            multiplier = 1.0
            
            # 1. Blind Protocol (Today - Meltdown)
            if w_idx >= 30: multiplier = 0.75
            elif w_idx >= 20: multiplier = 0.85
            elif w_idx >= 15: multiplier = 0.95
            
            # 2. Hangover Rule (Yesterday - Recovery)
            if w_lag >= 30:
                multiplier *= 0.90
                
            # 3. Fear Rule (Tomorrow - Anticipation)
            # If tomorrow's Shadow Forecast is > 20% (Disaster imminent), reduce today by 10%
            if lead_1 > 0.20:
                 multiplier *= 0.90
                
            return int(val * multiplier)
            
        if 'weather_index' in X_test.columns:
            w_indices = X_test['weather_index'].values
            
            if 'w_lag_1' in test_df.columns:
                w_lags = test_df['w_lag_1'].values
            else:
                w_lags = np.zeros(len(w_indices))
                
            # [FIX] Pull lead_1 from test_df
            if 'lead_1_shadow_cancel_rate' in test_df.columns:
                leads = test_df['lead_1_shadow_cancel_rate'].values
            else:
                leads = np.zeros(len(w_indices))
                print("   [WARNING] lead_1 missing (Anticipation Rule skipped)")
                
            y_pred_adjusted = []
            for p, w, lag, lead in zip(y_pred, w_indices, w_lags, leads):
                y_pred_adjusted.append(apply_blind_protocol_vec(p, w, lag, lead))
            y_pred = np.array(y_pred_adjusted)
        
        # Calculate Error
        test_df = test_df.copy()
        test_df['predicted'] = y_pred
        test_df['diff'] = test_df['predicted'] - test_df['y']
        test_df['abs_diff'] = test_df['diff'].abs()
        test_df['error_pct'] = (test_df['abs_diff'] / test_df['y']) * 100
        
        mape = test_df['error_pct'].mean()
        mae = test_df['abs_diff'].mean()
        
        print(f"   Shape: Train={len(train_df)}, Test={len(test_df)}")
        print(f"   MAPE: {mape:.2f}%")
        print(f"   MAE:  {int(mae):,}")
        
        results.append({
            'year': test_year,
            'mape': mape,
            'mae': mae,
            'count': len(test_df)
        })
        
        # Highlight Specific Rows
        print(f"   >>> Extreme Error Days (>10%):")
        bad_days = test_df[test_df['error_pct'] > 10].sort_values('error_pct', ascending=False).head(5)
        for _, row in bad_days.iterrows():
            print(f"       {row['ds'].date()}: Actual={int(row['y']):,} vs Pred={int(row['predicted']):,} (Err: {row['error_pct']:.1f}%) [Wx:{row.get('weather_index',0)}, CancelRate:{row.get('predicted_cancel_rate',0):.2f}]")

        # Save CSV
        fname = f"backtest_{test_year}.csv"
        test_df[['ds', 'y', 'predicted', 'error_pct', 'features']].to_csv(fname, index=False) if 'features' in test_df else test_df[['ds', 'y', 'predicted', 'error_pct']].to_csv(fname, index=False)

    print("\n=== Summary ===")
    print(pd.DataFrame(results))

if __name__ == "__main__":
    run_backtest()
