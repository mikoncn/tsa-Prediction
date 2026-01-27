import sqlite3
import pandas as pd
import numpy as np
import os
import sys
import pickle
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score

# Add src to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.config import DB_PATH

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def load_and_prep_data():
    print("Loading data from DB...")
    conn = get_db_connection()
    
    # 1. Load Weather Data
    print("   Reading weather table...")
    df_weather = pd.read_sql("SELECT * FROM weather", conn)
    
    # 2. Load BTS Traffic (Ground Truth)
    print("   Reading bts_traffic table...")
    df_bts = pd.read_sql("SELECT date, cancel_rate FROM bts_traffic", conn)
    
    conn.close()
    
    # 3. Aggregate Weather (Hubs -> National)
    print("   Aggregating weather metrics...")
    # metrics: snowfall_cm, windspeed_kmh, precipitation_mm, temperature_min_c, severity_score
    
    df_weather_agg = df_weather.groupby('date').agg({
        'snowfall_cm': ['max', 'mean'],
        'windspeed_kmh': ['max', 'mean'],
        'precipitation_mm': ['max', 'mean'],
        'temperature_min_c': ['min', 'mean'], # Min temp is critical for cold snaps
        'severity_score': 'sum' # Total national severity
    }).reset_index()
    
    # Flatten MultiIndex columns
    df_weather_agg.columns = ['_'.join(col).strip() if col[1] else col[0] for col in df_weather_agg.columns.values]
    
    # Rename for clarity
    df_weather_agg.rename(columns={
        'date_': 'date',
        'snowfall_cm_max': 'max_snow',
        'snowfall_cm_mean': 'mean_snow',
        'windspeed_kmh_max': 'max_wind',
        'windspeed_kmh_mean': 'mean_wind',
        'precipitation_mm_max': 'max_precip',
        'precipitation_mm_mean': 'mean_precip',
        'temperature_min_c_min': 'min_temp',
        'temperature_min_c_mean': 'mean_temp',
        'severity_score_sum': 'national_severity'
    }, inplace=True)

    # [NEW] Quadratic Snow Penalty (Exponential Damage)
    # User Request: "Snow ** 2" to punish deep snow
    df_weather_agg['max_snow_sq'] = df_weather_agg['max_snow'] ** 2
    df_weather_agg['mean_snow_sq'] = df_weather_agg['mean_snow'] ** 2
    
    # 4. Merge with Target
    print("   Merging with target (Cancel Rate)...")
    merged_df = pd.merge(df_weather_agg, df_bts, on='date', how='inner')
    
    # 5. Add Date Features
    merged_df['date'] = pd.to_datetime(merged_df['date'])
    merged_df['month'] = merged_df['date'].dt.month
    merged_df['day_of_year'] = merged_df['date'].dt.dayofyear
    
    # 6. Apply COVID Masking (Data Hygiene)
    mask_pandemic = (merged_df['date'] >= '2020-03-01') & (merged_df['date'] <= '2021-12-31')
    before_len = len(merged_df)
    merged_df = merged_df[~mask_pandemic]
    after_len = len(merged_df)
    print(f"   [Data Hygiene] Filtered out COVID-19 period (2020-03 to 2021-12). Rows: {before_len} -> {after_len}")
    
    return merged_df

def train_model():
    df = load_and_prep_data()
    print(f"Data ready. Shape: {df.shape}")
    
    # Features
    features = [
        'max_snow', 'mean_snow',
        'max_snow_sq', 'mean_snow_sq', # <--- [NEW]
        'max_wind', 'mean_wind',
        'max_precip', 'mean_precip',
        'min_temp', 'mean_temp',
        'national_severity',
        'month', 'day_of_year'
    ]
    
    X = df[features]
    y = df['cancel_rate']
    
    # Train/Test Split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    print("-" * 30)
    print("Training Shadow Model (Polynomial Regression - Degree 2)...")
    
    # [NEW] Polynomial Regression Pipeline
    # Using Ridge to prevent overfitting on the polynomial terms
    # degree=2 allows capturing interaction (Snow*Temp) and Quadratic (Snow^2) effects
    from sklearn.preprocessing import PolynomialFeatures, StandardScaler
    from sklearn.linear_model import Ridge
    from sklearn.pipeline import Pipeline
    
    model = Pipeline([
        ('scaler', StandardScaler()),
        ('poly', PolynomialFeatures(degree=2, include_bias=False)),
        ('regressor', Ridge(alpha=1.0))
    ])
    
    model.fit(X_train, y_train)
    
    # Validation
    y_pred = model.predict(X_test)
    y_pred = np.clip(y_pred, 0, 1.0) # Clip predictions
    
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    
    print(f"Validation Results (Poly V2):")
    print(f"MAE: {mae:.4f} (Average error in cancel rate)")
    print(f"R2 Score: {r2:.4f}")
    
    # Coefficients
    print("-" * 30)
    try:
        if hasattr(model['regressor'], 'coef_'):
            feature_names = model['poly'].get_feature_names_out(features)
            coefs = model['regressor'].coef_
            
            coef_df = pd.DataFrame({'feature': feature_names, 'coef': coefs})
            coef_df['abs_coef'] = coef_df['coef'].abs()
            print("Top 10 Coefficients:")
            print(coef_df.sort_values('abs_coef', ascending=False).head(10).to_string(index=False))
    except: pass
        
    # Save Logic
    # Save to src/models/shadow_weather_model.pkl
    save_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'shadow_weather_model.pkl')
    with open(save_path, 'wb') as f:
        pickle.dump(model, f)
        
    print(f"Model saved to: {save_path}")
    print("-" * 30)

if __name__ == "__main__":
    # Small fix for loop in printing feature importance (variable 'd' was undefined in thought process, fixed here)
    d = 0 
    train_model()
