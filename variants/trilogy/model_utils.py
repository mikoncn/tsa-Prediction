import pandas as pd
import numpy as np

def get_aggregated_weather_features(conn):
    """
    Reads 'weather' table from DB and returns a DataFrame with 
    daily aggregated national weather features, ready for the Shadow Model.
    
    Returns columns:
    ['date', 'max_snow', 'mean_snow', 'max_wind', 'mean_wind', 
     'max_precip', 'mean_precip', 'min_temp', 'mean_temp', 
     'national_severity', 'month', 'day_of_year']
    """
    print("   [Model Utils] Reading and aggregating weather data...")
    df_weather = pd.read_sql("SELECT * FROM weather", conn)
    
    # Aggregate Weather (Hubs -> National)
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
    
    # Rename for clarity to match Shadow Model training features
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
    
    # [NEW] Quadratic Snow Penalty for Inference
    df_weather_agg['max_snow_sq'] = df_weather_agg['max_snow'] ** 2
    df_weather_agg['mean_snow_sq'] = df_weather_agg['mean_snow'] ** 2
    
    # Add Date Features required by model
    df_weather_agg['date'] = pd.to_datetime(df_weather_agg['date'])
    df_weather_agg['month'] = df_weather_agg['date'].dt.month
    df_weather_agg['day_of_year'] = df_weather_agg['date'].dt.dayofyear
    
    print(f"   [Model Utils] Aggregated {len(df_weather_agg)} daily records.")
    return df_weather_agg
