import os

# Define project root (one level up from src/)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Database Path
DB_PATH = os.path.join(PROJECT_ROOT, 'tsa_data.db')

# Model Paths
MODEL_DIR = os.path.join(PROJECT_ROOT) 

# [ARCH] Forecast Model (T+1 to T+7) - Trained by train_xgb.py
FORECAST_MODEL_PATH = os.path.join(PROJECT_ROOT, 'xgb_forecast_v1.json')

# [ARCH] Sniper Model (T+0 Nowcast) - Trained/Used by predict_sniper.py
SNIPER_MODEL_PATH = os.path.join(PROJECT_ROOT, 'sniper_jit_v1.json')
