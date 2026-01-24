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

# API Endpoints
POLYMARKET_API_URL = "https://gamma-api.polymarket.com/events"
OPENSKY_API_URL = "https://opensky-network.org/api/flights/arrival"
TSA_URL = "https://www.tsa.gov/travel/passenger-volumes"
