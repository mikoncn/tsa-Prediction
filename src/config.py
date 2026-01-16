import os

# Define project root (one level up from src/)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Database Path
DB_PATH = os.path.join(PROJECT_ROOT, 'tsa_data.db')

# Model Paths
MODEL_DIR = os.path.join(PROJECT_ROOT) # Currently models are in root or moved? 
# Plan says move predict_sniper.py to src/models. 
# The JSON model file usually stays in root or can be moved to data/models.
# For now, let's assume valid paths.
SNIPER_MODEL_PATH = os.path.join(PROJECT_ROOT, 'sniper_model.json')
