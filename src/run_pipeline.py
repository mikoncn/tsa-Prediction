import sys
import os
import time

# Ensure src is in path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.etl import build_tsa_db, fetch_polymarket, get_weather_features, merge_db
from src.models import train_xgb

def run_all():
    print("🚀 [Mikon AI] Starting Headless Pipeline...", flush=True)
    start_time = time.time()
    
    steps = [
        ("1. Fetch TSA Data", build_tsa_db.run, {'latest': True}),
        ("2. Sync Weather", get_weather_features.run, {}),
        ("3. Sync Flights (Skipped)", lambda **k: None, {}),
        ("4. Fetch Market", fetch_polymarket.run, {'recent': True}),
        ("5. Merge DB", merge_db.run, {}),
        ("6. Train Model", train_xgb.run, {})
    ]
    
    for name, func, kwargs in steps:
        print(f"\n👉 {name}...", flush=True)
        try:
            func(**kwargs)
            print(f"   ✅ {name} Completed.")
        except Exception as e:
            print(f"   ❌ {name} FAILED: {e}", flush=True)
            # Fail fast or continue? User prefers defined reliability. 
            # If TSA or Merge fails, pipeline is broken.
            # If Market/Flight fails, maybe recoverable?
            # For n8n, usually better to fail hard so we get notified.
            sys.exit(1)
            
    elapsed = time.time() - start_time
    print(f"\n✨ Pipeline Finished Successfully in {elapsed:.1f}s", flush=True)

if __name__ == "__main__":
    run_all()
