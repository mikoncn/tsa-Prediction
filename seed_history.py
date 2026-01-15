import pandas as pd
import os

print("🌱 Seeding Prediction History from Backtest (Jan 1 - Jan 13)...")

val_file = "xgb_validation.csv"
history_file = "prediction_history.csv"

if not os.path.exists(val_file):
    print(f"Error: {val_file} not found.")
    exit()

# 1. Load Validation Data
df_val = pd.read_csv(val_file)

# 2. Transform to History Format
# Format: target_date, predicted_throughput, model_run_date
df_seed = pd.DataFrame()
df_seed['target_date'] = df_val['date']
df_seed['predicted_throughput'] = df_val['predicted']
df_seed['model_run_date'] = '2025-12-31' # Simulate a run before 2026

print(f"Loaded {len(df_seed)} rows from validation.")

# 3. Load Existing History
if os.path.exists(history_file):
    df_hist = pd.read_csv(history_file)
    print(f"Existing history has {len(df_hist)} rows.")
    
    # Merge (Avoid duplicates)
    # Filter out entries where target_date is already in seed ? No, keep seed.
    # Actually, keep seed and existing.
    
    # Check if seed already exists
    mask_exists = (df_hist['model_run_date'] == '2025-12-31')
    if mask_exists.sum() > 0:
        print("Warning: 2025-12-31 run already exists. Replacing it.")
        df_hist = df_hist[~mask_exists]
        
    final_df = pd.concat([df_hist, df_seed], ignore_index=True)
else:
    final_df = df_seed

# 4. Save
final_df = final_df.sort_values(['model_run_date', 'target_date'])
final_df.to_csv(history_file, index=False)
print(f"✅ Successfully seeded history. Total rows: {len(final_df)}")
print(final_df.head())
