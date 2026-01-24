import pandas as pd
import sys
import os

sys.path.append(os.getcwd())
from src.utils.holiday_utils import get_holiday_features, TARGET_HOLIDAYS

def generate_report():
    print("Generating Holiday Definitions Report...")
    
    # 1. Generate range
    dates = pd.date_range(start='2025-01-01', end='2026-12-31')
    
    # 2. Get Features
    df = get_holiday_features(dates)
    df['date'] = dates
    
    # 3. Filter for interesting days (Holidays, Windows, Spring Break)
    mask = (df['is_holiday'] == 1) | (df['is_holiday_travel_window'] == 1) | (df['is_spring_break'] == 1)
    df_filtered = df[mask].copy()
    
    # 4. Format
    df_filtered = df_filtered[['date', 'holiday_name', 'is_holiday', 'is_holiday_travel_window', 'is_spring_break']]
    
    # 5. Export
    csv_path = 'holiday_definitions.csv'
    df_filtered.to_csv(csv_path, index=False)
    print(f"Saved: {csv_path}")
    print("\nSample (Jan 2026):")
    print(df_filtered[df_filtered['date'].astype(str).str.startswith('2026-01')].head(10))

if __name__ == "__main__":
    generate_report()
