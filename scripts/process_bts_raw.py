import os
import glob
import pandas as pd
import zipfile
import io

# ==========================================
# Configuration
# ==========================================

# Target Airports (Hubs)
TARGET_AIRPORTS = ['ORD', 'ATL', 'DFW', 'DEN', 'JFK', 'LGA', 'EWR', 'LAX', 'SFO', 'SEA', 'MCO', 'LAS']

# Input Directory (BTS Raw Data)
# Assumes script is in root 'tsa' folder and data is in 'BTS' subfolder
INPUT_DIR = 'BTS'

# Output File
OUTPUT_FILE = 'bts_master_12_hubs.csv'

# Columns to Keep
KEEP_COLS = ['FL_DATE', 'ORIGIN', 'DEST', 'DEP_DELAY', 'CANCELLED']

# ==========================================
# Processing Logic
# ==========================================

def process_bts_data():
    print(f"=== Starting BTS Data Consolidation ===")
    print(f"Scanning directory: {INPUT_DIR}")
    
    # Check if directory exists
    if not os.path.exists(INPUT_DIR):
        print(f"[Error] Directory '{INPUT_DIR}' not found.")
        return

    # Find all zip files
    zip_files = glob.glob(os.path.join(INPUT_DIR, '*.zip'))
    print(f"Found {len(zip_files)} zip files.")
    
    if not zip_files:
        print("[Warning] No zip files found. Exiting.")
        return

    all_dfs = []
    total_processed_files = 0
    total_rows_raw = 0
    
    for zip_path in zip_files:
        try:
            print(f"Processing: {os.path.basename(zip_path)}...", end="", flush=True)
            
            with zipfile.ZipFile(zip_path, 'r') as z:
                # Find the CSV inside (assume one CSV per zip or take the first one)
                csv_files = [f for f in z.namelist() if f.lower().endswith('.csv')]
                
                if not csv_files:
                    print(" [Skipped - No CSV]")
                    continue
                
                # Read the first CSV found
                target_csv = csv_files[0]
                with z.open(target_csv) as f:
                    # Read CSV into DataFrame
                    # Using low_memory=False to avoid dtypes warning on large files if any, 
                    # though we are filtering immediately.
                    # CRITICAL OPTIMIZATION: We can't use 'usecols' easily because column names might vary slightly 
                    # (e.g. some might have "Origin" vs "ORIGIN"). 
                    # But BTS data is usually standard. Let's try to read all and filter columns after.
                    # Or read just the needed columns if we are confident. 
                    # Let's read all for safety regarding column names case, then standardize.
                    
                    df_chunk = pd.read_csv(f, low_memory=False)
                    
                    rows_before = len(df_chunk)
                    total_rows_raw += rows_before
                    
                    # Standardize column names to upper case just in case
                    df_chunk.columns = [c.upper() for c in df_chunk.columns]
                    
                    # Ensure required columns exist
                    missing_cols = [c for c in KEEP_COLS if c not in df_chunk.columns]
                    if missing_cols:
                        print(f" [Skipped - Missing columns: {missing_cols}]")
                        continue

                    # Filter for Target Airports (ORIGIN)
                    df_filtered = df_chunk[df_chunk['ORIGIN'].isin(TARGET_AIRPORTS)].copy()
                    
                    # Keep selected columns
                    df_filtered = df_filtered[KEEP_COLS]
                    
                    # Append to list
                    if not df_filtered.empty:
                        all_dfs.append(df_filtered)
                        print(f" [Done. Kept {len(df_filtered)}/{rows_before} rows]")
                    else:
                        print(f" [Done. No target flights]")
                        
                    total_processed_files += 1

        except Exception as e:
            print(f" [Error: {e}]")
            continue

    print("-" * 30)
    print(f"Processed {total_processed_files} files.")
    
    if not all_dfs:
        print("[Warning] No data found matching criteria.")
        return

    # Consolidate
    print("Concatenating data...")
    master_df = pd.concat(all_dfs, ignore_index=True)
    
    # Post-Processing
    print("Cleaning and Sorting...")
    
    # Convert FL_DATE to datetime
    master_df['FL_DATE'] = pd.to_datetime(master_df['FL_DATE'])
    
    # Fill NaN Dep Delay with 0 (Assuming on-time or cancelled without delay data)
    # Note: Cancelled flights might have NaN delay. 
    # If CANCELLED is 1, Delay might be NaN. keeping it as 0 or keeping it as NaN? 
    # User requirement: "Fill NaN in DEP_DELAY with 0 (or handle appropriately)."
    # We will fill with 0.
    master_df['DEP_DELAY'] = master_df['DEP_DELAY'].fillna(0)
    
    # Sort by Date
    master_df = master_df.sort_values(by='FL_DATE', ascending=True)
    
    # Validation Statistics
    min_date = master_df['FL_DATE'].min().strftime('%Y-%m-%d')
    max_date = master_df['FL_DATE'].max().strftime('%Y-%m-%d')
    total_rows = len(master_df)
    
    print("=" * 30)
    print("FINAL VALIDATION REPORT")
    print("=" * 30)
    print(f"Total Rows: {total_rows}")
    print(f"Date Range: {min_date} to {max_date}")
    print(f"Airports: {master_df['ORIGIN'].unique()}")
    print("-" * 30)
    print("First 5 Rows:")
    print(master_df.head())
    print("-" * 30)
    
    # Save
    print(f"Saving to {OUTPUT_FILE}...")
    master_df.to_csv(OUTPUT_FILE, index=False)
    print("Done.")

if __name__ == "__main__":
    process_bts_data()
