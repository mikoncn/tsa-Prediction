import requests
import json
import sqlite3
import datetime
from datetime import timedelta
import sys
import os

# Add src path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.config import DB_PATH

def clean_label(label):
    import re
    # 1. Standardize Comparators (Case Insensitive)
    label = re.sub(r"between\s+", "", label, flags=re.IGNORECASE)
    label = re.sub(r"\s+and\s+", " - ", label, flags=re.IGNORECASE)
    label = re.sub(r"(greater|more)\s+than\s+", "> ", label, flags=re.IGNORECASE)
    label = re.sub(r"(fewer|less|under)\s+(than\s+)?", "< ", label, flags=re.IGNORECASE)
    
    # 2. Extract and Format Numbers (e.g. 1,600,000 -> 1.6M)
    # Pattern: Digit sequence, optional commas, optional decimal, optional 'million' or 'm'
    # We want to catch "1,600,000" or "1.6m" or "1.8 million"
    
    def num_replacer(match):
        raw = match.group(0)
        # Remove commas, spaces
        clean = raw.lower().replace(',', '').replace('million', '').replace('m', '').replace(' ','')
        try:
            val = float(clean)
            if val >= 1_000_000:
                return f"{val/1_000_000:.1f}M"
            elif val > 0: # Handle small numbers just in case, but usually they are > 1M
                return f"{val/1_000_000:.1f}M" # Force M format even for small if contextual?
            return raw 
        except:
            return raw

    # Regex to find numbers: 
    # Look for digits that might have commas, followed optionally by 'm' or 'million'
    # excluding things like dates '2026' if possible? No, usually markets are just numbers.
    
    label = re.sub(r'\b(?:\d{1,3}(?:,\d{3})*|\d+)(\.\d+)?\s*(?:million|m)?\b', num_replacer, label, flags=re.IGNORECASE)
    
    return label.strip()

def fetch_market_data(target_date):
    # Construct slug: number-of-tsa-passengers-{month}-{day}
    # Date format: 2026-01-17 -> january-17
    # Note: Polymarket slugs usually use lowercase full month name.
    
    dt = datetime.datetime.strptime(target_date, "%Y-%m-%d")
    month_name = dt.strftime("%B").lower() # january
    day = dt.day
    
    slug = f"number-of-tsa-passengers-{month_name}-{day}"
    url = f"https://gamma-api.polymarket.com/events?slug={slug}"
    
    print(f"Fetching {slug}...")
    
    try:
        headers = {"User-Agent": "MikonAI/1.0"}
        res = requests.get(url, headers=headers, timeout=10)
        data = res.json()
        
        if not data:
            print(f"  No event found for {slug}")
            return []
            
        event = data[0] # Expecting list
        markets = event.get('markets', [])
        
        results = []
        for m in markets:
            q_text = m.get('question', '')
            
            # Simple Outcome Parser
            # Check for generic pattern "Will TSA ... be X?"
            # Or "... count ... be X?"
            # We want to extract "X" as the label. e.g. "> 2.2 million"
            
            # Refined Label Logic:
            # 1. If question contains " > ", " < ", "between"
            # 2. Extract that phrase.
            
            label = q_text
            import re
            # Regex to find bucket info: " > 2,100,000", " < 2,300,000", " 2,100,000 - 2,300,000"
            # Pattern: (between|over|under|<|>) [\d,]+(\.[\d]+)?( million)?
            # Simplification: Split by "be " or "passenger count "?
            
            # Common pattern: "Will the total... be [TARGET]?"
            match = re.search(r"be (.*?)\?", q_text)
            if match:
                label = match.group(1).strip()
            
            # Match "between X and Y"
            # Match "greater than X"
            # Match "under X"
            # Match "fewer than X"
            
            label = clean_label(label)

            raw_outcomes = m.get('outcomes')
            outcomes = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else raw_outcomes
            
            raw_prices = m.get('outcomePrices')
            prices = json.loads(raw_prices) if isinstance(raw_prices, str) else raw_prices
            
            if not outcomes or not prices:
                continue
            
            try:
                yes_idx = outcomes.index("Yes")
                yes_price = float(prices[yes_idx])
                
                results.append({
                    "target_date": target_date,
                    "market_slug": slug,
                    "outcome_label": label, # e.g. "> 2.2M"
                    "price": yes_price
                })
            except ValueError:
                # If outcomes aren't Yes/No (e.g. multiple choice A, B, C)
                # Store all, BUT FILTER OUT explicit "Yes" or "No" if they appear as labels
                # The user specifically asked to remove "No" and "Yes" from appearing as OPTIONS.
                
                for o, p in zip(outcomes, prices):
                     if o.lower() in ["yes", "no"]:
                         continue
                         
                     results.append({
                        "target_date": target_date,
                        "market_slug": slug,
                        "outcome_label": o, 
                        "price": float(p)
                    })
        
        print(f"  Found {len(results)} betting lines.")
        return results
        
    except Exception as e:
        print(f"  Error fetching {slug}: {e}")
        return []

def save_snapshots(snapshots):
    if not snapshots:
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    data_tuple = [(s['target_date'], s['market_slug'], s['outcome_label'], s['price']) for s in snapshots]
    
    cursor.executemany('''
        INSERT INTO market_sentiment_snapshots (target_date, market_slug, outcome_label, price)
        VALUES (?, ?, ?, ?)
    ''', data_tuple)
    
    conn.commit()
    conn.close()
    print(f"Saved {len(snapshots)} snapshots to DB.")

def main():
    print("=== Polymarket ETL Started ===")
    
    # 解析参数
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--recent', action='store_true', help='Only fetch from T-1 to T+3')
    args = parser.parse_args()
    
    if args.recent:
        # 实时同步模式：仅覆盖 T-1 到 T+3 (重点观测区)
        start_dt = datetime.date.today() - timedelta(days=1)
        days_to_fetch = 5
        print(f"[Quick Sync] Fetching 5 days from {start_dt}")
    else:
        # 全量更新模式：T-3 到 T+10
        start_dt = datetime.date.today() - timedelta(days=3)
        days_to_fetch = 14
        print(f"[Full Sync] Fetching 14 days from {start_dt}")
    
    snapshots = []
    
    for i in range(days_to_fetch):
        target_dt = start_dt + timedelta(days=i)
        target_str = target_dt.strftime("%Y-%m-%d")
        
        batch = fetch_market_data(target_str)
        snapshots.extend(batch)
        
    save_snapshots(snapshots)
    print("=== Polymarket ETL Finished ===")

if __name__ == "__main__":
    main()
