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
    
    # 2. Extract and Format Numbers
    def num_replacer(match):
        raw = match.group(0)
        clean = raw.lower().replace(',', '').replace('million', '').replace('m', '').replace(' ','')
        try:
            val = float(clean)
            if val >= 1_000_000:
                return f"{val/1_000_000:.1f}M"
            elif val > 0:
                return f"{val/1_000_000:.1f}M"
            return raw 
        except:
            return raw

    label = re.sub(r'\b(?:\d{1,3}(?:,\d{3})*|\d+)(\.\d+)?\s*(?:million|m)?\b', num_replacer, label, flags=re.IGNORECASE)
    return label.strip()

def fetch_market_data(target_date):
    from src.config import POLYMARKET_API_URL
    
    if " - " in target_date or (target_date.count('-') > 2): # Weekly slug
        slug = f"number-of-tsa-passengers-{target_date.lower().replace(' ', '-')}"
    else:
        try:
            dt = datetime.datetime.strptime(target_date, "%Y-%m-%d")
            month_name = dt.strftime("%B").lower()
            day = dt.day
            slug = f"number-of-tsa-passengers-{month_name}-{day}"
        except:
            slug = f"number-of-tsa-passengers-{target_date.lower().replace(' ', '-')}"

    url = f"{POLYMARKET_API_URL}?slug={slug}"
    print(f"Fetching {slug}...")
    
    try:
        headers = {"User-Agent": "MikonAI/1.0"}
        res = requests.get(url, headers=headers, timeout=10)
        res.raise_for_status()
        data = res.json()
        
        if not data:
            print(f"  No event found for {slug}")
            return []
            
        event = data[0]
        markets = event.get('markets', [])
        
        results = []
        for m in markets:
            q_text = m.get('question', '')
            label = q_text
            import re
            match = re.search(r"be (.*?)\?", q_text)
            if match:
                label = match.group(1).strip()
            
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
                    "outcome_label": label,
                    "price": yes_price
                })
            except ValueError:
                for o, p in zip(outcomes, prices):
                     if o.lower() in ["yes", "no"]:
                         continue
                     results.append({
                        "target_date": target_date,
                        "market_slug": slug,
                        "outcome_label": o, 
                        "price": float(p)
                    })
        return results
    except Exception as e:
        print(f"  Error fetching {slug}: {e}")
        return []

def save_snapshots(snapshots):
    if not snapshots: return
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    data_tuple = [(s['target_date'], s['market_slug'], s['outcome_label'], s['price']) for s in snapshots]
    cursor.executemany('''
        INSERT INTO market_sentiment_snapshots (target_date, market_slug, outcome_label, price)
        VALUES (?, ?, ?, ?)
    ''', data_tuple)
    conn.commit()
    conn.close()

def run(recent=False):
    print("=== Polymarket ETL Started ===")
    if recent:
        start_dt = datetime.date.today() - timedelta(days=1)
        days_to_fetch = 10
    else:
        start_dt = datetime.date.today() - timedelta(days=3)
        days_to_fetch = 14
    
    snapshots = []
    for i in range(days_to_fetch):
        target_dt = start_dt + timedelta(days=i)
        target_str = target_dt.strftime("%Y-%m-%d")
        batch = fetch_market_data(target_str)
        snapshots.extend(batch)
    
    save_snapshots(snapshots)
    
    # Weekly Markets (Custom slugs)
    weekly_slugs = ["january-19-january-25", "january-26-february-1"]
    weekly_snapshots = []
    for slug_part in weekly_slugs:
        batch = fetch_market_data(slug_part)
        weekly_snapshots.extend(batch)
    
    if weekly_snapshots:
        save_snapshots(weekly_snapshots)
    print("=== Polymarket ETL Finished ===")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--recent', action='store_true')
    args = parser.parse_args()
    run(recent=args.recent)
