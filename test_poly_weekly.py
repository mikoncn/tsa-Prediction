import requests
import datetime

def test_slug(slug):
    url = f"https://clob.polymarket.com/events?slug={slug}"
    try:
        res = requests.get(url, timeout=10)
        data = res.json()
        if data:
            print(f"SUCCESS: {slug}")
            # print(data[0].get('title'))
            return True
        else:
            print(f"FAILED: {slug} (No data)")
            return False
    except Exception as e:
        print(f"ERROR: {slug} ({e})")
        return False

# Potential patterns based on user link
slugs = [
    "number-of-tsa-passengers-january-19-january-25",
    "number-of-tsa-passengers-january-26-february-1",
    "number-of-tsa-passengers-january-26-january-1", # maybe
    "number-of-tsa-passengers-january-19-25", # maybe
]

for s in slugs:
    test_slug(s)
