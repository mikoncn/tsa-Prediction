import requests
import json

def test_slug(slug):
    # Try Gamma API which is more reliable for slug lookups
    url = f"https://gamma-api.polymarket.com/events?slug={slug}"
    try:
        res = requests.get(url, timeout=10)
        if res.status_code == 200:
            data = res.json()
            if data and len(data) > 0:
                print(f"SUCCESS: {slug}")
                print(f"Title: {data[0].get('title')}")
                return True
        print(f"FAILED: {slug} (Status: {res.status_code})")
        return False
    except Exception as e:
        print(f"ERROR: {slug} ({e})")
        return False

slugs = [
    "number-of-tsa-passengers-january-19-january-25",
    "number-of-tsa-passengers-january-26-february-1"
]

for s in slugs:
    test_slug(s)
