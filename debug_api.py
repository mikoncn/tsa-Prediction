import requests

try:
    print("Fetching http://127.0.0.1:5000/api/predictions ...")
    r = requests.get("http://127.0.0.1:5000/api/predictions")
    if r.status_code != 200:
        print(f"Failed: {r.status_code}")
        print(r.text)
        exit()
        
    data = r.json()
    
    val = data.get('validation', [])
    forecast = data.get('forecast', [])
    print(f"Validation Count: {len(val)}")
    print(f"Forecast Count: {len(forecast)}")
    
    if len(forecast) > 0:
        print(f"Sample Forecast: {forecast[0]}")
    
    if len(val) > 0:
        print("First 3 records:")
        for v in val[:3]:
            print(v)
    else:
        print("No validation data found!")

except Exception as e:
    print(f"Error: {e}")
