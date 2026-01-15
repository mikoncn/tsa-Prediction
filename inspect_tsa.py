import requests
from bs4 import BeautifulSoup

url = "https://www.tsa.gov/travel/passenger-volumes"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    print(f"URL: {response.url}")
    
    soup = BeautifulSoup(response.content, 'html.parser')
    tables = soup.find_all('table')
    
    print(f"Total tables found: {len(tables)}")
    
    for i, table in enumerate(tables):
        print(f"\n=== TABLE {i+1} ===")
        
        # Headers
        headers_list = [th.get_text(strip=True) for th in table.find_all('th')]
        print(f"Headers ({len(headers_list)}): {headers_list}")
        
        # First Data Row
        rows = table.find_all('tr')
        data_rows = [r for r in rows if r.find('td')]
        if data_rows:
            first_row_cols = [td.get_text(strip=True) for td in data_rows[0].find_all('td')]
            print(f"First Row ({len(first_row_cols)} cols): {first_row_cols}")
        else:
            print("No data rows.")

except Exception as e:
    print(f"Error: {e}")
