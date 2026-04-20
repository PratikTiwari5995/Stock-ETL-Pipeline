import requests
from dotenv import load_dotenv
import os
import datetime as dt
import time
import csv

load_dotenv() 
API_KEY = os.getenv('MASSIVE_API_KEY')
MASSIVE_BASE_URL = 'https://api.massive.com'
SLEEP_SECONDS = 15


def get_stock_prices(req_date):
    url = f"{MASSIVE_BASE_URL}/v2/aggs/grouped/locale/us/market/stocks/{req_date}"
    try:
        params = {
            "adjusted": "true",
            "include_otc": "false",
            "apiKey": API_KEY
        }
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        results = data.get("results", [])
    except Exception as e:
        print(f"Error: {e}")   
        results = []
    return results    


def extract_historical_data():
    # Use os.path.join for cross-platform paths (Windows, Mac, Linux)
    output_dir = os.path.join("data", "raw")
    os.makedirs(output_dir, exist_ok=True)
    
    start_date = dt.date(year=2026, month=4, day=9)
    end_date = dt.date(year=2026, month=4, day=16)

    out_file = os.path.join(
        output_dir,
        f"massive_eod_grouped_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
    )

    ingest_ts = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()

    with open(out_file, "w", newline="", encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["trade_date", "symbol", "open", "high", "low", "close", "volume", "_src_file", "_ingest_ts"])
        
        current_date = start_date
        while current_date <= end_date:
            print(f"Fetching {current_date} ...", end=" ")
            results = get_stock_prices(current_date)
            
            if results:
                print(f"✅ {len(results)} rows")
                for row in results:
                    writer.writerow([
                        current_date.strftime('%Y-%m-%d'),
                        row.get("T", ""),
                        row.get("o", ""),
                        row.get("h", ""),
                        row.get("l", ""),
                        row.get("c", ""),
                        row.get("v", ""),
                        out_file,
                        ingest_ts
                    ])
                f.flush()
            else:
                print(f"No data (weekend/holiday)")

            current_date += dt.timedelta(days=1)
            time.sleep(SLEEP_SECONDS)

    print(f"\nDone! Saved: {out_file}") 


if __name__ == '__main__':
    extract_historical_data()