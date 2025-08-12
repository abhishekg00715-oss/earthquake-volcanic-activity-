# fetch_usgs_quakes.py
import requests
import pandas as pd
from io import StringIO

start = 1925
end   = 2025
minmag = 4.0

all_dfs = []

for year in range(start, end, 10):
    start_date = f"{year}-08-09"
    end_date = f"{min(year + 9, end)}-08-09" # Fetch 10 years at a time

    url = (
        "https://earthquake.usgs.gov/fdsnws/event/1/query"
        f"?format=csv&starttime={start_date}&endtime={end_date}&minmagnitude={minmag}&orderby=time"
    )

    print("Requesting:", url)
    try:
        r = requests.get(url, timeout=180)
        r.raise_for_status()
        df = pd.read_csv(StringIO(r.text))
        all_dfs.append(df)
        print(f"Fetched data for {start_date} to {end_date}. Rows: {len(df)}")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {start_date} to {end_date}: {e}")
        # You might want to add more robust error handling here, like retries.

df = pd.concat(all_dfs, ignore_index=True)
df['time'] = pd.to_datetime(df['time'], utc=True)
out_csv = "usgs_earthquakes_1925_2025_mag4plus.csv"
out_parquet = "usgs_earthquakes_1925_2025_mag4plus.parquet"
df.to_csv(out_csv, index=False)
df.to_parquet(out_parquet, index=False)
print("Saved:", out_csv, "and", out_parquet, "rows:", len(df))
