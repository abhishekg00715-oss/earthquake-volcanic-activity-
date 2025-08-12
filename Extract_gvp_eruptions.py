# fetch_gvp_eruptions.py


# This requests the GVP WFS layer in CSV. We'll filter to 1925-08-09..2025-08-09 locally.
base = "https://webservices.volcano.si.edu/geoserver/GVP-VOTW/ows"
params = {
    "service": "WFS",
    "version": "1.0.0",
    "request": "GetFeature",
    "typeName": "GVP-VOTW:Smithsonian_VOTW_Holocene_Eruptions",
    "outputFormat": "csv",
    "maxFeatures": "1000000"
}

print("Requesting GVP WFS CSV...")
r = requests.get(base, params=params, timeout=180)
r.raise_for_status()

df = pd.read_csv(StringIO(r.text))
# Try common date columns; create a normalized 'start_date' column if possible
for col in ['StartDate','Start_year','Start','StartYear','EventDate','BeginDate']:
    if col in df.columns:
        df['start_date'] = pd.to_datetime(df[col], errors='coerce')
        break

# filter to 1925-08-09 .. 2025-08-09 if start_date present
start = pd.to_datetime("1925-08-09")
end   = pd.to_datetime("2025-08-09")
if 'start_date' in df.columns:
    df = df[(df['start_date'] >= start) & (df['start_date'] <= end)]

out_csv = "gvp_eruptions_1925_2025.csv"
out_parquet = "gvp_eruptions_1925_2025.parquet"
df.to_csv(out_csv, index=False)
df.to_parquet(out_parquet, index=False)
print("Saved:", out_csv, "and", out_parquet, "rows:", len(df))
