# This is module to load the datasets - Earthquakes and volcanic eruptions
def normalize_cols(df: pd.DataFrame):
    return df.rename(columns=lambda c: str(c).strip().lower().replace(' ', '_'))

def load_usgs(path: str):
    """Load USGS CSV and normalize column names."""
    df = pd.read_csv(path, low_memory=False)
    df = normalize_cols(df)
    return df

def load_gvp(path: str):
    """Load GVP CSV and normalize column names."""
    df = pd.read_csv(path, low_memory=False)
    df = normalize_cols(df)
    return df
