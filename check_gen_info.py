import pandas as pd

# Check the structure of gen_info.pkl
gen_info_path = "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard/data/gen_info.pkl"

try:
    df = pd.read_pickle(gen_info_path)
    print("gen_info.pkl structure:")
    print(f"Shape: {df.shape}")
    print(f"\nColumns: {list(df.columns)}")
    print(f"\nFirst few rows:")
    print(df.head())
    print(f"\nData types:")
    print(df.dtypes)
except Exception as e:
    print(f"Error loading gen_info.pkl: {e}")