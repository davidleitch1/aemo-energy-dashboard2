#!/usr/bin/env python3
"""
Check the structure of both gen_info.pkl and geninfo_july25.xlsx files.
"""

import pandas as pd
import pickle
from pathlib import Path

# File paths
PICKLE_PATH = "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/genhist/gen_info.pkl"
EXCEL_PATH = "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/geninfo_july25.xlsx"

print("Checking pickle file structure...")
with open(PICKLE_PATH, 'rb') as f:
    pickle_data = pickle.load(f)

print(f"Pickle data type: {type(pickle_data)}")
if isinstance(pickle_data, pd.DataFrame):
    print(f"Pickle DataFrame shape: {pickle_data.shape}")
    print(f"Pickle columns: {list(pickle_data.columns)}")
    print(f"First few rows:")
    print(pickle_data.head())
elif isinstance(pickle_data, dict):
    print(f"Pickle is a dictionary with {len(pickle_data)} keys")
    print(f"First few keys: {list(pickle_data.keys())[:5]}")
    if pickle_data:
        first_key = list(pickle_data.keys())[0]
        print(f"Sample value for key '{first_key}': {pickle_data[first_key]}")

print("\n" + "="*80 + "\n")

print("Checking Excel file structure...")
excel_df = pd.read_excel(EXCEL_PATH)
print(f"Excel DataFrame shape: {excel_df.shape}")
print(f"Excel columns: {list(excel_df.columns)}")
print(f"First few rows:")
print(excel_df.head())