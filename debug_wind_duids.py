#!/usr/bin/env python3
"""Debug wind DUID differences between gen_info files"""

import pickle
import pandas as pd

# Paths
notebook_gen_info = '/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard/data/gen_info.pkl'
prod_gen_info = '/Volumes/davidleitch/aemo_production/data/gen_info.pkl'

# Load both gen_info files
print("Loading gen_info files...")

with open(notebook_gen_info, 'rb') as f:
    gen_info_notebook = pickle.load(f)

with open(prod_gen_info, 'rb') as f:
    gen_info_prod = pickle.load(f)

print(f"\nNotebook gen_info shape: {gen_info_notebook.shape}")
print(f"Production gen_info shape: {gen_info_prod.shape}")

# Check wind DUIDs with case-insensitive comparison
print("\nChecking Wind DUIDs...")

# Notebook version (your code uses .str.lower())
wind_duids_notebook = gen_info_notebook[gen_info_notebook['Fuel'].str.lower() == 'wind']['DUID'].tolist()
print(f"Notebook Wind DUIDs (using .str.lower()): {len(wind_duids_notebook)}")

# Production version (my code uses exact match)
wind_duids_prod = gen_info_prod[gen_info_prod['Fuel'] == 'Wind']['DUID'].tolist()
print(f"Production Wind DUIDs (exact match): {len(wind_duids_prod)}")

# Check what fuel values exist
print("\nUnique Fuel values in notebook gen_info:")
print(gen_info_notebook['Fuel'].value_counts())

print("\nUnique Fuel values in production gen_info:")
print(gen_info_prod['Fuel'].value_counts())

# Find differences
set_notebook = set(wind_duids_notebook)
set_prod = set(wind_duids_prod)

only_notebook = set_notebook - set_prod
only_prod = set_prod - set_notebook

if only_notebook:
    print(f"\nDUIDs only in notebook list: {only_notebook}")
if only_prod:
    print(f"\nDUIDs only in production list: {only_prod}")

# Check if the files are identical
if gen_info_notebook.equals(gen_info_prod):
    print("\n✓ Gen_info files are identical")
else:
    print("\n✗ Gen_info files are different")