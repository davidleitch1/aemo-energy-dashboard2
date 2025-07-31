#!/usr/bin/env python3
"""Check column names in prices30.parquet"""

import duckdb
from pathlib import Path

# Data path
DATA_PATH = '/Volumes/davidleitch/aemo_production/data'

conn = duckdb.connect(':memory:')
prices_path = Path(DATA_PATH) / 'prices30.parquet'

# Get column info
result = conn.execute(f"""
    SELECT * FROM parquet_scan('{prices_path}') LIMIT 1
""").description

print("Columns in prices30.parquet:")
for col in result:
    print(f"  - {col[0]}")

# Also check a sample
print("\nSample data:")
df = conn.execute(f"""
    SELECT * FROM parquet_scan('{prices_path}') 
    WHERE REGIONID = 'NSW1' 
    LIMIT 5
""").fetchdf()

print(df)
conn.close()