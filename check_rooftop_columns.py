#!/usr/bin/env python3
"""
Check rooftop solar parquet file columns
"""

import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from aemo_dashboard.shared.config import config

print(f"Checking rooftop solar file: {config.rooftop_solar_file}")

# Read parquet file
try:
    # Check schema first
    schema = pq.read_schema(config.rooftop_solar_file)
    print("\nParquet schema:")
    for field in schema:
        print(f"  - {field.name}: {field.type}")
    
    # Read sample data
    df = pd.read_parquet(config.rooftop_solar_file).head()
    print(f"\nDataFrame shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    print("\nSample data:")
    print(df)
    
except Exception as e:
    print(f"Error reading file: {e}")