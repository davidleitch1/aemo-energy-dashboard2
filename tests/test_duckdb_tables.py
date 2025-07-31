#!/usr/bin/env python3
"""Test DuckDB table creation"""

import os
import sys
from pathlib import Path

# Add the src directory to path
sys.path.insert(0, 'src')

# Load environment variables
from dotenv import load_dotenv

# Check for custom env file first
custom_env_path = os.getenv('GAUGE_ENV_FILE')
if custom_env_path and Path(custom_env_path).exists():
    load_dotenv(custom_env_path)
    print(f"Loaded environment from custom path: {custom_env_path}")
else:
    # Try to load .env from current directory
    if Path('.env').exists():
        load_dotenv('.env')
        print(f"Loaded environment from: .env")

print("\n=== Testing DuckDB Table Creation ===")

# Import after environment is loaded
from aemo_dashboard.shared.config import config
import duckdb

# Show config values
print("\nConfig values:")
print(f"  gen_output_file: {config.gen_output_file}")
print(f"  spot_hist_file: {config.spot_hist_file}")
print(f"  transmission_output_file: {config.transmission_output_file}")
print(f"  rooftop_solar_file: {config.rooftop_solar_file}")

# Check environment variables for 5-minute files
print("\nEnvironment variables:")
print(f"  GEN_OUTPUT_FILE: {os.getenv('GEN_OUTPUT_FILE')}")
print(f"  GEN_OUTPUT_FILE_5MIN: {os.getenv('GEN_OUTPUT_FILE_5MIN')}")
print(f"  SPOT_HIST_FILE: {os.getenv('SPOT_HIST_FILE')}")
print(f"  SPOT_HIST_FILE_5MIN: {os.getenv('SPOT_HIST_FILE_5MIN')}")

# Test the logic from shared_data_duckdb.py
print("\n\nTesting path construction logic:")
gen_30_path = str(config.gen_output_file).replace('scada5.parquet', 'scada30.parquet')
gen_5_path = str(config.gen_output_file)

print(f"  gen_30_path: {gen_30_path}")
print(f"  gen_5_path: {gen_5_path}")

# Check if files exist
print("\nFile existence check:")
for name, path in [("gen_30_path", gen_30_path), ("gen_5_path", gen_5_path)]:
    exists = os.path.exists(path) if path else False
    print(f"  {name}: {'✓' if exists else '✗'} {path}")

# Try creating views
print("\n\nTesting DuckDB view creation:")
conn = duckdb.connect(':memory:')

try:
    # Try to create generation views with actual paths
    if os.path.exists(gen_30_path):
        conn.execute(f"CREATE VIEW generation_30min AS SELECT * FROM read_parquet('{gen_30_path}')")
        count = conn.execute("SELECT COUNT(*) FROM generation_30min").fetchone()[0]
        print(f"✓ generation_30min created with {count} rows")
    else:
        print(f"✗ Cannot create generation_30min - file not found: {gen_30_path}")
        
    if os.path.exists(gen_5_path):
        conn.execute(f"CREATE VIEW generation_5min AS SELECT * FROM read_parquet('{gen_5_path}')")
        count = conn.execute("SELECT COUNT(*) FROM generation_5min").fetchone()[0]
        print(f"✓ generation_5min created with {count} rows")
    else:
        print(f"✗ Cannot create generation_5min - file not found: {gen_5_path}")
        
    # Check what we have
    print("\n\nChecking available views:")
    views = conn.execute("SHOW TABLES").fetchall()
    for view in views:
        print(f"  - {view[0]}")
        
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

conn.close()