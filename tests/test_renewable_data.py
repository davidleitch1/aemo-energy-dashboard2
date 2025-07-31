#!/usr/bin/env python3
"""Test renewable data loading"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

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

# Now test renewable data loading
print("\n=== Testing Renewable Data Loading ===")

# Import after environment is loaded
from aemo_dashboard.nem_dash.nem_dash_query_manager import NEMDashQueryManager
from aemo_dashboard.generation.generation_query_manager import GenerationQueryManager

# Test direct generation query manager
print("\n1. Testing GenerationQueryManager directly:")
gen_manager = GenerationQueryManager()

# Try different time ranges and resolutions
end_date = datetime.now()
test_configs = [
    (timedelta(minutes=5), '5min', 'Last 5 minutes - 5min resolution'),
    (timedelta(minutes=30), '5min', 'Last 30 minutes - 5min resolution'),
    (timedelta(hours=1), '5min', 'Last hour - 5min resolution'),
    (timedelta(minutes=30), '30min', 'Last 30 minutes - 30min resolution'),
    (timedelta(hours=1), '30min', 'Last hour - 30min resolution'),
]

for time_delta, resolution, desc in test_configs:
    print(f"\n{desc}:")
    start_date = end_date - time_delta
    try:
        data = gen_manager.query_generation_by_fuel(
            start_date=start_date,
            end_date=end_date,
            region='NEM',
            resolution=resolution
        )
        if hasattr(data, 'empty'):
            print(f"  Result: {'EMPTY' if data.empty else f'{len(data)} rows'}")
            if not data.empty:
                print(f"  Columns: {list(data.columns)}")
                print(f"  Date range: {data['settlementdate'].min()} to {data['settlementdate'].max()}")
                if 'fuel_type' in data.columns:
                    print(f"  Fuel types: {sorted(data['fuel_type'].unique())}")
        else:
            print(f"  Result type: {type(data)}")
    except Exception as e:
        print(f"  Error: {e}")

# Test NEM dash query manager
print("\n\n2. Testing NEMDashQueryManager.get_renewable_data():")
try:
    query_manager = NEMDashQueryManager()
    renewable_data = query_manager.get_renewable_data()
    print(f"Result type: {type(renewable_data)}")
    print(f"Result: {renewable_data}")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

# Check file paths
print("\n\n3. Checking data file existence:")
files_to_check = [
    ('GEN_OUTPUT_FILE', os.getenv('GEN_OUTPUT_FILE')),
    ('GEN_OUTPUT_FILE_5MIN', os.getenv('GEN_OUTPUT_FILE_5MIN')),
    ('ROOFTOP_SOLAR_FILE', os.getenv('ROOFTOP_SOLAR_FILE')),
]

for name, path in files_to_check:
    if path:
        exists = os.path.exists(path)
        size = os.path.getsize(path) / 1024 / 1024 if exists else 0
        print(f"{name}: {path}")
        print(f"  Exists: {'✓' if exists else '✗'}")
        if exists:
            print(f"  Size: {size:.1f} MB")
    else:
        print(f"{name}: Not set")