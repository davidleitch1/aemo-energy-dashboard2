#!/usr/bin/env python3
"""Test data loading for renewable gauge"""

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
    # Try to load .env
    current_path = Path.cwd()
    for _ in range(3):
        env_file = current_path / '.env'
        if env_file.exists():
            load_dotenv(env_file)
            print(f"Loaded environment from: {env_file}")
            break
        current_path = current_path.parent

# Now test data loading
print("\n=== Testing Data Configuration ===")
print(f"GEN_OUTPUT_FILE: {os.getenv('GEN_OUTPUT_FILE')}")
print(f"SPOT_HIST_FILE: {os.getenv('SPOT_HIST_FILE')}")
print(f"ROOFTOP_SOLAR_FILE: {os.getenv('ROOFTOP_SOLAR_FILE')}")
print(f"GEN_INFO_FILE: {os.getenv('GEN_INFO_FILE')}")

# Check if files exist
gen_file = os.getenv('GEN_OUTPUT_FILE')
if gen_file and os.path.exists(gen_file):
    print(f"\n✓ Generation file exists: {gen_file}")
    print(f"  Size: {os.path.getsize(gen_file) / 1024 / 1024:.1f} MB")
else:
    print(f"\n✗ Generation file NOT found: {gen_file}")

# Now test the query manager
print("\n=== Testing Query Manager ===")
try:
    from aemo_dashboard.nem_dash.nem_dash_query_manager import NEMDashQueryManager
    from aemo_dashboard.shared.logging_config import get_logger
    
    logger = get_logger(__name__)
    query_manager = NEMDashQueryManager()
    
    print("Query manager initialized")
    
    # Test renewable data
    print("\n=== Testing Renewable Data Query ===")
    renewable_data = query_manager.get_renewable_data()
    print(f"Result type: {type(renewable_data)}")
    print(f"Result: {renewable_data}")
    
    # Test generation data directly
    print("\n=== Testing Generation Query ===")
    from datetime import datetime, timedelta
    end_date = datetime.now()
    start_date = end_date - timedelta(minutes=15)
    
    # Try different approaches
    print(f"\nQuerying generation data from {start_date} to {end_date}...")
    
    # Check if generation_manager exists
    if hasattr(query_manager, 'generation_manager'):
        print("Using generation_manager...")
        gen_data = query_manager.generation_manager.query_generation_by_fuel(
            start_date=start_date,
            end_date=end_date,
            region='NEM',
            resolution='5min'
        )
        print(f"Generation data shape: {gen_data.shape if hasattr(gen_data, 'shape') else 'N/A'}")
        if hasattr(gen_data, 'empty'):
            print(f"Generation data empty: {gen_data.empty}")
        if hasattr(gen_data, 'head'):
            print(f"First few rows:\n{gen_data.head()}")
    else:
        print("No generation_manager attribute found")
        
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

# Test direct file access
print("\n=== Testing Direct Parquet Access ===")
try:
    import pandas as pd
    gen_file = os.getenv('GEN_OUTPUT_FILE')
    if gen_file and os.path.exists(gen_file):
        print(f"Reading {gen_file}...")
        df = pd.read_parquet(gen_file)
        print(f"Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print(f"Date range: {df['settlementdate'].min()} to {df['settlementdate'].max()}")
        
        # Check latest data
        latest = df.nlargest(10, 'settlementdate')
        print(f"\nLatest 10 records:")
        print(latest[['settlementdate', 'duid', 'scadavalue']].head())
except Exception as e:
    print(f"Error reading parquet: {e}")