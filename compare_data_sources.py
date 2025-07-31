#!/usr/bin/env python3
"""Compare wind data between development and production databases"""

import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# Data paths
DEV_PATH = '/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2'
PROD_PATH = '/Volumes/davidleitch/aemo_production/data'

def check_latest_data():
    """Check latest timestamps in both datasets"""
    
    print("Checking latest data in both sources...\n")
    
    # Check development data
    try:
        dev_scada5 = Path(DEV_PATH) / "scada5.parquet"
        if dev_scada5.exists():
            df_dev = pd.read_parquet(dev_scada5)
            df_dev['settlementdate'] = pd.to_datetime(df_dev['settlementdate'])
            latest_dev = df_dev['settlementdate'].max()
            print(f"Development scada5 latest: {latest_dev}")
            
            # Check specific date range
            check_date = datetime(2025, 7, 25, 21, 0, 0)
            dev_data = df_dev[
                (df_dev['settlementdate'] >= check_date) & 
                (df_dev['settlementdate'] <= check_date + timedelta(hours=1))
            ]
            print(f"Development records for July 25 21:00-22:00: {len(dev_data)}")
        else:
            print("Development scada5.parquet not found")
    except Exception as e:
        print(f"Error reading development data: {e}")
    
    print()
    
    # Check production data
    try:
        prod_scada5 = Path(PROD_PATH) / "scada5.parquet"
        if prod_scada5.exists():
            df_prod = pd.read_parquet(prod_scada5)
            df_prod['settlementdate'] = pd.to_datetime(df_prod['settlementdate'])
            latest_prod = df_prod['settlementdate'].max()
            print(f"Production scada5 latest: {latest_prod}")
            
            # Check specific date range
            prod_data = df_prod[
                (df_prod['settlementdate'] >= check_date) & 
                (df_prod['settlementdate'] <= check_date + timedelta(hours=1))
            ]
            print(f"Production records for July 25 21:00-22:00: {len(prod_data)}")
        else:
            print("Production scada5.parquet not found")
    except Exception as e:
        print(f"Error reading production data: {e}")
    
    # Also check gen_info location
    print("\n\nChecking gen_info.pkl locations:")
    
    # Your notebook path
    notebook_gen_info = '/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard/data/gen_info.pkl'
    if Path(notebook_gen_info).exists():
        print(f"✓ Found at notebook path: {notebook_gen_info}")
    else:
        print(f"✗ Not found at notebook path: {notebook_gen_info}")
    
    # Development path
    dev_gen_info = Path(DEV_PATH) / "gen_info.pkl"
    if dev_gen_info.exists():
        print(f"✓ Found at dev path: {dev_gen_info}")
    else:
        print(f"✗ Not found at dev path: {dev_gen_info}")
    
    # Production path
    prod_gen_info = Path(PROD_PATH) / "gen_info.pkl"
    if prod_gen_info.exists():
        print(f"✓ Found at prod path: {prod_gen_info}")
    else:
        print(f"✗ Not found at prod path: {prod_gen_info}")

if __name__ == "__main__":
    check_latest_data()