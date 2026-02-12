#!/usr/bin/env python3
"""
Check data availability and date ranges for all data sources
"""

import os
import sys
import pandas as pd
import pickle
from pathlib import Path
from datetime import datetime

# Add the src directory to path
sys.path.insert(0, 'src')

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

def check_data_ranges():
    """Check date ranges for all data sources"""
    
    # Get file paths from environment
    gen_5min_path = os.getenv('GEN_OUTPUT_FILE_5MIN')
    gen_30min_path = os.getenv('GEN_OUTPUT_FILE')
    price_5min_path = os.getenv('SPOT_HIST_FILE')
    rooftop_path = os.getenv('ROOFTOP_SOLAR_FILE')
    gen_info_path = os.getenv('GEN_INFO_FILE')
    
    # If 5-minute paths not set, try to derive them
    if not gen_5min_path and gen_30min_path:
        gen_5min_path = gen_30min_path.replace('scada30.parquet', 'scada5.parquet')
    if not price_5min_path:
        price_5min_path = os.getenv('SPOT_HIST_FILE')
        if price_5min_path and 'prices30' in price_5min_path:
            price_5min_path = price_5min_path.replace('prices30.parquet', 'prices5.parquet')
    
    print("="*60)
    print("AEMO Data Availability Check")
    print("="*60)
    
    # Check generation 5-minute data
    if gen_5min_path and os.path.exists(gen_5min_path):
        print(f"\n1. Generation 5-minute data: {gen_5min_path}")
        try:
            df = pd.read_parquet(gen_5min_path)
            df['settlementdate'] = pd.to_datetime(df['settlementdate'])
            print(f"   Date range: {df['settlementdate'].min()} to {df['settlementdate'].max()}")
            print(f"   Total records: {len(df):,}")
            print(f"   Unique DUIDs: {df['duid'].nunique()}")
            
            # Check a few recent records
            recent = df.nlargest(5, 'settlementdate')[['settlementdate', 'duid', 'scadavalue']]
            print(f"   Most recent records:")
            print(recent.to_string(index=False))
        except Exception as e:
            print(f"   Error reading file: {e}")
    else:
        print(f"\n1. Generation 5-minute data: NOT FOUND at {gen_5min_path}")
    
    # Check generation 30-minute data
    if gen_30min_path and os.path.exists(gen_30min_path):
        print(f"\n2. Generation 30-minute data: {gen_30min_path}")
        try:
            df = pd.read_parquet(gen_30min_path)
            df['settlementdate'] = pd.to_datetime(df['settlementdate'])
            print(f"   Date range: {df['settlementdate'].min()} to {df['settlementdate'].max()}")
            print(f"   Total records: {len(df):,}")
            print(f"   Unique DUIDs: {df['duid'].nunique()}")
        except Exception as e:
            print(f"   Error reading file: {e}")
    
    # Check price 5-minute data
    if price_5min_path and os.path.exists(price_5min_path):
        print(f"\n3. Price 5-minute data: {price_5min_path}")
        try:
            df = pd.read_parquet(price_5min_path)
            df['settlementdate'] = pd.to_datetime(df['settlementdate'])
            print(f"   Date range: {df['settlementdate'].min()} to {df['settlementdate'].max()}")
            print(f"   Total records: {len(df):,}")
            print(f"   Unique regions: {df['regionid'].unique()}")
        except Exception as e:
            print(f"   Error reading file: {e}")
    else:
        print(f"\n3. Price 5-minute data: NOT FOUND at {price_5min_path}")
    
    # Check rooftop data
    if rooftop_path and os.path.exists(rooftop_path):
        print(f"\n4. Rooftop solar data: {rooftop_path}")
        try:
            df = pd.read_parquet(rooftop_path)
            df['settlementdate'] = pd.to_datetime(df['settlementdate'])
            print(f"   Date range: {df['settlementdate'].min()} to {df['settlementdate'].max()}")
            print(f"   Total records: {len(df):,}")
            print(f"   Columns: {list(df.columns)}")
            if 'regionid' in df.columns:
                print(f"   Unique regions: {df['regionid'].unique()}")
        except Exception as e:
            print(f"   Error reading file: {e}")
    
    # Check DUID mapping
    if gen_info_path and os.path.exists(gen_info_path):
        print(f"\n5. DUID mapping: {gen_info_path}")
        try:
            with open(gen_info_path, 'rb') as f:
                gen_info = pickle.load(f)
            if isinstance(gen_info, pd.DataFrame):
                print(f"   Total DUIDs: {len(gen_info)}")
                print(f"   Columns: {list(gen_info.columns)}")
                
                # Check fuel types
                if 'Fuel' in gen_info.columns:
                    fuel_counts = gen_info['Fuel'].value_counts()
                    print(f"\n   Fuel types:")
                    for fuel, count in fuel_counts.items():
                        print(f"   - {fuel}: {count} DUIDs")
            else:
                print(f"   Type: {type(gen_info)}")
        except Exception as e:
            print(f"   Error reading file: {e}")
    
    print("\n" + "="*60)

if __name__ == "__main__":
    check_data_ranges()