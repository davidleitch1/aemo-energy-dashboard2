#!/usr/bin/env python3
"""
Check the actual size and date ranges of the data files
"""

import pandas as pd
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from aemo_dashboard.shared.config import config

def check_file_info(file_path, name):
    """Check basic info about a parquet file"""
    print(f"\n{name}:")
    print("-" * 40)
    
    try:
        # Get metadata using pyarrow
        import pyarrow.parquet as pq
        parquet_file = pq.ParquetFile(file_path)
        schema = parquet_file.schema_arrow
        columns = [field.name for field in schema]
        print(f"Columns: {columns}")
        total_rows = parquet_file.metadata.num_rows
        print(f"Total rows: {total_rows:,}")
        
        # Get date range by reading just the date column
        if 'settlementdate' in columns:
            dates = pd.read_parquet(file_path, columns=['settlementdate'])
            date_min = dates['settlementdate'].min()
            date_max = dates['settlementdate'].max()
            date_range_days = (date_max - date_min).days
            
            print(f"Date range: {date_min} to {date_max}")
            print(f"Duration: {date_range_days} days ({date_range_days/365:.1f} years)")
            
            # Expected rows calculation
            if '30' in name.lower():
                expected_rows = date_range_days * 24 * 2  # 48 per day for 30-min
            else:
                expected_rows = date_range_days * 24 * 12  # 288 per day for 5-min
            
            # For generation, multiply by number of DUIDs
            if 'generation' in name.lower() or 'scada' in name.lower():
                # Count unique DUIDs
                duid_sample = pd.read_parquet(file_path, columns=['duid'])
                unique_duids = duid_sample['duid'].nunique()
                print(f"Unique DUIDs: {unique_duids}")
                expected_rows *= unique_duids
                
            print(f"Expected rows (approx): {expected_rows:,}")
            print(f"Actual vs Expected: {total_rows/expected_rows:.1f}x")
            
    except Exception as e:
        print(f"Error: {e}")

def main():
    """Check all data files"""
    print("="*60)
    print("DATA FILE SIZE ANALYSIS")
    print("="*60)
    
    files_to_check = [
        (config.gen_output_file, "Generation 5min"),
        (str(config.gen_output_file).replace('scada5.parquet', 'scada30.parquet'), "Generation 30min"),
        (config.spot_hist_file, "Prices 5min"),
        (str(config.spot_hist_file).replace('prices5.parquet', 'prices30.parquet'), "Prices 30min"),
        (config.transmission_output_file, "Transmission 5min"),
        (str(config.transmission_output_file).replace('transmission5.parquet', 'transmission30.parquet'), "Transmission 30min"),
        (config.rooftop_solar_file, "Rooftop Solar"),
    ]
    
    for file_path, name in files_to_check:
        check_file_info(file_path, name)
    
    print("\n" + "="*60)
    print("RECOMMENDATION:")
    print("="*60)
    print("The generation data files are extremely large (38M+ rows).")
    print("This appears to cover multiple years of data.")
    print("For a dashboard, consider:")
    print("1. Limiting data to recent period (e.g., last 90 days)")
    print("2. Using aggregated data for historical views")
    print("3. Loading data on-demand for specific date ranges")
    print("="*60)

if __name__ == "__main__":
    main()