#!/usr/bin/env python3
"""
Check all parquet files for corruption or access issues
"""

import os
import sys
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime
import time

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from aemo_dashboard.shared.config import config

def check_parquet_file(file_path, description):
    """Check a single parquet file for issues"""
    print(f"\n{'='*60}")
    print(f"Checking: {description}")
    print(f"Path: {file_path}")
    print(f"{'='*60}")
    
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"❌ FILE NOT FOUND")
        return False
    
    # Get file stats
    file_stats = os.stat(file_path)
    file_size_mb = file_stats.st_size / (1024 * 1024)
    mod_time = datetime.fromtimestamp(file_stats.st_mtime)
    
    print(f"File size: {file_size_mb:.2f} MB")
    print(f"Last modified: {mod_time}")
    print(f"Age: {(datetime.now() - mod_time).total_seconds() / 60:.1f} minutes")
    
    # Try to read file metadata
    try:
        start_time = time.time()
        
        # First try with pyarrow to get detailed error info
        parquet_file = pq.ParquetFile(file_path)
        num_rows = parquet_file.metadata.num_rows
        num_columns = len(parquet_file.schema)
        
        print(f"✅ Metadata read successful")
        print(f"   Rows: {num_rows:,}")
        print(f"   Columns: {num_columns}")
        print(f"   Schema: {', '.join([field.name for field in parquet_file.schema])[:100]}...")
        
        # Try to read first and last row groups
        if parquet_file.num_row_groups > 0:
            first_group = parquet_file.read_row_group(0)
            print(f"   First row group: {len(first_group)} rows")
            
            if parquet_file.num_row_groups > 1:
                last_group = parquet_file.read_row_group(parquet_file.num_row_groups - 1)
                print(f"   Last row group: {len(last_group)} rows")
        
        # Now try with pandas (what the dashboard uses)
        df_sample = pd.read_parquet(file_path, engine='pyarrow').head(5)
        print(f"✅ Pandas read successful (read time: {time.time() - start_time:.2f}s)")
        print(f"   Sample shape: {df_sample.shape}")
        
        # Check for recent data
        if 'settlementdate' in df_sample.columns:
            latest = pd.read_parquet(file_path, columns=['settlementdate']).max()['settlementdate']
            print(f"   Latest data: {latest}")
            data_age = (datetime.now() - pd.to_datetime(latest)).total_seconds() / 60
            print(f"   Data age: {data_age:.1f} minutes")
            
            if data_age > 60:
                print(f"⚠️  WARNING: Data is more than 1 hour old")
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR reading file: {type(e).__name__}: {str(e)}")
        
        # Try to determine if file is being written
        try:
            # Wait and retry
            print("   Waiting 2 seconds and retrying...")
            time.sleep(2)
            
            # Check if file size changed
            new_size = os.path.getsize(file_path)
            if new_size != file_stats.st_size:
                print(f"⚠️  File size changed from {file_stats.st_size} to {new_size} bytes")
                print(f"   File appears to be actively written to")
            
            # Try reading again
            pq.ParquetFile(file_path)
            print("✅ Second attempt successful - file was being written")
            return True
            
        except Exception as e2:
            print(f"❌ Second attempt also failed: {str(e2)}")
            
            # Check if it's a partial write issue
            if "magic bytes" in str(e).lower():
                print("🔧 File appears to be corrupted or partially written")
                print("   This often happens when the data updater is interrupted")
            
        return False

def main():
    """Check all parquet files used by the dashboard"""
    print("AEMO Dashboard Parquet File Integrity Check")
    print(f"Timestamp: {datetime.now()}")
    
    # List of files to check based on shared_data_duckdb.py
    files_to_check = [
        # Generation data
        (config.gen_output_file, "Generation 5-minute (scada5.parquet)"),
        (str(config.gen_output_file).replace('scada5.parquet', 'scada30.parquet'), "Generation 30-minute (scada30.parquet)"),
        
        # Price data
        (config.spot_hist_file, "Prices 5-minute (prices5.parquet)"),
        (str(config.spot_hist_file).replace('prices5.parquet', 'prices30.parquet'), "Prices 30-minute (prices30.parquet)"),
        
        # Transmission data
        (config.transmission_output_file, "Transmission 5-minute (transmission5.parquet)"),
        (str(config.transmission_output_file).replace('transmission5.parquet', 'transmission30.parquet'), "Transmission 30-minute (transmission30.parquet)"),
        
        # Rooftop solar
        (config.rooftop_solar_file, "Rooftop Solar (rooftop30.parquet)"),
        
        # Reference data
        (config.gen_info_file, "Generator Info (gen_info.pkl)")
    ]
    
    # Also check for the problematic file from the error
    error_file = '/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2/scada5.parquet'
    if os.path.exists(error_file):
        files_to_check.insert(0, (error_file, "ERROR FILE - scada5.parquet in 'data 2' directory"))
    
    results = {}
    
    for file_path, description in files_to_check:
        # Handle .pkl files differently
        if str(file_path).endswith('.pkl'):
            print(f"\n{'='*60}")
            print(f"Checking: {description}")
            print(f"Path: {file_path}")
            print(f"{'='*60}")
            
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path) / (1024 * 1024)
                print(f"✅ File exists ({file_size:.2f} MB)")
                results[description] = True
            else:
                print(f"❌ FILE NOT FOUND")
                results[description] = False
        else:
            results[description] = check_parquet_file(file_path, description)
    
    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    
    successful = sum(1 for v in results.values() if v)
    total = len(results)
    
    print(f"Files checked: {total}")
    print(f"Successful: {successful}")
    print(f"Failed: {total - successful}")
    
    if total - successful > 0:
        print("\nFailed files:")
        for name, success in results.items():
            if not success:
                print(f"  - {name}")
    
    # Check for the specific issue
    if "ERROR FILE" in [k for k in results.keys()]:
        print("\n⚠️  CRITICAL ISSUE FOUND:")
        print("There is a 'data 2' directory with corrupted files!")
        print("This is likely causing the intermittent startup issues.")
        print("\nRECOMMENDED FIX:")
        print("1. Remove the 'data 2' directory")
        print("2. Update dashboard configuration to use correct data path")

if __name__ == "__main__":
    main()