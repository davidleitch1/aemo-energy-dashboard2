#!/usr/bin/env python3
"""
Check freshness and consistency of AEMO data files
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import sys

# Data path
data_path = Path('/Volumes/davidleitch/aemo_production/data')

def check_file_freshness(filename):
    """Check a parquet file's data freshness"""
    file_path = data_path / filename
    
    if not file_path.exists():
        return None
        
    # Get file modification time
    file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
    
    # Read last few rows
    df = pd.read_parquet(file_path)
    
    if 'settlementdate' in df.columns:
        df['settlementdate'] = pd.to_datetime(df['settlementdate'])
        latest_data = df['settlementdate'].max()
        record_count = len(df)
    else:
        latest_data = None
        record_count = len(df)
    
    return {
        'file_modified': file_mtime,
        'latest_data': latest_data,
        'record_count': record_count,
        'file_size_mb': file_path.stat().st_size / (1024 * 1024)
    }

def main():
    print("="*80)
    print("AEMO DATA FRESHNESS CHECK")
    print(f"Current time: {datetime.now()}")
    print("="*80)
    
    files_to_check = [
        'prices5.parquet',
        'prices30.parquet',
        'scada5.parquet', 
        'scada30.parquet',
        'transmission5.parquet',
        'transmission30.parquet',
        'rooftop30.parquet'
    ]
    
    now = datetime.now()
    
    for filename in files_to_check:
        print(f"\n{filename}:")
        info = check_file_freshness(filename)
        
        if info:
            print(f"  File modified: {info['file_modified']}")
            print(f"  File age: {(now - info['file_modified']).total_seconds() / 60:.1f} minutes")
            
            if info['latest_data']:
                print(f"  Latest data: {info['latest_data']}")
                data_age = (now - info['latest_data']).total_seconds() / 60
                print(f"  Data age: {data_age:.1f} minutes")
                
                # Check if data is stale (more than 15 minutes old)
                if data_age > 15:
                    print(f"  ⚠️ WARNING: Data is {data_age:.1f} minutes old")
                else:
                    print(f"  ✅ Data is fresh")
                    
            print(f"  Records: {info['record_count']:,}")
            print(f"  Size: {info['file_size_mb']:.1f} MB")
        else:
            print("  ❌ File not found")
    
    # Check for consistency between 5-min and 30-min files
    print("\n" + "="*80)
    print("CONSISTENCY CHECKS")
    print("="*80)
    
    # Check prices
    prices5_info = check_file_freshness('prices5.parquet')
    prices30_info = check_file_freshness('prices30.parquet')
    
    if prices5_info and prices30_info:
        time_diff = abs((prices5_info['latest_data'] - prices30_info['latest_data']).total_seconds() / 60)
        print(f"\nPrice data time difference: {time_diff:.1f} minutes")
        if time_diff > 30:
            print("  ⚠️ WARNING: Large time difference between 5-min and 30-min prices")
    
    # Check SCADA
    scada5_info = check_file_freshness('scada5.parquet')
    scada30_info = check_file_freshness('scada30.parquet')
    
    if scada5_info and scada30_info:
        time_diff = abs((scada5_info['latest_data'] - scada30_info['latest_data']).total_seconds() / 60)
        print(f"\nSCADA data time difference: {time_diff:.1f} minutes")
        if time_diff > 30:
            print("  ⚠️ WARNING: Large time difference between 5-min and 30-min SCADA")
            
    # Check overall system health
    print("\n" + "="*80)
    print("OVERALL STATUS")
    print("="*80)
    
    all_fresh = True
    for filename in ['prices5.parquet', 'scada5.parquet']:
        info = check_file_freshness(filename)
        if info and info['latest_data']:
            data_age = (now - info['latest_data']).total_seconds() / 60
            if data_age > 15:
                all_fresh = False
                break
    
    if all_fresh:
        print("✅ System appears to be running normally")
    else:
        print("⚠️ Some data appears stale - collector may need attention")

if __name__ == "__main__":
    main()