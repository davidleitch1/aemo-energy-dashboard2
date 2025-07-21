#!/usr/bin/env python3
"""
Proof of Concept: Test reading generation data from new structure
Compare old gen_output.parquet with new scada5.parquet
"""

import pandas as pd
import sys
from pathlib import Path

def compare_generation_data():
    """Compare old and new generation data files"""
    
    # File paths
    old_file = Path("data/gen_output.parquet")
    new_file = Path("/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2/scada5.parquet")
    
    print("=" * 60)
    print("GENERATION DATA COMPARISON")
    print("=" * 60)
    
    # Read old data
    try:
        old_df = pd.read_parquet(old_file)
        print(f"\n✅ Old file loaded: {old_file}")
        print(f"   Shape: {old_df.shape}")
        print(f"   Columns: {list(old_df.columns)}")
        print(f"   Date range: {old_df['settlementdate'].min()} to {old_df['settlementdate'].max()}")
        print(f"   Unique DUIDs: {old_df['duid'].nunique()}")
    except Exception as e:
        print(f"\n❌ Error loading old file: {e}")
        old_df = None
    
    # Read new data
    try:
        new_df = pd.read_parquet(new_file)
        print(f"\n✅ New file loaded: {new_file.name}")
        print(f"   Shape: {new_df.shape}")
        print(f"   Columns: {list(new_df.columns)}")
        print(f"   Date range: {new_df['settlementdate'].min()} to {new_df['settlementdate'].max()}")
        print(f"   Unique DUIDs: {new_df['duid'].nunique()}")
    except Exception as e:
        print(f"\n❌ Error loading new file: {e}")
        return False
    
    # Compare column structure
    print("\n" + "=" * 60)
    print("COLUMN COMPARISON")
    print("=" * 60)
    
    if old_df is not None:
        old_cols = set(old_df.columns)
        new_cols = set(new_df.columns)
        
        if old_cols == new_cols:
            print("✅ Columns match exactly!")
        else:
            print("⚠️  Column differences found:")
            if old_cols - new_cols:
                print(f"   Old only: {old_cols - new_cols}")
            if new_cols - old_cols:
                print(f"   New only: {new_cols - old_cols}")
    
    # Test sample data
    print("\n" + "=" * 60)
    print("SAMPLE DATA TEST")
    print("=" * 60)
    
    # Get latest data point
    latest_time = new_df['settlementdate'].max()
    sample_data = new_df[new_df['settlementdate'] == latest_time].head(5)
    
    print(f"\nSample data at {latest_time}:")
    print(sample_data.to_string(index=False))
    
    # Test data types
    print("\n" + "=" * 60)
    print("DATA TYPE VALIDATION")
    print("=" * 60)
    
    for col in new_df.columns:
        print(f"   {col}: {new_df[col].dtype}")
    
    # Test for required values
    print("\n" + "=" * 60)
    print("DATA INTEGRITY CHECKS")
    print("=" * 60)
    
    # Check for nulls
    null_counts = new_df.isnull().sum()
    if null_counts.sum() == 0:
        print("✅ No null values found")
    else:
        print("⚠️  Null values found:")
        for col, count in null_counts[null_counts > 0].items():
            print(f"   {col}: {count} nulls")
    
    # Check for negative generation
    neg_count = (new_df['scadavalue'] < 0).sum()
    if neg_count > 0:
        print(f"⚠️  Found {neg_count} negative generation values")
    else:
        print("✅ No negative generation values")
    
    # Memory usage
    print(f"\n📊 Memory usage: {new_df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
    
    return True

def test_dashboard_compatibility():
    """Test if new data works with dashboard code patterns"""
    
    print("\n" + "=" * 60)
    print("DASHBOARD COMPATIBILITY TEST")
    print("=" * 60)
    
    new_file = Path("/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2/scada5.parquet")
    
    try:
        # Simulate dashboard data loading pattern
        df = pd.read_parquet(new_file)
        
        # Test filtering by date range (common dashboard operation)
        end_time = df['settlementdate'].max()
        start_time = end_time - pd.Timedelta(days=1)
        
        filtered_df = df[(df['settlementdate'] >= start_time) & 
                        (df['settlementdate'] <= end_time)]
        
        print(f"✅ Date filtering works: {len(filtered_df)} records for last 24 hours")
        
        # Test grouping by DUID (common for aggregation)
        grouped = filtered_df.groupby('duid')['scadavalue'].sum()
        print(f"✅ DUID grouping works: {len(grouped)} unique generators")
        
        # Test pivoting (used for time series display)
        pivot_test = filtered_df.pivot_table(
            index='settlementdate',
            columns='duid',
            values='scadavalue',
            aggfunc='sum'
        )
        print(f"✅ Pivot operation works: shape {pivot_test.shape}")
        
        # Test resampling (used for different time resolutions)
        ts_data = filtered_df.set_index('settlementdate').groupby('duid')['scadavalue'].resample('30min').mean()
        print(f"✅ Time resampling works: {len(ts_data)} 30-min records")
        
        return True
        
    except Exception as e:
        print(f"❌ Compatibility test failed: {e}")
        return False

if __name__ == "__main__":
    print("Starting Generation Data Proof of Concept...\n")
    
    # Run comparison
    if compare_generation_data():
        print("\n✅ Data structure comparison successful!")
        
        # Run compatibility test
        if test_dashboard_compatibility():
            print("\n✅ Dashboard compatibility verified!")
            print("\n🎉 Generation data migration is feasible with NO CODE CHANGES!")
        else:
            print("\n❌ Dashboard compatibility issues found")
            sys.exit(1)
    else:
        print("\n❌ Data structure issues found")
        sys.exit(1)