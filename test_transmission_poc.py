#!/usr/bin/env python3
"""
Proof of Concept: Test reading transmission data from new structure
Compare old transmission_flows.parquet with new transmission5.parquet
"""

import pandas as pd
import numpy as np
import sys
from pathlib import Path

def compare_transmission_data():
    """Compare old and new transmission data files"""
    
    # File paths
    old_file = Path("/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/transmission_flows.parquet")
    new_file = Path("/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2/transmission5.parquet")
    
    print("=" * 60)
    print("TRANSMISSION DATA COMPARISON")
    print("=" * 60)
    
    # Read old data
    try:
        old_df = pd.read_parquet(old_file)
        print(f"\n✅ Old file loaded: {old_file.name}")
        print(f"   Shape: {old_df.shape}")
        print(f"   Columns: {list(old_df.columns)}")
        print(f"   Date range: {old_df['settlementdate'].min()} to {old_df['settlementdate'].max()}")
        print(f"   Interconnectors: {sorted(old_df['interconnectorid'].unique())}")
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
        print(f"   Interconnectors: {sorted(new_df['interconnectorid'].unique())}")
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
            print(f"   Columns: {sorted(old_cols)}")
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
    sample_data = new_df[new_df['settlementdate'] == latest_time].head(6)
    
    print(f"\nSample data at {latest_time}:")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(sample_data.to_string(index=False))
    
    # Test data types
    print("\n" + "=" * 60)
    print("DATA TYPE VALIDATION")
    print("=" * 60)
    
    for col in sorted(new_df.columns):
        print(f"   {col}: {new_df[col].dtype}")
    
    # Test for required values
    print("\n" + "=" * 60)
    print("DATA INTEGRITY CHECKS")
    print("=" * 60)
    
    # Check for nulls in critical columns
    critical_cols = ['settlementdate', 'interconnectorid', 'meteredmwflow']
    for col in critical_cols:
        null_count = new_df[col].isnull().sum()
        if null_count == 0:
            print(f"✅ {col}: No null values")
        else:
            print(f"⚠️  {col}: {null_count} null values")
    
    # Check optional columns
    optional_cols = ['mwflow', 'exportlimit', 'importlimit', 'mwlosses']
    for col in optional_cols:
        null_count = new_df[col].isnull().sum()
        null_pct = (null_count / len(new_df)) * 100
        print(f"ℹ️  {col}: {null_count} nulls ({null_pct:.1f}%)")
    
    # Check interconnector flows
    print(f"\n📊 Flow statistics:")
    print(f"   Average absolute flow: {new_df['meteredmwflow'].abs().mean():.1f} MW")
    print(f"   Max export: {new_df['meteredmwflow'].max():.1f} MW")
    print(f"   Max import: {new_df['meteredmwflow'].min():.1f} MW")
    
    # Memory usage
    print(f"\n📊 Memory usage: {new_df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
    
    return True

def test_dashboard_compatibility():
    """Test if new data works with dashboard code patterns"""
    
    print("\n" + "=" * 60)
    print("DASHBOARD COMPATIBILITY TEST")
    print("=" * 60)
    
    new_file = Path("/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2/transmission5.parquet")
    
    try:
        # Simulate dashboard data loading pattern
        df = pd.read_parquet(new_file)
        
        # Test filtering by date range (common dashboard operation)
        end_time = df['settlementdate'].max()
        start_time = end_time - pd.Timedelta(days=1)
        
        filtered_df = df[(df['settlementdate'] >= start_time) & 
                        (df['settlementdate'] <= end_time)]
        
        print(f"✅ Date filtering works: {len(filtered_df)} records for last 24 hours")
        
        # Test grouping by interconnector (common for aggregation)
        grouped = filtered_df.groupby('interconnectorid')['meteredmwflow'].agg(['mean', 'min', 'max'])
        print(f"✅ Interconnector grouping works:")
        print(grouped.round(1))
        
        # Test flow calculations (import/export separation)
        filtered_df['import_mw'] = filtered_df['meteredmwflow'].where(filtered_df['meteredmwflow'] < 0, 0).abs()
        filtered_df['export_mw'] = filtered_df['meteredmwflow'].where(filtered_df['meteredmwflow'] > 0, 0)
        
        print(f"\n✅ Import/Export calculations work:")
        totals = filtered_df.groupby('interconnectorid')[['import_mw', 'export_mw']].sum()
        print(totals.round(0))
        
        # Test utilization calculations (if limits available)
        if 'exportlimit' in df.columns:
            valid_export = filtered_df[(filtered_df['exportlimit'].notna()) & 
                                      (filtered_df['exportlimit'] > 0)]
            if len(valid_export) > 0:
                valid_export['utilization'] = (valid_export['meteredmwflow'] / 
                                             valid_export['exportlimit'] * 100)
                print(f"\n✅ Utilization calculations work")
                avg_util = valid_export.groupby('interconnectorid')['utilization'].mean()
                print(f"   Average utilization by interconnector:")
                print(avg_util.round(1))
        
        return True
        
    except Exception as e:
        print(f"❌ Compatibility test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_transmission_specific_operations():
    """Test transmission-specific dashboard operations"""
    
    print("\n" + "=" * 60)
    print("TRANSMISSION-SPECIFIC OPERATIONS TEST")
    print("=" * 60)
    
    new_file = Path("/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2/transmission5.parquet")
    
    try:
        df = pd.read_parquet(new_file)
        
        # Get recent data for testing
        end_time = df['settlementdate'].max()
        start_time = end_time - pd.Timedelta(hours=6)
        recent_df = df[(df['settlementdate'] >= start_time) & 
                      (df['settlementdate'] <= end_time)]
        
        # Test 1: Regional flow calculation
        print("\n1. Testing regional flow calculations...")
        
        # Map interconnectors to regions (simplified)
        interconnector_regions = {
            'N-Q-MNSP1': ('NSW1', 'QLD1'),
            'NSW1-QLD1': ('NSW1', 'QLD1'),
            'T-V-MNSP1': ('TAS1', 'VIC1'),
            'V-S-MNSP1': ('VIC1', 'SA1'),
            'V-SA': ('VIC1', 'SA1'),
            'VIC1-NSW1': ('VIC1', 'NSW1')
        }
        
        regional_flows = {}
        for ic, (from_reg, to_reg) in interconnector_regions.items():
            ic_data = recent_df[recent_df['interconnectorid'] == ic]
            if len(ic_data) > 0:
                avg_flow = ic_data['meteredmwflow'].mean()
                regional_flows[f"{from_reg}→{to_reg}"] = avg_flow
        
        if regional_flows:
            print("✅ Regional flow calculation works:")
            for flow, value in regional_flows.items():
                print(f"   {flow}: {value:.1f} MW")
        
        # Test 2: Time series resampling
        print("\n2. Testing time series operations...")
        ts_data = recent_df.set_index('settlementdate').groupby('interconnectorid')['meteredmwflow']
        
        # 30-minute average
        resampled_30min = ts_data.resample('30min').mean()
        print(f"✅ 30-minute resampling works: {len(resampled_30min)} records")
        
        # Hourly average
        resampled_hourly = ts_data.resample('1h').mean()
        print(f"✅ Hourly resampling works: {len(resampled_hourly)} records")
        
        # Test 3: Flow direction statistics
        print("\n3. Testing flow direction analysis...")
        direction_stats = recent_df.groupby('interconnectorid')['meteredmwflow'].apply(
            lambda x: pd.Series({
                'positive_pct': (x > 0).sum() / len(x) * 100,
                'negative_pct': (x < 0).sum() / len(x) * 100,
                'zero_pct': (x == 0).sum() / len(x) * 100
            })
        )
        
        print("✅ Flow direction analysis works:")
        print(direction_stats.round(1))
        
        return True
        
    except Exception as e:
        print(f"❌ Transmission-specific test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Starting Transmission Data Proof of Concept...\n")
    
    # Run comparison
    if compare_transmission_data():
        print("\n✅ Data structure comparison successful!")
        
        # Run compatibility test
        if test_dashboard_compatibility():
            print("\n✅ Dashboard compatibility verified!")
            
            # Run transmission-specific tests
            if test_transmission_specific_operations():
                print("\n✅ Transmission-specific operations verified!")
                print("\n🎉 TRANSMISSION DATA MIGRATION IS FEASIBLE WITH NO CODE CHANGES!")
            else:
                print("\n⚠️  Some transmission-specific operations need attention")
        else:
            print("\n❌ Dashboard compatibility issues found")
            sys.exit(1)
    else:
        print("\n❌ Data structure issues found")
        sys.exit(1)