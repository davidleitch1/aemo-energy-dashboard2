#!/usr/bin/env python3
"""
Simple test to verify data integration logic for station analysis
"""

import pandas as pd
import pickle
from datetime import datetime, timedelta
from pathlib import Path

def test_basic_integration():
    """Test basic data integration without complex imports"""
    print("🔍 Testing Basic Data Integration")
    print("=" * 40)
    
    # Test data loading
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)
    
    print(f"Date range: {start_date.date()} to {end_date.date()}")
    
    # Load generation data
    print("\n--- Loading Generation Data ---")
    gen_file = "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2/scada5.parquet"
    gen_data = pd.read_parquet(gen_file)
    
    # Filter to date range
    gen_filtered = gen_data[
        (gen_data['settlementdate'] >= start_date) & 
        (gen_data['settlementdate'] <= end_date)
    ]
    
    print(f"✅ Generation data: {len(gen_filtered):,} records for date range")
    print(f"   Unique DUIDs: {gen_filtered['duid'].nunique()}")
    print(f"   Sample DUIDs: {gen_filtered['duid'].unique()[:5].tolist()}")
    
    # Load price data
    print("\n--- Loading Price Data ---")
    price_file = "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2/prices5.parquet"
    price_data = pd.read_parquet(price_file)
    
    # Filter to date range
    price_filtered = price_data[
        (price_data['settlementdate'] >= start_date) & 
        (price_data['settlementdate'] <= end_date)
    ]
    
    print(f"✅ Price data: {len(price_filtered):,} records for date range")
    print(f"   Regions: {price_filtered['regionid'].unique().tolist()}")
    
    # Load DUID mapping
    print("\n--- Loading DUID Mapping ---")
    mapping_file = "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard/data/gen_info.pkl"
    
    with open(mapping_file, 'rb') as f:
        duid_mapping = pickle.load(f)
    
    print(f"✅ DUID mapping: {len(duid_mapping):,} entries")
    
    # Convert mapping to DataFrame
    if isinstance(duid_mapping, pd.DataFrame):
        duid_df = duid_mapping.copy()
        if 'DUID' not in duid_df.columns:
            duid_df = duid_df.T
            duid_df = duid_df.reset_index()
            duid_df.columns = ['DUID'] + list(duid_df.columns[1:])
    else:
        duid_df = pd.DataFrame(duid_mapping).T.reset_index()
        duid_df.columns = ['DUID'] + list(duid_df.columns[1:])
    
    print(f"   DUID DataFrame shape: {duid_df.shape}")
    print(f"   Columns: {list(duid_df.columns)}")
    
    # Test integration step by step
    print("\n--- Testing Integration ---")
    
    # Step 1: Join generation with DUID mapping
    print("Step 1: Join generation with DUID mapping...")
    gen_with_mapping = gen_filtered.merge(
        duid_df,
        left_on='duid',
        right_on='DUID',
        how='left'
    )
    
    print(f"   Result: {len(gen_with_mapping):,} records")
    print(f"   Columns: {list(gen_with_mapping.columns)}")
    
    # Check for missing regions
    missing_regions = gen_with_mapping['Region'].isna().sum()
    print(f"   Missing regions: {missing_regions:,} records")
    
    if missing_regions > 0:
        missing_duids = gen_with_mapping[gen_with_mapping['Region'].isna()]['duid'].unique()
        print(f"   Sample missing DUIDs: {missing_duids[:5].tolist()}")
    
    # Step 2: Join with price data
    print("\nStep 2: Join with price data...")
    integrated_data = gen_with_mapping.merge(
        price_filtered,
        left_on=['settlementdate', 'Region'],
        right_on=['settlementdate', 'regionid'],
        how='inner'
    )
    
    print(f"   Result: {len(integrated_data):,} records")
    print(f"   Columns: {list(integrated_data.columns)}")
    
    if len(integrated_data) == 0:
        print("❌ No integrated data - checking region matching...")
        
        # Debug region matching
        gen_regions = gen_with_mapping['Region'].dropna().unique()
        price_regions = price_filtered['regionid'].unique()
        
        print(f"   Generation regions: {gen_regions}")
        print(f"   Price regions: {price_regions}")
        
        # Check overlap
        common_regions = set(gen_regions) & set(price_regions)
        print(f"   Common regions: {list(common_regions)}")
        
        # Check time overlap
        gen_times = set(gen_with_mapping['settlementdate'].unique())
        price_times = set(price_filtered['settlementdate'].unique())
        common_times = len(gen_times & price_times)
        print(f"   Common timestamps: {common_times}")
        
    else:
        print("✅ Integration successful!")
        
        # Test specific DUID filtering
        print("\n--- Testing DUID Filtering ---")
        sample_duid = integrated_data['duid'].iloc[0]
        print(f"Testing with DUID: {sample_duid}")
        
        station_data = integrated_data[integrated_data['duid'] == sample_duid]
        print(f"   Station data: {len(station_data):,} records")
        
        if len(station_data) > 0:
            print(f"   Date range: {station_data['settlementdate'].min()} to {station_data['settlementdate'].max()}")
            print(f"   Generation range: {station_data['scadavalue'].min():.1f} to {station_data['scadavalue'].max():.1f} MW")
            print("✅ Station filtering works!")
        else:
            print("❌ No station data after filtering")

if __name__ == "__main__":
    test_basic_integration()