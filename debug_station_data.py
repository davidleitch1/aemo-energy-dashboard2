#!/usr/bin/env python3
"""
Debug script to check what data is available for Gladstone station
"""

import sys
import os
sys.path.insert(0, 'src')

from datetime import datetime
import pandas as pd
from aemo_dashboard.shared.generation_adapter import load_generation_data

def debug_gladstone_data():
    """Check what data is available for Gladstone across different time periods"""
    
    print("🔍 Debugging Gladstone Station Data Availability")
    print("=" * 60)
    
    # Load full generation data
    print("\n📊 Loading full generation data...")
    start_date = datetime(2020, 1, 1)
    end_date = datetime.now()
    
    df = load_generation_data(
        start_date=start_date,
        end_date=end_date,
        resolution='auto'
    )
    
    print(f"Total generation records: {len(df):,}")
    print(f"Date range: {df['settlementdate'].min()} to {df['settlementdate'].max()}")
    print(f"Unique DUIDs: {df['duid'].nunique():,}")
    
    # Look for Gladstone-related DUIDs
    print("\n🏭 Searching for Gladstone-related DUIDs...")
    gladstone_duids = df[df['duid'].str.contains('GLAD', case=False, na=False)]['duid'].unique()
    print(f"Gladstone DUIDs found: {list(gladstone_duids)}")
    
    if len(gladstone_duids) == 0:
        print("❌ No Gladstone DUIDs found. Checking all DUIDs containing 'G'...")
        g_duids = df[df['duid'].str.startswith('G')]['duid'].unique()
        print(f"DUIDs starting with 'G': {sorted(g_duids)[:20]}...")  # Show first 20
        return
    
    # Analyze data availability for each Gladstone DUID
    for duid in gladstone_duids:
        print(f"\n📈 Analyzing {duid}:")
        duid_data = df[df['duid'] == duid].copy()
        
        if len(duid_data) == 0:
            print(f"   ❌ No data found for {duid}")
            continue
            
        print(f"   Records: {len(duid_data):,}")
        print(f"   Date range: {duid_data['settlementdate'].min()} to {duid_data['settlementdate'].max()}")
        print(f"   Generation range: {duid_data['scadavalue'].min():.1f} to {duid_data['scadavalue'].max():.1f} MW")
        
        # Check data availability by year
        duid_data['year'] = duid_data['settlementdate'].dt.year
        yearly_counts = duid_data.groupby('year').size()
        print(f"   Data by year: {yearly_counts.to_dict()}")
        
        # Check recent vs historical data
        recent_cutoff = datetime(2025, 6, 1)
        recent_data = duid_data[duid_data['settlementdate'] >= recent_cutoff]
        historical_data = duid_data[duid_data['settlementdate'] < recent_cutoff]
        
        print(f"   Recent data (Jun 2025+): {len(recent_data):,} records")
        print(f"   Historical data (pre-Jun 2025): {len(historical_data):,} records")
        
        if len(historical_data) == 0:
            print(f"   ⚠️  No historical data for {duid} - this explains the Jun 2025 limitation!")

def check_station_duid_mapping():
    """Check if Gladstone station DUID mapping is correct"""
    
    print("\n🗂️ Checking DUID mapping file...")
    
    try:
        import pickle
        from aemo_dashboard.shared.config import config
        
        with open(config.gen_info_file, 'rb') as f:
            duid_mapping = pickle.load(f)
        
        print(f"Total DUIDs in mapping: {len(duid_mapping)}")
        
        # Look for Gladstone in mapping
        gladstone_stations = {}
        for duid, info in duid_mapping.items():
            if 'glad' in str(info).lower() or 'glad' in duid.lower():
                gladstone_stations[duid] = info
        
        print(f"\nGladstone entries in mapping:")
        for duid, info in gladstone_stations.items():
            print(f"  {duid}: {info}")
            
    except Exception as e:
        print(f"Error loading DUID mapping: {e}")

if __name__ == '__main__':
    try:
        debug_gladstone_data()
        check_station_duid_mapping()
        
    except KeyboardInterrupt:
        print("\n🛑 Debug interrupted by user")
    except Exception as e:
        print(f"\n❌ Debug failed: {e}")
        import traceback
        traceback.print_exc()