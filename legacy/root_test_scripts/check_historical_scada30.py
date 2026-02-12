#!/usr/bin/env python3
"""
Check historical scada30 data for negative values (battery charging).
Focus on data from approximately 1 year ago.
"""

import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# Production data path
DATA_PATH = Path("/Volumes/davidleitch/aemo_production/data")

def check_historical_negative_values():
    """Check if historical scada30 data contains negative values"""
    
    print("="*60)
    print("Checking Historical SCADA30 for Negative Values")
    print("="*60)
    
    # Load scada30 data
    scada30_file = DATA_PATH / "scada30.parquet"
    
    if not scada30_file.exists():
        print(f"ERROR: {scada30_file} not found")
        return
    
    print(f"\nLoading scada30 data from {scada30_file}...")
    df = pd.read_parquet(scada30_file)
    
    # Convert settlementdate to datetime if needed
    df['settlementdate'] = pd.to_datetime(df['settlementdate'])
    
    # Get data overview
    print(f"\nData Overview:")
    print(f"Total records: {len(df):,}")
    print(f"Date range: {df['settlementdate'].min()} to {df['settlementdate'].max()}")
    print(f"Unique DUIDs: {df['duid'].nunique()}")
    
    # Check for any negative values in entire dataset
    negative_mask = df['scadavalue'] < 0
    negative_count = negative_mask.sum()
    
    print(f"\n" + "="*60)
    print(f"OVERALL NEGATIVE VALUES")
    print("="*60)
    print(f"Total negative records: {negative_count:,} ({negative_count/len(df)*100:.2f}%)")
    
    if negative_count > 0:
        negative_df = df[negative_mask]
        print(f"Negative value range: {negative_df['scadavalue'].min():.2f} to {negative_df['scadavalue'].max():.2f} MW")
        print(f"Unique DUIDs with negative values: {negative_df['duid'].nunique()}")
        
        # Show DUIDs with most negative values
        print("\nTop 10 DUIDs by negative value count:")
        top_negative = negative_df['duid'].value_counts().head(10)
        for duid, count in top_negative.items():
            print(f"  {duid}: {count:,} records")
    
    # Check data from different time periods
    print(f"\n" + "="*60)
    print("NEGATIVE VALUES BY TIME PERIOD")
    print("="*60)
    
    # Define time periods to check
    periods = [
        ("Last 30 days", 30),
        ("6 months ago", 180),
        ("1 year ago", 365),
        ("2 years ago", 730)
    ]
    
    current_date = df['settlementdate'].max()
    
    for period_name, days_ago in periods:
        target_date = current_date - timedelta(days=days_ago)
        start_date = target_date - timedelta(days=7)  # Check a week of data
        end_date = target_date + timedelta(days=7)
        
        period_mask = (df['settlementdate'] >= start_date) & (df['settlementdate'] <= end_date)
        period_df = df[period_mask]
        
        if len(period_df) > 0:
            period_negative = period_df[period_df['scadavalue'] < 0]
            print(f"\n{period_name} (around {target_date.date()}):")
            print(f"  Total records: {len(period_df):,}")
            print(f"  Negative records: {len(period_negative):,} ({len(period_negative)/len(period_df)*100:.2f}%)")
            
            if len(period_negative) > 0:
                print(f"  Min value: {period_negative['scadavalue'].min():.2f} MW")
                print(f"  DUIDs with negatives: {period_negative['duid'].nunique()}")
        else:
            print(f"\n{period_name}: No data available")
    
    # Check specific battery DUIDs
    print(f"\n" + "="*60)
    print("KNOWN BATTERY DUIDS CHECK")
    print("="*60)
    
    battery_duids = [
        'HPR1',      # Hornsdale Power Reserve
        'DALNTH1',   # Dalrymple BESS
        'LBB1',      # Lake Bonney
        'TIB1',      # Torrens Island
        'TB2B1',     # Tailem Bend
        'BLYTHB1',   # Blyth
        'BALBG1',    # Ballarat
        'GANNBG1',   # Gannawarra
        'WGBG1',     # Wallgrove
        'VICBG1'     # Victorian Big Battery
    ]
    
    for duid in battery_duids:
        duid_df = df[df['duid'] == duid]
        if len(duid_df) > 0:
            negative_duid = duid_df[duid_df['scadavalue'] < 0]
            positive_duid = duid_df[duid_df['scadavalue'] > 0]
            zero_duid = duid_df[duid_df['scadavalue'] == 0]
            
            print(f"\n{duid}:")
            print(f"  Total records: {len(duid_df):,}")
            print(f"  Positive (discharge): {len(positive_duid):,} ({len(positive_duid)/len(duid_df)*100:.1f}%)")
            print(f"  Negative (charging): {len(negative_duid):,} ({len(negative_duid)/len(duid_df)*100:.1f}%)")
            print(f"  Zero: {len(zero_duid):,} ({len(zero_duid)/len(duid_df)*100:.1f}%)")
            
            if len(negative_duid) > 0:
                print(f"  ✅ HAS CHARGING DATA!")
                print(f"  Charging range: {negative_duid['scadavalue'].min():.1f} to {negative_duid['scadavalue'].max():.1f} MW")
                print(f"  Date of first negative: {negative_duid['settlementdate'].min()}")
                print(f"  Date of last negative: {negative_duid['settlementdate'].max()}")
            else:
                print(f"  ❌ NO CHARGING DATA FOUND")
    
    # Check when negative values stop appearing
    if negative_count > 0:
        print(f"\n" + "="*60)
        print("NEGATIVE VALUE TIMELINE")
        print("="*60)
        
        negative_df = df[df['scadavalue'] < 0].copy()
        negative_df['date'] = negative_df['settlementdate'].dt.date
        
        # Group by date and count
        daily_negatives = negative_df.groupby('date').size()
        
        print(f"\nFirst date with negatives: {daily_negatives.index.min()}")
        print(f"Last date with negatives: {daily_negatives.index.max()}")
        
        # Check if there's a clear cutoff
        if daily_negatives.index.max() < current_date.date() - timedelta(days=30):
            print(f"\n⚠️ WARNING: No negative values in the last 30 days!")
            print(f"Negative values stopped on: {daily_negatives.index.max()}")
            
            # Show the transition period
            transition_date = daily_negatives.index.max()
            print(f"\nDaily negative record counts around transition:")
            for i in range(-5, 6):
                check_date = transition_date + timedelta(days=i)
                count = daily_negatives.get(check_date, 0)
                print(f"  {check_date}: {count:,} negative records")

if __name__ == "__main__":
    check_historical_negative_values()