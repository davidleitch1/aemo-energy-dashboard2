#!/usr/bin/env python3
"""
Check South Australian battery storage data for positive and negative values.
Analyzes past month of data to verify charging (negative) and discharging (positive) patterns.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import pickle

# Production data path
DATA_PATH = Path("/Volumes/davidleitch/aemo_production/data")

def check_battery_data():
    """Check SA battery data for positive and negative values over past month"""
    
    print("Checking SA battery storage data for charging/discharging patterns...")
    print(f"Data path: {DATA_PATH}")
    
    # Check if path exists
    if not DATA_PATH.exists():
        print(f"ERROR: Data path {DATA_PATH} does not exist")
        return None
    
    # Calculate date range - past 30 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    try:
        # Load generator info to identify battery storage DUIDs
        gen_info_file = DATA_PATH / "gen_info.pkl"
        print(f"Loading generator info from {gen_info_file}...")
        with open(gen_info_file, 'rb') as f:
            gen_info_df = pickle.load(f)
        
        # Filter for SA battery storage units
        sa_batteries = gen_info_df[
            (gen_info_df['Region'] == 'SA1') & 
            (gen_info_df['Fuel'] == 'Battery Storage')
        ].copy()
        
        battery_duids = sa_batteries['DUID'].tolist()
        print(f"\nFound {len(battery_duids)} SA battery storage units:")
        for duid in battery_duids:
            site_name = sa_batteries[sa_batteries['DUID'] == duid]['Site Name'].values[0]
            print(f"  - {duid}: {site_name}")
        
        # Load 30-minute generation data
        gen_file = DATA_PATH / "scada30.parquet"
        print(f"\nLoading generation data from {gen_file}...")
        gen_data = pd.read_parquet(gen_file)
        
        # Filter for battery DUIDs and date range
        gen_data['settlementdate'] = pd.to_datetime(gen_data['settlementdate'])
        battery_data = gen_data[
            (gen_data['duid'].isin(battery_duids)) & 
            (gen_data['settlementdate'] >= start_date) &
            (gen_data['settlementdate'] <= end_date)
        ].copy()
        
        print(f"\nFound {len(battery_data)} battery records for past month")
        
        # Analyze each battery DUID
        print("\n" + "="*60)
        print("BATTERY STORAGE ANALYSIS BY DUID")
        print("="*60)
        
        for duid in battery_duids:
            duid_data = battery_data[battery_data['duid'] == duid]['scadavalue']
            if len(duid_data) > 0:
                site_name = sa_batteries[sa_batteries['DUID'] == duid]['Site Name'].values[0]
                
                positive_vals = duid_data[duid_data > 0]
                negative_vals = duid_data[duid_data < 0]
                zero_vals = duid_data[duid_data == 0]
                
                print(f"\n{duid} ({site_name}):")
                print(f"  Total periods: {len(duid_data)}")
                print(f"  Positive (discharge): {len(positive_vals)} periods ({len(positive_vals)/len(duid_data)*100:.1f}%)")
                if len(positive_vals) > 0:
                    print(f"    Range: {positive_vals.min():.1f} to {positive_vals.max():.1f} MW")
                    print(f"    Mean: {positive_vals.mean():.1f} MW")
                
                print(f"  Negative (charging): {len(negative_vals)} periods ({len(negative_vals)/len(duid_data)*100:.1f}%)")
                if len(negative_vals) > 0:
                    print(f"    Range: {negative_vals.min():.1f} to {negative_vals.max():.1f} MW")
                    print(f"    Mean: {negative_vals.mean():.1f} MW")
                
                print(f"  Zero: {len(zero_vals)} periods ({len(zero_vals)/len(duid_data)*100:.1f}%)")
        
        # Overall battery storage summary
        print("\n" + "="*60)
        print("OVERALL SA BATTERY STORAGE SUMMARY")
        print("="*60)
        
        all_battery_values = battery_data['scadavalue']
        positive_all = all_battery_values[all_battery_values > 0]
        negative_all = all_battery_values[all_battery_values < 0]
        zero_all = all_battery_values[all_battery_values == 0]
        
        print(f"Total battery records: {len(all_battery_values)}")
        print(f"Positive values: {len(positive_all)} ({len(positive_all)/len(all_battery_values)*100:.1f}%)")
        print(f"Negative values: {len(negative_all)} ({len(negative_all)/len(all_battery_values)*100:.1f}%)")
        print(f"Zero values: {len(zero_all)} ({len(zero_all)/len(all_battery_values)*100:.1f}%)")
        
        if len(negative_all) > 0:
            print(f"\nCharging (negative) statistics:")
            print(f"  Min (max charging): {negative_all.min():.1f} MW")
            print(f"  Max (min charging): {negative_all.max():.1f} MW")
            print(f"  Mean charging: {negative_all.mean():.1f} MW")
            print(f"  Total energy charged: {negative_all.sum()/2:.1f} MWh")
        
        if len(positive_all) > 0:
            print(f"\nDischarging (positive) statistics:")
            print(f"  Min discharge: {positive_all.min():.1f} MW")
            print(f"  Max discharge: {positive_all.max():.1f} MW")
            print(f"  Mean discharge: {positive_all.mean():.1f} MW")
            print(f"  Total energy discharged: {positive_all.sum()/2:.1f} MWh")
        
        # Daily pattern analysis
        print("\n" + "="*60)
        print("DAILY CHARGE/DISCHARGE PATTERN")
        print("="*60)
        
        battery_data['date'] = battery_data['settlementdate'].dt.date
        daily_summary = battery_data.groupby('date')['scadavalue'].agg([
            ('positive_sum', lambda x: x[x > 0].sum()),
            ('negative_sum', lambda x: x[x < 0].sum()),
            ('positive_count', lambda x: (x > 0).sum()),
            ('negative_count', lambda x: (x < 0).sum())
        ])
        
        # Show first and last 5 days
        print("\nFirst 5 days:")
        print(daily_summary.head().to_string())
        
        print("\nLast 5 days:")
        print(daily_summary.tail().to_string())
        
        # Check for days without charging
        days_without_charging = daily_summary[daily_summary['negative_count'] == 0]
        if len(days_without_charging) > 0:
            print(f"\n⚠️ WARNING: {len(days_without_charging)} days without any charging activity!")
            print("Days without charging:")
            for date in days_without_charging.index:
                print(f"  - {date}")
        
        return battery_data
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    data = check_battery_data()
    if data is not None:
        print("\nAnalysis complete!")
    else:
        print("\nAnalysis failed!")