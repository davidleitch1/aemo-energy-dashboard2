#!/usr/bin/env python3
"""
Recalculate renewable energy records from historical data
First from 30-minute data (5+ years), then check against recent 5-minute data
"""

import os
import sys
import pandas as pd
import numpy as np
import pickle
import json
from pathlib import Path
from datetime import datetime, timedelta

# Add the src directory to path
sys.path.insert(0, 'src')

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Renewable fuel types - including both Water and Hydro as they're the same
RENEWABLE_FUELS = ['Wind', 'Solar', 'Water', 'Hydro', 'Rooftop Solar', 'Biomass']
# Excluded fuel types
EXCLUDED_FUELS = ['Battery Storage', 'Transmission Flow']

def load_30min_generation_data():
    """Load 30-minute generation data"""
    gen_30min_path = os.getenv('GEN_OUTPUT_FILE')
    if '30' not in gen_30min_path:
        gen_30min_path = gen_30min_path.replace('scada5.parquet', 'scada30.parquet')
    
    print(f"Loading 30-minute generation data from: {gen_30min_path}")
    df = pd.read_parquet(gen_30min_path)
    df['settlementdate'] = pd.to_datetime(df['settlementdate'])
    return df

def load_5min_generation_data():
    """Load 5-minute generation data"""
    gen_5min_path = os.getenv('GEN_OUTPUT_FILE_5MIN')
    if not gen_5min_path:
        gen_5min_path = os.getenv('GEN_OUTPUT_FILE').replace('scada30.parquet', 'scada5.parquet')
    
    print(f"Loading 5-minute generation data from: {gen_5min_path}")
    df = pd.read_parquet(gen_5min_path)
    df['settlementdate'] = pd.to_datetime(df['settlementdate'])
    return df

def load_rooftop_data():
    """Load rooftop solar data"""
    rooftop_path = os.getenv('ROOFTOP_SOLAR_FILE')
    print(f"Loading rooftop data from: {rooftop_path}")
    df = pd.read_parquet(rooftop_path)
    df['settlementdate'] = pd.to_datetime(df['settlementdate'])
    return df

def load_duid_mapping():
    """Load DUID to fuel type mapping"""
    gen_info_path = os.getenv('GEN_INFO_FILE')
    with open(gen_info_path, 'rb') as f:
        gen_info = pickle.load(f)
    return gen_info.set_index('DUID')['Fuel'].to_dict()

def calculate_renewable_stats(gen_df, rooftop_df, duid_to_fuel, resolution='30min'):
    """
    Calculate renewable statistics for each timestamp
    Returns DataFrame with renewable %, individual fuel contributions
    """
    # Map fuel types
    gen_df['fuel_type'] = gen_df['duid'].map(duid_to_fuel)
    
    # Exclude battery storage and transmission
    gen_df = gen_df[~gen_df['fuel_type'].isin(EXCLUDED_FUELS)]
    
    # Group by timestamp and fuel type
    fuel_by_time = gen_df.groupby(['settlementdate', 'fuel_type'])['scadavalue'].sum().unstack(fill_value=0)
    
    # Add rooftop data
    if resolution == '30min':
        # Rooftop is already 30-minute, just pivot and align
        rooftop_pivot = rooftop_df.pivot(
            index='settlementdate',
            columns='regionid',
            values='power'
        ).sum(axis=1)  # Sum all regions
        
        # Align with generation data
        rooftop_aligned = rooftop_pivot.reindex(fuel_by_time.index, fill_value=0)
        fuel_by_time['Rooftop Solar'] = rooftop_aligned
    else:
        # For 5-minute data, we'll need to interpolate rooftop
        # For now, we'll skip rooftop for 5-minute analysis
        print("Note: Rooftop interpolation for 5-minute data not implemented yet")
        fuel_by_time['Rooftop Solar'] = 0
    
    # Calculate totals
    results = pd.DataFrame(index=fuel_by_time.index)
    
    # Individual renewable contributions
    results['wind_mw'] = fuel_by_time.get('Wind', 0)
    results['solar_mw'] = fuel_by_time.get('Solar', 0)
    results['rooftop_mw'] = fuel_by_time.get('Rooftop Solar', 0)
    results['water_mw'] = fuel_by_time.get('Water', 0) + fuel_by_time.get('Hydro', 0)  # Combine Water and Hydro
    results['biomass_mw'] = fuel_by_time.get('Biomass', 0)
    
    # Total renewable
    renewable_cols = []
    for fuel in RENEWABLE_FUELS:
        if fuel in fuel_by_time.columns:
            renewable_cols.append(fuel)
    
    results['renewable_mw'] = fuel_by_time[renewable_cols].sum(axis=1)
    results['total_mw'] = fuel_by_time.sum(axis=1)
    results['renewable_pct'] = (results['renewable_mw'] / results['total_mw'] * 100).round(2)
    
    # Add hour for hourly records
    results['hour'] = results.index.hour
    
    return results

def find_records(stats_df, resolution='30min'):
    """Find all records from the statistics DataFrame"""
    records = {
        'all_time': {
            'renewable_pct': {'value': 0, 'timestamp': None},
            'wind_mw': {'value': 0, 'timestamp': None},
            'solar_mw': {'value': 0, 'timestamp': None},
            'rooftop_mw': {'value': 0, 'timestamp': None},
            'water_mw': {'value': 0, 'timestamp': None},
            'biomass_mw': {'value': 0, 'timestamp': None}
        },
        'hourly': {}
    }
    
    # Find all-time records
    for metric in ['renewable_pct', 'wind_mw', 'solar_mw', 'rooftop_mw', 'water_mw', 'biomass_mw']:
        max_idx = stats_df[metric].idxmax()
        max_value = stats_df.loc[max_idx, metric]
        records['all_time'][metric] = {
            'value': float(max_value),
            'timestamp': max_idx.isoformat()
        }
    
    # Find hourly records for renewable percentage
    for hour in range(24):
        hour_data = stats_df[stats_df['hour'] == hour]
        if not hour_data.empty:
            max_idx = hour_data['renewable_pct'].idxmax()
            max_value = hour_data.loc[max_idx, 'renewable_pct']
            records['hourly'][str(hour)] = {
                'value': float(max_value),
                'timestamp': max_idx.isoformat()
            }
    
    return records

def main():
    print("="*60)
    print("Recalculating Renewable Energy Records")
    print("="*60)
    
    # Load DUID mapping
    print("\nLoading DUID mapping...")
    duid_to_fuel = load_duid_mapping()
    
    # Step 1: Calculate records from 30-minute data
    print("\n" + "-"*60)
    print("STEP 1: Processing 30-minute historical data (5+ years)")
    print("-"*60)
    
    gen_30min = load_30min_generation_data()
    rooftop_30min = load_rooftop_data()
    
    print(f"Generation data range: {gen_30min['settlementdate'].min()} to {gen_30min['settlementdate'].max()}")
    print(f"Total records: {len(gen_30min):,}")
    
    # Calculate statistics for 30-minute data
    print("\nCalculating renewable statistics from 30-minute data...")
    stats_30min = calculate_renewable_stats(gen_30min, rooftop_30min, duid_to_fuel, resolution='30min')
    
    # Find records
    print("\nFinding records from 30-minute data...")
    records_30min = find_records(stats_30min, resolution='30min')
    
    # Print 30-minute records
    print("\n30-MINUTE DATA RECORDS:")
    print(f"All-time renewable %: {records_30min['all_time']['renewable_pct']['value']:.1f}% at {records_30min['all_time']['renewable_pct']['timestamp']}")
    print(f"Max wind: {records_30min['all_time']['wind_mw']['value']:.0f} MW at {records_30min['all_time']['wind_mw']['timestamp']}")
    print(f"Max solar: {records_30min['all_time']['solar_mw']['value']:.0f} MW at {records_30min['all_time']['solar_mw']['timestamp']}")
    print(f"Max rooftop: {records_30min['all_time']['rooftop_mw']['value']:.0f} MW at {records_30min['all_time']['rooftop_mw']['timestamp']}")
    print(f"Max water/hydro: {records_30min['all_time']['water_mw']['value']:.0f} MW at {records_30min['all_time']['water_mw']['timestamp']}")
    
    # Step 2: Check if 5-minute data has broken any records
    print("\n" + "-"*60)
    print("STEP 2: Checking recent 5-minute data for new records")
    print("-"*60)
    
    try:
        gen_5min = load_5min_generation_data()
        print(f"5-minute data range: {gen_5min['settlementdate'].min()} to {gen_5min['settlementdate'].max()}")
        
        # For now, process 5-minute data without rooftop (needs interpolation)
        print("\nCalculating renewable statistics from 5-minute data...")
        stats_5min = calculate_renewable_stats(gen_5min, None, duid_to_fuel, resolution='5min')
        
        # Check for new records
        records_updated = False
        
        # Check all-time records (excluding rooftop since we don't have it for 5-min)
        for metric in ['renewable_pct', 'wind_mw', 'solar_mw', 'water_mw', 'biomass_mw']:
            max_5min_idx = stats_5min[metric].idxmax()
            max_5min_value = stats_5min.loc[max_5min_idx, metric]
            
            if max_5min_value > records_30min['all_time'][metric]['value']:
                print(f"\nNEW RECORD! {metric}: {max_5min_value:.1f} at {max_5min_idx} (was {records_30min['all_time'][metric]['value']:.1f})")
                records_30min['all_time'][metric] = {
                    'value': float(max_5min_value),
                    'timestamp': max_5min_idx.isoformat()
                }
                records_updated = True
        
        # Check hourly records
        for hour in range(24):
            hour_data_5min = stats_5min[stats_5min['hour'] == hour]
            if not hour_data_5min.empty:
                max_idx = hour_data_5min['renewable_pct'].idxmax()
                max_value = hour_data_5min.loc[max_idx, 'renewable_pct']
                
                hour_key = str(hour)
                if hour_key in records_30min['hourly']:
                    if max_value > records_30min['hourly'][hour_key]['value']:
                        print(f"\nNEW HOURLY RECORD! Hour {hour}: {max_value:.1f}% at {max_idx} (was {records_30min['hourly'][hour_key]['value']:.1f}%)")
                        records_30min['hourly'][hour_key] = {
                            'value': float(max_value),
                            'timestamp': max_idx.isoformat()
                        }
                        records_updated = True
        
        if not records_updated:
            print("\nNo records were broken by recent 5-minute data.")
            
    except Exception as e:
        print(f"\nError processing 5-minute data: {e}")
        print("Continuing with 30-minute records only...")
    
    # Save the records
    data_dir = os.getenv('DATA_DIR', '/tmp')
    records_file = Path(data_dir) / 'renewable_records_calculated.json'
    
    print(f"\n" + "-"*60)
    print(f"Saving records to: {records_file}")
    print("-"*60)
    
    records_file.parent.mkdir(parents=True, exist_ok=True)
    with open(records_file, 'w') as f:
        json.dump(records_30min, f, indent=2)
    
    print("\nRecords saved successfully!")
    
    # Also save a simpler version for the gauge (just renewable % records)
    gauge_records = {
        'all_time': records_30min['all_time']['renewable_pct'],
        'hourly': records_30min['hourly']
    }
    
    gauge_file = Path(data_dir) / 'renewable_records.json'
    with open(gauge_file, 'w') as f:
        json.dump(gauge_records, f, indent=2)
    
    print(f"Gauge records saved to: {gauge_file}")
    
    # Print summary
    print("\n" + "="*60)
    print("FINAL RECORDS SUMMARY")
    print("="*60)
    print(f"All-time renewable %: {records_30min['all_time']['renewable_pct']['value']:.1f}%")
    print(f"Max wind generation: {records_30min['all_time']['wind_mw']['value']:.0f} MW")
    print(f"Max solar generation: {records_30min['all_time']['solar_mw']['value']:.0f} MW")
    print(f"Max rooftop generation: {records_30min['all_time']['rooftop_mw']['value']:.0f} MW")
    print(f"Max water/hydro generation: {records_30min['all_time']['water_mw']['value']:.0f} MW")
    print("\nHourly renewable % records:")
    for hour in range(24):
        if str(hour) in records_30min['hourly']:
            print(f"  Hour {hour:2d}: {records_30min['hourly'][str(hour)]['value']:.1f}%")

if __name__ == "__main__":
    main()