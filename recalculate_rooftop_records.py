#!/usr/bin/env python3
"""
Recalculate rooftop and renewable records with correct region filtering

This script recalculates all renewable energy records using only the five main regions
(NSW1, QLD1, SA1, TAS1, VIC1) for rooftop solar.
"""

import os
import sys
import json
import pickle
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# Add src to path
sys.path.insert(0, 'src')

from dotenv import load_dotenv
load_dotenv('.env')

# Configuration
RENEWABLE_FUELS = ['Wind', 'Solar', 'Water', 'Rooftop Solar', 'Hydro', 'Biomass']
EXCLUDED_FUELS = ['Battery Storage', 'Transmission Flow']
MAIN_REGIONS = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']

# Override paths for development machine
os.environ['GEN_OUTPUT_FILE_5MIN'] = '/Volumes/davidleitch/aemo_production/data/scada5.parquet'
os.environ['ROOFTOP_SOLAR_FILE'] = '/Volumes/davidleitch/aemo_production/data/rooftop30.parquet'
os.environ['GEN_INFO_FILE'] = '/Volumes/davidleitch/aemo_production/data/gen_info.pkl'
os.environ['DATA_DIR'] = '/Volumes/davidleitch/aemo_production/data'

def henderson_weights(n=13):
    """Generate Henderson filter weights"""
    if n == 13:
        return np.array([-0.019, -0.028, 0.0, 0.066, 0.147, 0.214, 0.240,
                        0.214, 0.147, 0.066, 0.0, -0.028, -0.019])
    else:
        raise ValueError(f"Henderson weights not defined for n={n}")

def henderson_smooth(data, n=13):
    """Apply Henderson filter to smooth data"""
    weights = henderson_weights(n)
    half_window = n // 2

    smoothed = np.copy(data)
    for i in range(half_window, len(data) - half_window):
        smoothed[i] = np.sum(weights * data[i-half_window:i+half_window+1])

    return smoothed

def interpolate_rooftop_to_5min(df_30min):
    """Convert 30-minute rooftop data to 5-minute using Henderson smoothing"""
    start = df_30min.index.min()
    end = df_30min.index.max() + pd.Timedelta(minutes=25)
    index_5min = pd.date_range(start=start, end=end, freq='5min')

    df_5min = pd.DataFrame(index=index_5min)

    for col in df_30min.columns:
        if col != 'NEM':
            series_5min = df_30min[col].reindex(index_5min).interpolate(method='linear')
            series_5min = series_5min.ffill().bfill().fillna(0)
            values = series_5min.values
            smoothed = henderson_smooth(values)
            df_5min[col] = smoothed

    # Recalculate NEM total from main regions only
    region_cols = [c for c in df_5min.columns if c != 'NEM']
    df_5min['NEM'] = df_5min[region_cols].sum(axis=1)

    return df_5min

print("="*70)
print("Recalculating Renewable Energy Records (Corrected for Main Regions Only)")
print("="*70)

# Load file paths - use development machine paths
gen_5min_path = os.getenv('GEN_OUTPUT_FILE_5MIN', '/Volumes/davidleitch/aemo_production/data/scada5.parquet')
rooftop_path = os.getenv('ROOFTOP_SOLAR_FILE', '/Volumes/davidleitch/aemo_production/data/rooftop30.parquet')
gen_info_path = os.getenv('GEN_INFO_FILE', '/Volumes/davidleitch/aemo_production/data/gen_info.pkl')
data_dir = os.getenv('DATA_DIR', '/Volumes/davidleitch/aemo_production/data')

print(f"\nLoading data from:")
print(f"  Generation: {gen_5min_path}")
print(f"  Rooftop: {rooftop_path}")
print(f"  Gen Info: {gen_info_path}")

# Load generator info
with open(gen_info_path, 'rb') as f:
    gen_info = pickle.load(f)
duid_to_fuel = gen_info.set_index('DUID')['Fuel'].to_dict()

# Load generation data
print("\nLoading generation data...")
gen_df = pd.read_parquet(gen_5min_path)
gen_df['settlementdate'] = pd.to_datetime(gen_df['settlementdate'])
gen_df['fuel_type'] = gen_df['duid'].map(duid_to_fuel)
gen_df = gen_df.dropna(subset=['fuel_type'])
gen_df = gen_df[~gen_df['fuel_type'].isin(EXCLUDED_FUELS)]
print(f"  Loaded {len(gen_df):,} generation records")
print(f"  Generation data range: {gen_df['settlementdate'].min()} to {gen_df['settlementdate'].max()}")

# Store the earliest date with generation data
gen_start_date = gen_df['settlementdate'].min()

# Load rooftop data (MAIN REGIONS ONLY)
print("\nLoading rooftop data (main regions only)...")
rooftop_df = pd.read_parquet(rooftop_path)
rooftop_df['settlementdate'] = pd.to_datetime(rooftop_df['settlementdate'])
rooftop_df = rooftop_df[rooftop_df['regionid'].isin(MAIN_REGIONS)]  # KEY FIX

# Filter rooftop to only dates where we have generation data
rooftop_df = rooftop_df[rooftop_df['settlementdate'] >= gen_start_date]
print(f"  Loaded {len(rooftop_df):,} rooftop records (main regions only, from {gen_start_date})")

# Pivot and interpolate rooftop to 5-min
print("\nInterpolating rooftop to 5-minute resolution...")
rooftop_wide = rooftop_df.pivot(index='settlementdate', columns='regionid', values='power').fillna(0)
rooftop_5min = interpolate_rooftop_to_5min(rooftop_wide)
print(f"  Created {len(rooftop_5min):,} 5-minute rooftop records")

# Calculate records
print("\n" + "="*70)
print("Calculating All-Time Records")
print("="*70)

# Rooftop record
rooftop_max = rooftop_5min['NEM'].max()
rooftop_max_time = rooftop_5min['NEM'].idxmax()
print(f"\n🏠 ROOFTOP SOLAR:")
print(f"   Record: {rooftop_max:,.0f} MW")
print(f"   Time: {rooftop_max_time}")

# Wind record
wind_totals = gen_df[gen_df['fuel_type'] == 'Wind'].groupby('settlementdate')['scadavalue'].sum()
wind_max = wind_totals.max()
wind_max_time = wind_totals.idxmax()
print(f"\n🌬️  WIND:")
print(f"   Record: {wind_max:,.0f} MW")
print(f"   Time: {wind_max_time}")

# Solar record
solar_totals = gen_df[gen_df['fuel_type'] == 'Solar'].groupby('settlementdate')['scadavalue'].sum()
solar_max = solar_totals.max()
solar_max_time = solar_totals.idxmax()
print(f"\n☀️  SOLAR:")
print(f"   Record: {solar_max:,.0f} MW")
print(f"   Time: {solar_max_time}")

# Hydro/Water record
water_totals = gen_df[gen_df['fuel_type'].isin(['Water', 'Hydro'])].groupby('settlementdate')['scadavalue'].sum()
water_max = water_totals.max()
water_max_time = water_totals.idxmax()
print(f"\n💧 HYDRO/WATER:")
print(f"   Record: {water_max:,.0f} MW")
print(f"   Time: {water_max_time}")

# Renewable percentage record (VECTORIZED)
print("\n" + "="*70)
print("Calculating Renewable Percentage Record (Vectorized)")
print("="*70)

# Step 1: Tag each fuel type as renewable or not
print("\nPreparing generation data...")
gen_df['is_renewable'] = gen_df['fuel_type'].isin(RENEWABLE_FUELS)

# Step 2: Group by timestamp and calculate renewable vs total generation
print("Aggregating by timestamp...")
gen_by_time = gen_df.groupby('settlementdate').agg(
    renewable_gen=pd.NamedAgg(column='scadavalue', aggfunc=lambda x: x[gen_df.loc[x.index, 'is_renewable']].sum()),
    total_gen=pd.NamedAgg(column='scadavalue', aggfunc='sum')
).reset_index()

# Step 3: Add rooftop solar (convert rooftop_5min index to column)
print("Adding rooftop solar data...")
rooftop_for_merge = rooftop_5min[['NEM']].reset_index()
rooftop_for_merge.columns = ['settlementdate', 'rooftop_mw']

# Step 4: Merge generation with rooftop (INNER join - only timestamps where both exist)
print("Merging generation and rooftop data...")
combined = gen_by_time.merge(rooftop_for_merge, on='settlementdate', how='inner')

# Step 5: Calculate renewable percentage
print(f"Calculating renewable percentages for {len(combined):,} timestamps...")
combined['renewable_mw'] = combined['renewable_gen'] + combined['rooftop_mw']
combined['total_mw'] = combined['total_gen'] + combined['rooftop_mw']

# Filter out timestamps with unrealistically low total (indicates incomplete data)
# Normal NEM demand is 18,000-30,000 MW, minimum credible is ~15,000 MW
combined = combined[combined['total_mw'] >= 15000]
print(f"Filtered to {len(combined):,} timestamps with complete data (total_mw >= 15,000)")

combined['renewable_pct'] = (combined['renewable_mw'] / combined['total_mw'] * 100)

# Step 6: Find the maximum
renewable_max_idx = combined['renewable_pct'].idxmax()
renewable_max = combined.loc[renewable_max_idx]

# Create DataFrame for hourly analysis (compatible with existing code)
renewable_df = combined[['settlementdate', 'renewable_pct', 'renewable_mw', 'total_mw']].copy()
renewable_df.columns = ['timestamp', 'renewable_pct', 'renewable_mw', 'total_mw']

print(f"\n🎉 RENEWABLE PERCENTAGE:")
print(f"   Record: {renewable_max['renewable_pct']:.1f}%")
print(f"   Time: {renewable_max['settlementdate']}")
print(f"   Renewable: {renewable_max['renewable_mw']:,.0f} MW")
print(f"   Total: {renewable_max['total_mw']:,.0f} MW")

# Build records structure
print("\n" + "="*70)
print("Building Records File")
print("="*70)

records = {
    'all_time': {
        'renewable_pct': {
            'value': float(renewable_max['renewable_pct']),
            'timestamp': renewable_max['settlementdate'].isoformat()
        },
        'wind_mw': {
            'value': float(wind_max),
            'timestamp': wind_max_time.isoformat()
        },
        'solar_mw': {
            'value': float(solar_max),
            'timestamp': solar_max_time.isoformat()
        },
        'rooftop_mw': {
            'value': float(rooftop_max),
            'timestamp': rooftop_max_time.isoformat()
        },
        'water_mw': {
            'value': float(water_max),
            'timestamp': water_max_time.isoformat()
        }
    },
    'hourly': {}
}

# Calculate hourly records (renewable percentage by hour of day)
print("\nCalculating hourly records...")
renewable_df['hour'] = pd.to_datetime(renewable_df['timestamp']).dt.hour

for hour in range(24):
    hour_data = renewable_df[renewable_df['hour'] == hour]
    if not hour_data.empty:
        max_row = hour_data.loc[hour_data['renewable_pct'].idxmax()]
        records['hourly'][str(hour)] = {
            'value': float(max_row['renewable_pct']),
            'timestamp': max_row['timestamp'].isoformat()
        }
        print(f"  Hour {hour:2d}: {max_row['renewable_pct']:.1f}%")

# Save records
output_file = Path(data_dir) / 'renewable_records_calculated.json'
print(f"\nSaving corrected records to: {output_file}")
with open(output_file, 'w') as f:
    json.dump(records, f, indent=2)

print("\n" + "="*70)
print("✅ RECORDS RECALCULATION COMPLETE")
print("="*70)
print("\nNext steps:")
print("1. Restart the renewable gauge on production machine:")
print("   pkill -f renewable_gauge_stacked.py")
print("   cd /Users/davidleitch/aemo_production/aemo-energy-dashboard2")
print("   nohup /Users/davidleitch/miniforge3/bin/python3 renewable_gauge_stacked.py --port 5007 > /tmp/renewable_gauge.log 2>&1 &")
print("="*70)
