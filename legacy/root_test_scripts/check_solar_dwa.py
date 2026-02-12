#!/usr/bin/env python3
"""Check Solar DWA calculation matching dashboard logic"""

import pandas as pd
import duckdb
from pathlib import Path
import pickle
from datetime import datetime, time

# Set up paths  
data_dir = Path('/Volumes/davidleitch/aemo_production/data')

# Exactly match dashboard date handling
start_datetime = datetime.combine(pd.to_datetime('2024-07-01').date(), time.min)
end_datetime = datetime.combine(pd.to_datetime('2025-06-30').date(), time.max)

print(f'Date range: {start_datetime} to {end_datetime}')

# Load DUID mapping
with open(data_dir / 'gen_info.pkl', 'rb') as f:
    duid_mapping = pickle.load(f)

# Get Solar DUIDs in SA1  
solar_duids = duid_mapping[(duid_mapping['Fuel'] == 'Solar') & (duid_mapping['Region'] == 'SA1')]['DUID'].tolist()
print(f'Solar DUIDs in SA1: {len(solar_duids)}')

# Use scada30.parquet like the dashboard would for monthly aggregation
conn = duckdb.connect()

# First check what date range we actually have in the data
date_check_query = '''
SELECT 
    MIN(settlementdate) as min_date,
    MAX(settlementdate) as max_date,
    COUNT(DISTINCT settlementdate) as n_periods
FROM read_parquet(?)
WHERE settlementdate >= ?
  AND settlementdate <= ?
'''

date_info = conn.execute(date_check_query, [
    str(data_dir / 'scada30.parquet'),
    start_datetime,
    end_datetime
]).df()

print(f"\nData availability:")
print(f"  Min date: {date_info['min_date'].iloc[0]}")
print(f"  Max date: {date_info['max_date'].iloc[0]}")
print(f"  Periods: {date_info['n_periods'].iloc[0]}")

# Now get the actual generation data
query = '''
SELECT 
    settlementdate,
    duid,
    scadavalue
FROM read_parquet(?)
WHERE settlementdate >= ?
  AND settlementdate <= ?
  AND duid = ANY(?)
'''

gen_df = conn.execute(query, [
    str(data_dir / 'scada30.parquet'),
    start_datetime,
    end_datetime,
    solar_duids
]).df()

print(f'\nGeneration records loaded: {len(gen_df)}')

# Load prices
price_query = '''
SELECT settlementdate, rrp
FROM read_parquet(?)
WHERE settlementdate >= ?
  AND settlementdate <= ?
  AND regionid = 'SA1'
'''

price_df = conn.execute(price_query, [
    str(data_dir / 'prices30.parquet'),
    start_datetime,
    end_datetime
]).df()

print(f'Price records loaded: {len(price_df)}')

# Aggregate generation by settlement date (sum all solar DUIDs)
gen_agg = gen_df.groupby('settlementdate')['scadavalue'].sum().reset_index()
gen_agg.columns = ['settlementdate', 'total_solar_mw']

print(f'Aggregated to {len(gen_agg)} settlement periods')

# Merge with prices
merged = pd.merge(gen_agg, price_df, on='settlementdate', how='inner')
print(f'Merged records: {len(merged)}')

# Calculate DWA exactly as dashboard does
# Revenue = sum(generation * price * 0.5 hours)
# Energy = sum(generation * 0.5 hours)
revenue = (merged['total_solar_mw'] * merged['rrp'] * 0.5).sum()
energy = (merged['total_solar_mw'] * 0.5).sum()
dwa = revenue / energy if energy > 0 else 0

print(f'\n=== RESULTS ===')
print(f'Total Solar Energy: {energy:,.0f} MWh')
print(f'Total Solar Revenue: ${revenue:,.0f}')
print(f'Solar DWA (exact): ${dwa:.2f}/MWh')
print(f'Solar DWA (rounded): ${round(dwa)}/MWh')

# Check if we're filtering out any negative generation
negative_gen = gen_df[gen_df['scadavalue'] < 0]
if not negative_gen.empty:
    print(f'\nWarning: Found {len(negative_gen)} negative generation records')
    print(f'Total negative generation: {negative_gen["scadavalue"].sum():.2f} MW')

# Double-check by looking at monthly aggregation as dashboard might do
print(f'\n=== MONTHLY CHECK ===')
merged['month'] = pd.to_datetime(merged['settlementdate']).dt.to_period('M')
monthly = merged.groupby('month').agg({
    'total_solar_mw': 'sum',
    'rrp': 'mean'
})
print(monthly.head())