#!/usr/bin/env python3
"""Debug the daily summary to see what's happening with data"""

import sys
sys.path.insert(0, 'src')

from datetime import datetime, timedelta
from aemo_dashboard.nem_dash.daily_summary import calculate_daily_metrics
from aemo_dashboard.shared import adapter_selector
from aemo_dashboard.generation.generation_query_manager import GenerationQueryManager

print("=== DAILY SUMMARY DEBUG ===\n")

# Test time periods
now = datetime.now()
today_start = now - timedelta(hours=24)
today_end = now
last_year_start = now - timedelta(days=365, hours=24)
last_year_end = now - timedelta(days=365)

print(f"Today period: {today_start} to {today_end}")
print(f"Last year period: {last_year_start} to {last_year_end}")

# Test price data loading
print("\n1. Testing price data loading...")
today_prices = adapter_selector.load_price_data(
    start_date=today_start,
    end_date=today_end,
    resolution='30min'
)
print(f"Today price records: {len(today_prices)}")

last_year_prices = adapter_selector.load_price_data(
    start_date=last_year_start,
    end_date=last_year_end,
    resolution='30min'
)
print(f"Last year price records: {len(last_year_prices)}")

# Calculate averages
if not today_prices.empty:
    price_col = 'RRP' if 'RRP' in today_prices.columns else 'rrp'
    region_col = 'REGIONID' if 'REGIONID' in today_prices.columns else 'regionid'
    
    today_nem_avg = today_prices[price_col].mean()
    print(f"Today NEM average (all regions): ${today_nem_avg:.0f}")
    
    # By region
    for region in ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']:
        region_avg = today_prices[today_prices[region_col] == region][price_col].mean()
        print(f"  {region}: ${region_avg:.0f}")

if not last_year_prices.empty:
    last_year_nem_avg = last_year_prices[price_col].mean()
    print(f"\nLast year NEM average: ${last_year_nem_avg:.0f}")
    
    # Calculate percentage change
    if last_year_nem_avg > 0:
        pct_change = ((today_nem_avg - last_year_nem_avg) / last_year_nem_avg) * 100
        print(f"Percentage change: {pct_change:.0f}%")

# Test generation data loading
print("\n2. Testing generation data loading...")
gen_manager = GenerationQueryManager()

today_gen = gen_manager.query_generation_by_fuel(
    start_date=today_start,
    end_date=today_end,
    region=None,  # All regions
    resolution='30min'
)

print(f"Generation data shape: {today_gen.shape if not today_gen.empty else 'empty'}")
if not today_gen.empty:
    print(f"Columns: {list(today_gen.columns)}")
    print(f"Unique regions: {today_gen['region'].unique() if 'region' in today_gen.columns else 'no region column'}")
    print(f"Unique fuel types: {today_gen['fuel_type'].unique() if 'fuel_type' in today_gen.columns else 'no fuel_type column'}")
    
    # Sample data
    print("\nSample generation data:")
    print(today_gen.head())

# Now test the full metrics calculation
print("\n3. Testing full metrics calculation...")
today_metrics = calculate_daily_metrics(today_start, today_end)
print(f"Today metrics: {today_metrics}")