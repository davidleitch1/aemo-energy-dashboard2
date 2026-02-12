#!/usr/bin/env python3
"""
Analyze the difference between 5-minute and 30-minute source data
to understand why totals differ.
"""

import sys
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.data_service.shared_data_duckdb import duckdb_data_service

def analyze_data_differences():
    """Compare 5-min and 30-min raw data."""
    
    # Get the latest full 24 hours
    end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=1)
    
    print(f"Analyzing period: {start_date} to {end_date}")
    print("="*80)
    
    # Get 5-minute data
    query_5min = f"""
        SELECT 
            g.settlementdate,
            g.scadavalue,
            p.rrp
        FROM generation_5min g
        JOIN prices_5min p 
          ON g.settlementdate = p.settlementdate 
          AND p.regionid = 'SA1'
        WHERE g.duid = 'HPR1'
          AND g.settlementdate >= '{start_date.isoformat()}'
          AND g.settlementdate < '{end_date.isoformat()}'
        ORDER BY g.settlementdate
    """
    
    df_5min = duckdb_data_service.conn.execute(query_5min).df()
    df_5min['settlementdate'] = pd.to_datetime(df_5min['settlementdate'])
    
    # Get 30-minute data
    query_30min = f"""
        SELECT 
            g.settlementdate,
            g.scadavalue,
            p.rrp
        FROM generation_30min g
        JOIN prices_30min p 
          ON g.settlementdate = p.settlementdate 
          AND p.regionid = 'SA1'
        WHERE g.duid = 'HPR1'
          AND g.settlementdate >= '{start_date.isoformat()}'
          AND g.settlementdate < '{end_date.isoformat()}'
        ORDER BY g.settlementdate
    """
    
    df_30min = duckdb_data_service.conn.execute(query_30min).df()
    df_30min['settlementdate'] = pd.to_datetime(df_30min['settlementdate'])
    
    print(f"5-minute data points: {len(df_5min)}")
    print(f"30-minute data points: {len(df_30min)}")
    print()
    
    # Calculate raw MW totals (before any time conversion)
    print("RAW MW VALUES:")
    print(f"5-min sum of SCADAVALUE: {df_5min['scadavalue'].sum():.2f} MW")
    print(f"30-min sum of SCADAVALUE: {df_30min['scadavalue'].sum():.2f} MW")
    print()
    
    # Calculate MWh properly for each resolution
    df_5min['mwh'] = df_5min['scadavalue'] * (1/12)  # 5 min = 1/12 hour
    df_30min['mwh'] = df_30min['scadavalue'] * 0.5   # 30 min = 0.5 hour
    
    print("ENERGY TOTALS (MWh):")
    print(f"5-min total MWh: {df_5min['mwh'].sum():.2f}")
    print(f"30-min total MWh: {df_30min['mwh'].sum():.2f}")
    print(f"Difference: {df_5min['mwh'].sum() - df_30min['mwh'].sum():.2f} MWh")
    print()
    
    # Separate discharge and charge
    discharge_5min = df_5min[df_5min['scadavalue'] > 0]
    charge_5min = df_5min[df_5min['scadavalue'] < 0]
    discharge_30min = df_30min[df_30min['scadavalue'] > 0]
    charge_30min = df_30min[df_30min['scadavalue'] < 0]
    
    print("DISCHARGE:")
    print(f"5-min discharge MWh: {discharge_5min['mwh'].sum():.2f}")
    print(f"30-min discharge MWh: {discharge_30min['mwh'].sum():.2f}")
    print(f"5-min discharge periods: {len(discharge_5min)}")
    print(f"30-min discharge periods: {len(discharge_30min)}")
    print()
    
    print("CHARGE:")
    print(f"5-min charge MWh: {charge_5min['mwh'].sum():.2f}")
    print(f"30-min charge MWh: {charge_30min['mwh'].sum():.2f}")
    print(f"5-min charge periods: {len(charge_5min)}")
    print(f"30-min charge periods: {len(charge_30min)}")
    print()
    
    # Check if 30-min is properly averaged from 5-min
    print("CHECKING 30-MIN AGGREGATION:")
    # Aggregate 5-min to 30-min
    df_5min_copy = df_5min.copy()
    df_5min_copy = df_5min_copy.set_index('settlementdate')
    df_5min_agg_30min = df_5min_copy.resample('30min').agg({
        'scadavalue': 'mean',  # Average MW over the 30-min period
        'rrp': 'mean',  # Average price
        'mwh': 'sum'  # Total MWh (sum of 6 x 5-min periods)
    }).reset_index()
    
    print(f"5-min aggregated to 30-min: {len(df_5min_agg_30min)} periods")
    print(f"5-min->30min total MWh: {df_5min_agg_30min['mwh'].sum():.2f}")
    print(f"Native 30-min total MWh: {df_30min['mwh'].sum():.2f}")
    
    # Compare first few periods
    print("\nFIRST 5 PERIODS COMPARISON:")
    print("30-min data:")
    for i in range(min(5, len(df_30min))):
        row = df_30min.iloc[i]
        print(f"  {row['settlementdate']}: {row['scadavalue']:.2f} MW, {row['mwh']:.2f} MWh")
    
    print("\n5-min aggregated to 30-min:")
    for i in range(min(5, len(df_5min_agg_30min))):
        row = df_5min_agg_30min.iloc[i]
        print(f"  {row['settlementdate']}: {row['scadavalue']:.2f} MW, {row['mwh']:.2f} MWh")
    
    # Check for missing periods
    print("\nMISSING DATA CHECK:")
    # Get all expected 30-min timestamps
    expected_times = pd.date_range(start=start_date, end=end_date, freq='30min', inclusive='left')
    actual_30min_times = set(df_30min['settlementdate'])
    missing_30min = [t for t in expected_times if t not in actual_30min_times]
    
    if missing_30min:
        print(f"Missing 30-min periods: {len(missing_30min)}")
        print(f"First few missing: {missing_30min[:5]}")
    else:
        print("No missing 30-min periods")
    
    # Check 5-min
    expected_5min_times = pd.date_range(start=start_date, end=end_date, freq='5min', inclusive='left')
    actual_5min_times = set(df_5min['settlementdate'])
    missing_5min = [t for t in expected_5min_times if t not in actual_5min_times]
    
    if missing_5min:
        print(f"Missing 5-min periods: {len(missing_5min)}")
        print(f"First few missing: {missing_5min[:5]}")
    else:
        print("No missing 5-min periods")

if __name__ == "__main__":
    analyze_data_differences()