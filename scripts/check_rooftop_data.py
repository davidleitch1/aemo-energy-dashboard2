#!/usr/bin/env python3
"""
Check rooftop data availability.
"""
import os
import sys
from pathlib import Path

# Set environment variable BEFORE any imports
os.environ['USE_DUCKDB'] = 'true'

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

import pandas as pd
from aemo_dashboard.shared.config import Config

def check_rooftop():
    """Check rooftop data."""
    config = Config()
    rooftop_file = config.rooftop_solar_file
    
    print(f"Rooftop file: {rooftop_file}")
    
    if not rooftop_file or not Path(rooftop_file).exists():
        print("Rooftop file not found!")
        return
    
    # Load the file
    df = pd.read_parquet(rooftop_file)
    
    print(f"\nRooftop data shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")
    
    # Check date range
    df['settlementdate'] = pd.to_datetime(df['settlementdate'])
    
    print(f"\nDate range: {df['settlementdate'].min()} to {df['settlementdate'].max()}")
    
    # Check by year
    df['year'] = df['settlementdate'].dt.year
    year_counts = df.groupby('year').size()
    
    print("\nRecords by year:")
    print(year_counts)
    
    # Check 2025 specifically
    df_2025 = df[df['year'] == 2025]
    if not df_2025.empty:
        print(f"\n2025 data range: {df_2025['settlementdate'].min()} to {df_2025['settlementdate'].max()}")
        
        # Check for gaps
        df_2025['date'] = df_2025['settlementdate'].dt.date
        daily_counts = df_2025.groupby('date').size()
        
        # Expected 48 records per day (30-min intervals)
        days_with_gaps = daily_counts[daily_counts < 48]
        if not days_with_gaps.empty:
            print(f"\nDays with missing data in 2025:")
            print(days_with_gaps)
    else:
        print("\nNO DATA FOR 2025!")
    
    # Check regions
    if 'regionid' in df.columns:
        print(f"\nRegions: {df['regionid'].unique()}")

if __name__ == "__main__":
    check_rooftop()