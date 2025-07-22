#!/usr/bin/env python3
"""
Check for missing data issues.
"""
import os
import sys
from pathlib import Path
from datetime import datetime

# Set environment variable BEFORE any imports
os.environ['USE_DUCKDB'] = 'true'

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

import pandas as pd
from aemo_dashboard.generation.generation_query_manager import GenerationQueryManager

def check_missing_data():
    """Check for missing data."""
    query_manager = GenerationQueryManager()
    
    # Check each year
    for year in [2023, 2024, 2025]:
        print(f"\n{'='*50}")
        print(f"YEAR {year}")
        print('='*50)
        
        start_date = datetime(year, 1, 1)
        end_date = datetime(year, 12, 31, 23, 59, 59) if year < 2025 else datetime(2025, 7, 19, 23, 59, 59)
        
        # Get generation data
        gen_data = query_manager.query_generation_by_fuel(
            start_date=start_date,
            end_date=end_date,
            region='NEM',
            resolution='30min'
        )
        
        # Check VRE components
        for fuel in ['Wind', 'Solar']:
            fuel_data = gen_data[gen_data['fuel_type'] == fuel]
            if not fuel_data.empty:
                fuel_data = fuel_data.copy()
                fuel_data['date'] = pd.to_datetime(fuel_data['settlementdate']).dt.date
                
                # Daily averages
                daily = fuel_data.groupby('date')['total_generation_mw'].mean()
                
                print(f"\n{fuel}:")
                print(f"  Total days: {len(daily)}")
                print(f"  Average MW: {daily.mean():.0f}")
                print(f"  Min daily avg: {daily.min():.0f} MW on {daily.idxmin()}")
                print(f"  Max daily avg: {daily.max():.0f} MW on {daily.idxmax()}")
                
                # Check for suspiciously low values
                low_days = daily[daily < 100]
                if not low_days.empty:
                    print(f"  Days with < 100 MW average: {len(low_days)}")
                    print(f"    {low_days.index.tolist()[:5]}...")
        
        # Check data completeness
        actual_days = gen_data['settlementdate'].dt.date.nunique()
        expected_days = (end_date - start_date).days + 1
        
        print(f"\nData completeness: {actual_days}/{expected_days} days ({actual_days/expected_days*100:.1f}%)")
        
        if actual_days < expected_days:
            # Find missing days
            all_days = pd.date_range(start_date, end_date, freq='D').date
            actual_days_set = set(gen_data['settlementdate'].dt.date.unique())
            missing_days = set(all_days) - actual_days_set
            
            if missing_days:
                print(f"Missing days: {sorted(missing_days)[:10]}...")

if __name__ == "__main__":
    check_missing_data()