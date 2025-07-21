#!/usr/bin/env python3
"""
Test the annualisation fix.
"""
import os
import sys
from pathlib import Path

# Set environment variable BEFORE any imports
os.environ['USE_DUCKDB'] = 'true'

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from aemo_dashboard.penetration import PenetrationTab
from aemo_dashboard.shared.smoothing import apply_ewm_smoothing
import pandas as pd

def test_annualisation():
    """Test the annualisation calculation."""
    print("Testing annualisation calculation...")
    
    # Example: If daily average MW is 10,000 MW
    daily_avg_mw = 10000
    
    # Direct annualisation
    twh_direct = daily_avg_mw * 24 * 365 / 1_000_000
    print(f"\nDirect annualisation of {daily_avg_mw:,} MW:")
    print(f"  {daily_avg_mw} MW × 24 hours × 365 days / 1,000,000 = {twh_direct:.2f} TWh")
    
    # With 30-day smoothing
    # Create sample data
    import numpy as np
    days = 365
    # Add some variation
    daily_mw = np.random.normal(daily_avg_mw, 1000, days)
    daily_mw = pd.Series(daily_mw)
    
    # Apply EWM smoothing
    smoothed_mw = apply_ewm_smoothing(daily_mw, span=30)
    
    # Annualise the smoothed values
    smoothed_twh = smoothed_mw * 24 * 365 / 1_000_000
    
    print(f"\nWith EWM smoothing (span=30):")
    print(f"  Original MW range: {daily_mw.min():.0f} - {daily_mw.max():.0f}")
    print(f"  Smoothed MW range: {smoothed_mw.min():.0f} - {smoothed_mw.max():.0f}")
    print(f"  Smoothed TWh range: {smoothed_twh.min():.2f} - {smoothed_twh.max():.2f}")
    print(f"  Average smoothed TWh: {smoothed_twh.mean():.2f}")
    
    # Test with actual penetration tab
    print("\n\nTesting actual Penetration tab data...")
    tab = PenetrationTab()
    
    # Get a sample of actual data
    years = [2024]
    data = tab._get_generation_data(years)
    
    if not data.empty:
        # Filter for VRE
        vre_data = data[data['fuel_type'].isin(['Wind', 'Solar', 'Rooftop'])].copy()
        
        # Calculate total VRE
        total_vre = vre_data.groupby('settlementdate')['total_generation_mw'].sum().mean()
        print(f"\nActual VRE average: {total_vre:.0f} MW")
        print(f"Expected annualised: {total_vre * 24 * 365 / 1_000_000:.2f} TWh")
        
        # Compare to screenshot values (should be in 50-90 TWh range)
        print("\nExpected range from screenshot: 50-90 TWh")

if __name__ == "__main__":
    test_annualisation()