#!/usr/bin/env python3
"""
Test script to verify the VRE share calculation fix in the Insights tab.
"""

import sys
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from src.aemo_dashboard.shared.logging_config import setup_logging, get_logger
from src.aemo_dashboard.generation.generation_query_manager import GenerationQueryManager
from src.aemo_dashboard.shared.rooftop_adapter import load_rooftop_data

# Set up logging
setup_logging()
logger = get_logger(__name__)

def test_vre_calculation():
    """Test the VRE share calculation with the new approach"""
    
    # Initialize query manager
    query_manager = GenerationQueryManager()
    
    # Test regions
    regions = ['NSW1', 'QLD1', 'NEM']
    
    # Define test periods
    periods = {
        '2020': (datetime(2020, 1, 1), datetime(2020, 12, 31)),
        'Last 12 months': (datetime.now() - timedelta(days=365), datetime.now())
    }
    
    for region in regions:
        print(f"\n{'='*60}")
        print(f"Testing VRE calculation for {region}")
        print(f"{'='*60}")
        
        for period_name, (start_date, end_date) in periods.items():
            print(f"\nPeriod: {period_name} ({start_date.date()} to {end_date.date()})")
            
            try:
                # Load generation data using query manager
                gen_data = query_manager.query_generation_by_fuel(
                    start_date=start_date,
                    end_date=end_date,
                    region=region,
                    resolution='30min'
                )
                
                if gen_data.empty:
                    print(f"  WARNING: No generation data available")
                    continue
                
                # Load rooftop data
                rooftop_data = load_rooftop_data(
                    start_date=start_date,
                    end_date=end_date
                )
                
                # Calculate VRE share (same logic as in insights_tab.py)
                if 'fuel_type' in gen_data.columns:
                    # Data is already aggregated by fuel type
                    fuel_averages = gen_data.groupby('fuel_type')['total_generation_mw'].mean()
                    
                    # Calculate total generation average
                    total_gen_mw = fuel_averages.sum()
                    
                    # Calculate VRE average (Wind + Solar)
                    vre_fuels = ['Wind', 'Solar']
                    vre_mw = fuel_averages[fuel_averages.index.isin(vre_fuels)].sum()
                    
                    print(f"  Utility-scale generation:")
                    print(f"    Total average: {total_gen_mw:,.0f} MW")
                    print(f"    VRE average: {vre_mw:,.0f} MW")
                    print(f"    Fuel types found: {list(fuel_averages.index)}")
                    
                    # Add rooftop if available
                    rooftop_avg = 0.0
                    if rooftop_data is not None and not rooftop_data.empty:
                        if region == 'NEM':
                            # For NEM, sum all regions
                            region_cols = [col for col in rooftop_data.columns if col != 'settlementdate']
                            rooftop_avg = rooftop_data[region_cols].sum(axis=1).mean()
                        elif region in rooftop_data.columns:
                            rooftop_avg = rooftop_data[region].mean()
                        
                        print(f"    Rooftop solar average: {rooftop_avg:,.0f} MW")
                        
                        # Add rooftop to totals
                        vre_mw += rooftop_avg
                        total_gen_mw += rooftop_avg
                    
                    # Calculate VRE share
                    if total_gen_mw > 0:
                        vre_share = (vre_mw / total_gen_mw) * 100
                        print(f"  VRE share: {vre_share:.1f}% (VRE: {vre_mw:,.0f} MW / Total: {total_gen_mw:,.0f} MW)")
                    else:
                        print(f"  ERROR: Total generation is 0")
                else:
                    print(f"  ERROR: 'fuel_type' column not found in data")
                    print(f"  Available columns: {list(gen_data.columns)}")
                    
            except Exception as e:
                print(f"  ERROR: {e}")
                import traceback
                traceback.print_exc()

if __name__ == "__main__":
    print("Testing VRE share calculation fix...")
    test_vre_calculation()
    print("\nTest complete!")