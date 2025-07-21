#!/usr/bin/env python3
"""
Test the v2 penetration tab.
"""
import os
import sys
from pathlib import Path

# Set environment variable BEFORE any imports
os.environ['USE_DUCKDB'] = 'true'

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from aemo_dashboard.penetration import PenetrationTab

def test_v2():
    """Test the v2 implementation."""
    print("Testing Penetration Tab v2...")
    
    try:
        # Create tab instance
        tab = PenetrationTab()
        print("✓ Tab created successfully")
        
        # Test data loading
        years = [2024]
        data = tab._get_generation_data(years)
        
        print(f"\nData shape: {data.shape}")
        print(f"Unique fuel types: {sorted(data['fuel_type'].unique())}")
        
        # Check data for each fuel type
        for fuel in ['Wind', 'Solar', 'Rooftop']:
            fuel_data = data[data['fuel_type'] == fuel]
            if not fuel_data.empty:
                avg_mw = fuel_data['total_generation_mw'].mean()
                print(f"\n{fuel}:")
                print(f"  Records: {len(fuel_data)}")
                print(f"  Average MW: {avg_mw:.0f}")
                print(f"  Annualised TWh: {avg_mw * 24 * 365 / 1_000_000:.1f}")
        
        # Test chart creation
        print("\nCreating chart...")
        chart = tab._create_vre_production_chart()
        print("✓ Chart created successfully")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_v2()