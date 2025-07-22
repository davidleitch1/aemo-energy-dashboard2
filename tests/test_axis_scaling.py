#!/usr/bin/env python3
"""
Test Y-axis scaling for penetration charts when changing regions.
"""
import os
import sys
import time

# Set environment variables
os.environ['USE_DUCKDB'] = 'true'

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from aemo_dashboard.penetration import PenetrationTab

def test_axis_scaling():
    """Test that Y-axis scales properly when changing regions."""
    print("Testing Y-axis scaling...")
    
    # Create the penetration tab
    penetration = PenetrationTab()
    
    # Test with NEM
    print("\n1. Testing with NEM...")
    penetration.region_select.value = 'NEM'
    time.sleep(0.5)  # Allow update
    
    # Test with NSW1
    print("\n2. Testing with NSW1...")
    penetration.region_select.value = 'NSW1'
    time.sleep(0.5)  # Allow update
    
    # Test with TAS1 (smaller state)
    print("\n3. Testing with TAS1...")
    penetration.region_select.value = 'TAS1'
    time.sleep(0.5)  # Allow update
    
    print("\nAll charts should have updated with appropriate Y-axis scaling.")
    print("Check that:")
    print("- VRE by fuel chart Y-axis adjusts (NEM ~45 TWh, NSW ~14 TWh, TAS ~3 TWh)")
    print("- Thermal vs Renewables Y-axis adjusts (NEM ~170 TWh, NSW ~50 TWh, TAS ~10 TWh)")

if __name__ == "__main__":
    test_axis_scaling()