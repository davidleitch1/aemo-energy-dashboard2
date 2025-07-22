#!/usr/bin/env python3
"""
Verify that the fix works by testing with an active DUID
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import pandas as pd
from datetime import datetime, timedelta

# Mock fuzzywuzzy for testing
class MockFuzzywuzzy:
    def fuzz(self, s1, s2): return 100 if s1.lower() in s2.lower() else 50
    def process(self, query, choices, limit=5):
        return [(choice, 100 if query.lower() in choice.lower() else 50) for choice in choices[:limit]]

sys.modules['fuzzywuzzy'] = MockFuzzywuzzy()
sys.modules['fuzzywuzzy.fuzz'] = MockFuzzywuzzy()  
sys.modules['fuzzywuzzy.process'] = MockFuzzywuzzy()

try:
    from src.aemo_dashboard.station.station_analysis import StationAnalysisMotor
    print("✅ Motor imported successfully")
except Exception as e:
    print(f"❌ Failed to import motor: {e}")
    exit(1)

def test_with_active_duid():
    """Test motor with a DUID we know has recent data"""
    print("🔍 Testing with Known Active DUID")
    print("=" * 40)
    
    # Load active DUIDs list
    with open("data/active_duids.txt", 'r') as f:
        active_duids = [line.strip() for line in f if line.strip()]
    
    print(f"Active DUIDs available: {len(active_duids)}")
    
    # Test with first active DUID
    test_duid = active_duids[0]
    print(f"Testing with active DUID: {test_duid}")
    
    # Initialize motor
    motor = StationAnalysisMotor()
    
    # Load mapping
    if not motor.load_data():
        print("❌ Failed to load DUID mapping")
        return
    
    # Test with 1-day range (the problematic case)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)
    
    start_dt = datetime.combine(start_date.date(), datetime.min.time())
    end_dt = datetime.combine(end_date.date(), datetime.max.time())
    
    print(f"Date range: {start_dt} to {end_dt}")
    
    # Test filtering with active DUID
    if motor.filter_station_data(test_duid, start_dt, end_dt):
        print(f"✅ SUCCESS! Filter worked with active DUID")
        print(f"   Records found: {len(motor.station_data)}")
        
        if motor.station_data is not None and len(motor.station_data) > 0:
            print(f"   Date range: {motor.station_data['settlementdate'].min()} to {motor.station_data['settlementdate'].max()}")
            print(f"   Generation range: {motor.station_data['scadavalue'].min():.1f} to {motor.station_data['scadavalue'].max():.1f} MW")
            
            # Test metrics calculation
            metrics = motor.calculate_performance_metrics()
            if metrics:
                print(f"   ✅ Metrics calculated: {len(metrics)} metrics")
                print(f"   Sample metric - Total Generation: {metrics.get('total_generation_gwh', 'N/A')} GWh")
            
            print("\n🎉 The fix resolves the station analysis data issue!")
            print("✅ 1-day view will now show data for active stations")
        else:
            print("❌ No station data despite successful filter")
    else:
        print(f"❌ Filter still failed with active DUID {test_duid}")

if __name__ == "__main__":
    test_with_active_duid()