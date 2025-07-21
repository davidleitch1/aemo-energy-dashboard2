#!/usr/bin/env python3
"""
Simplified test of StationAnalysisMotor without dependencies
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import pandas as pd
import pickle
from datetime import datetime, timedelta

# Mock fuzzywuzzy for testing
class MockFuzzywuzzy:
    def fuzz(self, s1, s2):
        return 100 if s1.lower() in s2.lower() else 50
    
    def process(self, query, choices, limit=5):
        results = []
        for choice in choices[:limit]:
            score = 100 if query.lower() in choice.lower() else 50
            results.append((choice, score))
        return results

# Add to sys.modules
sys.modules['fuzzywuzzy'] = MockFuzzywuzzy()
sys.modules['fuzzywuzzy.fuzz'] = MockFuzzywuzzy()
sys.modules['fuzzywuzzy.process'] = MockFuzzywuzzy()

# Now try importing the motor
try:
    from src.aemo_dashboard.station.station_analysis import StationAnalysisMotor
    print("✅ StationAnalysisMotor imported successfully with mock fuzzywuzzy")
except Exception as e:
    print(f"❌ Failed to import StationAnalysisMotor: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

def test_motor_directly():
    """Test motor directly to find the issue"""
    print("🔍 Testing StationAnalysisMotor Directly")
    print("=" * 40)
    
    # Test the exact scenario that's failing
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)
    
    print(f"Date range: {start_date.date()} to {end_date.date()}")
    
    try:
        # Initialize motor
        motor = StationAnalysisMotor()
        print("✅ Motor initialized")
        
        # Load mapping
        if motor.load_data():
            print(f"✅ DUID mapping loaded: {len(motor.duid_mapping)} entries")
        else:
            print("❌ Failed to load DUID mapping")
            return
        
        # Convert dates like UI does
        start_dt = datetime.combine(start_date.date(), datetime.min.time())
        end_dt = datetime.combine(end_date.date(), datetime.max.time())
        
        print(f"Converted datetime range: {start_dt} to {end_dt}")
        
        # Test with valid DUID from mapping
        # The mapping might be a DataFrame transposed, let's find actual DUIDs
        if isinstance(motor.duid_mapping, dict):
            sample_duid = list(motor.duid_mapping.keys())[0]
        else:
            # It's probably a DataFrame - look for actual DUID values
            sample_duid = None
            for key in motor.duid_mapping.keys():
                if len(key) >= 4 and key not in ['Region', 'Site Name', 'Owner', 'Capacity(MW)', 'Storage(MWh)', 'Fuel']:
                    sample_duid = key
                    break
            
            if sample_duid is None:
                # Try to find DUIDs in the integrated data
                if motor.integrated_data is not None and len(motor.integrated_data) > 0:
                    sample_duid = motor.integrated_data['duid'].iloc[0]
                else:
                    print("❌ Could not find valid DUID to test with")
                    return
        
        print(f"Testing with DUID: {sample_duid}")
        
        # Call filter_station_data like UI does
        print("Calling filter_station_data...")
        result = motor.filter_station_data(sample_duid, start_dt, end_dt)
        
        if result:
            print(f"✅ Filter successful: {len(motor.station_data) if motor.station_data is not None else 0} records")
            
            if motor.station_data is not None and len(motor.station_data) > 0:
                print(f"   Date range: {motor.station_data['settlementdate'].min()} to {motor.station_data['settlementdate'].max()}")
                print(f"   Generation range: {motor.station_data['scadavalue'].min():.1f} to {motor.station_data['scadavalue'].max():.1f} MW")
                print("✅ Data filtering is working correctly!")
            else:
                print("❌ Filter returned True but no data in station_data")
        else:
            print("❌ Filter returned False - this is the issue!")
            
            # Debug why filter failed
            print("\nDebugging filter failure...")
            
            # Check if data was loaded
            if motor.gen_data is None:
                print("   - Generation data not loaded")
            else:
                print(f"   - Generation data: {len(motor.gen_data)} records")
                
            if motor.price_data is None:
                print("   - Price data not loaded")
            else:
                print(f"   - Price data: {len(motor.price_data)} records")
                
            if motor.integrated_data is None:
                print("   - Integrated data not created")
            else:
                print(f"   - Integrated data: {len(motor.integrated_data)} records")
        
    except Exception as e:
        print(f"❌ Error in motor test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_motor_directly()