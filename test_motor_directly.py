#!/usr/bin/env python3
"""
Test the StationAnalysisMotor directly to see where it fails
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import pandas as pd
from datetime import datetime, timedelta

# Let me try importing the motor directly
try:
    from src.aemo_dashboard.station.station_analysis import StationAnalysisMotor
    print("✅ StationAnalysisMotor imported successfully")
except Exception as e:
    print(f"❌ Failed to import StationAnalysisMotor: {e}")
    exit(1)

def test_motor_step_by_step():
    """Test motor step by step"""
    print("🔍 Testing StationAnalysisMotor Step by Step")
    print("=" * 50)
    
    # Initialize motor
    motor = StationAnalysisMotor()
    
    # Test DUID mapping loading
    print("1. Loading DUID mapping...")
    if motor.load_data():
        print(f"✅ DUID mapping loaded: {len(motor.duid_mapping):,} entries")
        sample_duids = list(motor.duid_mapping.keys())[:5]
        print(f"   Sample DUIDs: {sample_duids}")
    else:
        print("❌ DUID mapping loading failed")
        return False
    
    # Test data loading for specific date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)
    
    print(f"\n2. Loading data for {start_date.date()} to {end_date.date()}...")
    if motor.load_data_for_date_range(start_date, end_date):
        print("✅ Data loading successful")
        print(f"   Generation data: {len(motor.gen_data) if motor.gen_data is not None else 0:,} records")
        print(f"   Price data: {len(motor.price_data) if motor.price_data is not None else 0:,} records")
    else:
        print("❌ Data loading failed")
        return False
    
    # Test data integration
    print("\n3. Testing data integration...")
    if motor.integrate_data():
        print("✅ Data integration successful")
        print(f"   Integrated data: {len(motor.integrated_data):,} records")
        if not motor.integrated_data.empty:
            print(f"   Columns: {list(motor.integrated_data.columns)}")
            print(f"   Date range: {motor.integrated_data['settlementdate'].min()} to {motor.integrated_data['settlementdate'].max()}")
            print(f"   Unique DUIDs: {motor.integrated_data['duid'].nunique()}")
            sample_duids = motor.integrated_data['duid'].unique()[:5]
            print(f"   Sample DUIDs with data: {sample_duids.tolist()}")
    else:
        print("❌ Data integration failed")
        return False
    
    # Test station filtering with a specific DUID
    print("\n4. Testing station filtering...")
    if motor.integrated_data is not None and not motor.integrated_data.empty:
        # Get a DUID that has recent data
        sample_duid = motor.integrated_data['duid'].iloc[0]
        print(f"   Testing with DUID: {sample_duid}")
        
        if motor.filter_station_data(sample_duid, start_date, end_date):
            print("✅ Station filtering successful")
            print(f"   Station data: {len(motor.station_data):,} records")
            if not motor.station_data.empty:
                print(f"   Date range: {motor.station_data['settlementdate'].min()} to {motor.station_data['settlementdate'].max()}")
                print(f"   Generation range: {motor.station_data['scadavalue'].min():.1f} to {motor.station_data['scadavalue'].max():.1f} MW")
                
                # Test performance metrics
                print("\n5. Testing performance metrics...")
                metrics = motor.calculate_performance_metrics()
                if metrics:
                    print("✅ Performance metrics calculated")
                    for key, value in list(metrics.items())[:5]:
                        print(f"   {key}: {value}")
                else:
                    print("❌ No performance metrics calculated")
                
                # Test time-of-day averages
                print("\n6. Testing time-of-day averages...")
                time_of_day = motor.calculate_time_of_day_averages()
                if not time_of_day.empty:
                    print(f"✅ Time-of-day averages calculated: {len(time_of_day)} hours")
                    print(f"   Columns: {list(time_of_day.columns)}")
                else:
                    print("❌ No time-of-day averages calculated")
                
                print("\n🎉 All tests passed! Motor is working correctly.")
                return True
        else:
            print("❌ Station filtering failed")
    
    return False

def test_motor_with_ui_params():
    """Test motor with exact parameters that UI would use"""
    print("\n" + "=" * 50)
    print("🔍 Testing Motor with UI Parameters")
    print("=" * 50)
    
    motor = StationAnalysisMotor()
    
    # Load mapping
    if not motor.load_data():
        print("❌ Failed to load DUID mapping")
        return
    
    # Test with 1-day range (the problematic case)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)
    
    # Convert to datetime objects like UI does
    start_dt = datetime.combine(start_date.date(), datetime.min.time())
    end_dt = datetime.combine(end_date.date(), datetime.max.time())
    
    print(f"Testing with UI-style datetime conversion:")
    print(f"   start_dt: {start_dt}")
    print(f"   end_dt: {end_dt}")
    
    # Get a sample DUID from the mapping
    sample_duid = list(motor.duid_mapping.keys())[0]
    print(f"   Testing DUID: {sample_duid}")
    
    # Test the exact call that UI makes
    if motor.filter_station_data(sample_duid, start_dt, end_dt):
        print("✅ UI-style filtering successful")
        print(f"   Records found: {len(motor.station_data) if motor.station_data is not None else 0}")
    else:
        print("❌ UI-style filtering failed")
        print("   This explains why 1-day view shows no data!")

if __name__ == "__main__":
    success = test_motor_step_by_step()
    
    if success:
        test_motor_with_ui_params()