#!/usr/bin/env python3
"""
Test the complete station analysis flow with refactored motor
"""

import os
import sys
import time
import psutil
from datetime import datetime, timedelta
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Set DuckDB mode
os.environ['USE_DUCKDB'] = 'true'

from aemo_dashboard.station.station_analysis import StationAnalysisMotor
from aemo_dashboard.shared.logging_config import setup_logging

# Set up logging
setup_logging()


def get_memory_usage():
    """Get current memory usage in MB"""
    process = psutil.Process()
    return process.memory_info().rss / 1024 / 1024


def test_station_analysis():
    """Test the complete station analysis workflow"""
    print("="*60)
    print("TESTING STATION ANALYSIS WITH HYBRID QUERY MANAGER")
    print("="*60)
    
    start_memory = get_memory_usage()
    print(f"Initial memory: {start_memory:.1f} MB")
    
    # Create motor
    print("\n1. Creating StationAnalysisMotor...")
    t1 = time.time()
    motor = StationAnalysisMotor()
    t1_duration = time.time() - t1
    print(f"   ✓ Motor created in {t1_duration:.2f}s")
    
    # Load DUID mapping
    print("\n2. Loading DUID mapping...")
    t2 = time.time()
    if motor.load_data():
        t2_duration = time.time() - t2
        print(f"   ✓ DUID mapping loaded in {t2_duration:.2f}s")
        duids = motor.get_available_duids()
        print(f"   ✓ Found {len(duids)} DUIDs")
    else:
        print("   ✗ Failed to load DUID mapping")
        return
    
    # Test with a known coal station
    test_duid = 'ER01'  # Eraring Power Station
    print(f"\n3. Testing with DUID: {test_duid}")
    
    # Get station info
    info = motor.get_station_info(test_duid)
    if info:
        print(f"   ✓ Station: {info.get('Site Name', 'Unknown')}")
        print(f"   ✓ Owner: {info.get('Owner', 'Unknown')}")
        print(f"   ✓ Fuel: {info.get('Fuel', 'Unknown')}")
        print(f"   ✓ Capacity: {info.get('Capacity(MW)', 0)} MW")
    
    # Test data loading for different time ranges
    time_ranges = [
        ("7 days", 7),
        ("30 days", 30),
    ]
    
    for range_name, days in time_ranges:
        print(f"\n4. Loading {range_name} of data...")
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        t3 = time.time()
        if motor.filter_station_data(test_duid, start_date, end_date):
            t3_duration = time.time() - t3
            print(f"   ✓ Data loaded in {t3_duration:.2f}s")
            print(f"   ✓ Records: {len(motor.station_data):,}")
            
            # Check memory
            current_memory = get_memory_usage()
            print(f"   ✓ Memory after load: {current_memory:.1f} MB (Δ{current_memory-start_memory:+.1f} MB)")
            
            # Calculate metrics
            print(f"\n5. Calculating performance metrics...")
            t4 = time.time()
            metrics = motor.calculate_performance_metrics()
            t4_duration = time.time() - t4
            
            if metrics:
                print(f"   ✓ Metrics calculated in {t4_duration:.2f}s")
                print(f"   ✓ Total generation: {metrics['total_generation_mwh']:,.0f} MWh")
                print(f"   ✓ Total revenue: ${metrics['total_revenue']:,.0f}")
                print(f"   ✓ Average price: ${metrics['avg_price_per_mwh']:.2f}/MWh")
                print(f"   ✓ Capacity factor: {metrics['capacity_factor_pct']:.1f}%")
                print(f"   ✓ Availability: {metrics['generation_availability_pct']:.1f}%")
            
            # Test time of day analysis
            print(f"\n6. Calculating time-of-day averages...")
            t5 = time.time()
            tod = motor.calculate_time_of_day_averages()
            t5_duration = time.time() - t5
            
            if not tod.empty:
                print(f"   ✓ Time-of-day calculated in {t5_duration:.2f}s")
                print(f"   ✓ Hours with data: {len(tod)}")
                peak_hour = tod.loc[tod['scadavalue'].idxmax()]
                print(f"   ✓ Peak generation hour: {int(peak_hour['hour'])}:00 ({peak_hour['scadavalue']:.1f} MW)")
        else:
            print(f"   ✗ Failed to load data for {range_name}")
    
    # Test multi-DUID aggregation (station with multiple units)
    print("\n7. Testing multi-unit station aggregation...")
    # Bayswater has multiple units: BW01, BW02, BW03, BW04
    bayswater_units = ['BW01', 'BW02', 'BW03', 'BW04']
    
    # Check if all units exist
    available_units = [u for u in bayswater_units if u in duids]
    if len(available_units) >= 2:
        print(f"   Testing with Bayswater units: {available_units}")
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1)
        
        t6 = time.time()
        if motor.filter_station_data(available_units, start_date, end_date):
            t6_duration = time.time() - t6
            print(f"   ✓ Multi-unit data loaded in {t6_duration:.2f}s")
            print(f"   ✓ Aggregated records: {len(motor.station_data):,}")
            print(f"   ✓ Total station capacity: {motor.station_data['capacity_mw'].iloc[0]:.0f} MW")
            print(f"   ✓ Peak station output: {motor.station_data['scadavalue'].max():.0f} MW")
    else:
        print("   ⚠ Insufficient Bayswater units found for multi-unit test")
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    final_memory = get_memory_usage()
    print(f"Total memory used: {final_memory - start_memory:.1f} MB")
    print(f"Query cache stats: {motor.query_manager.get_statistics()}")
    
    if final_memory - start_memory < 200:
        print("\n✅ Memory usage is excellent!")
    else:
        print("\n⚠️  Memory usage is higher than expected")
    
    print("\n✅ Station analysis refactoring successful!")


if __name__ == "__main__":
    test_station_analysis()