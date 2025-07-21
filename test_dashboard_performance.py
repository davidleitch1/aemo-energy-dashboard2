#!/usr/bin/env python3
"""
Test dashboard performance with DuckDB adapters
"""

import os
import sys
from pathlib import Path
import time
import psutil
from datetime import datetime, timedelta

# Set DuckDB mode
os.environ['USE_DUCKDB'] = 'true'

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Import logging
from aemo_dashboard.shared.logging_config import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

def measure_time(func):
    """Decorator to measure function execution time"""
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        return result, end - start
    return wrapper

@measure_time
def test_generation_loading():
    """Test generation data loading performance"""
    from aemo_dashboard.shared.adapter_selector import load_generation_data
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    df = load_generation_data(start_date=start_date, end_date=end_date, resolution='30min')
    return len(df)

@measure_time
def test_price_loading():
    """Test price data loading performance"""
    from aemo_dashboard.shared.adapter_selector import load_price_data
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    df = load_price_data(start_date=start_date, end_date=end_date, resolution='30min')
    return len(df)

@measure_time
def test_price_analysis_integration():
    """Test price analysis data integration"""
    from aemo_dashboard.analysis.price_analysis import PriceAnalysisMotor
    
    motor = PriceAnalysisMotor()
    success = motor.load_data(use_30min_data=True)
    
    if success:
        motor.integrate_data()
        return len(motor.integrated_data) if motor.integrated_data is not None else 0
    return 0

@measure_time
def test_station_analysis():
    """Test station analysis performance"""
    from aemo_dashboard.station.station_analysis import StationAnalysisMotor
    
    motor = StationAnalysisMotor()
    motor.load_data()
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    motor.load_data_for_date_range(start_date, end_date)
    return True

def main():
    print("=" * 70)
    print("AEMO DASHBOARD PERFORMANCE TEST WITH DUCKDB")
    print("=" * 70)
    
    # Check adapter type
    from aemo_dashboard.shared.adapter_selector import adapter_type, USE_DUCKDB
    print(f"Adapter type: {adapter_type}")
    print(f"USE_DUCKDB: {USE_DUCKDB}")
    
    # Initial memory
    process = psutil.Process()
    initial_memory = process.memory_info().rss / 1024 / 1024
    print(f"\nInitial memory usage: {initial_memory:.1f} MB")
    
    results = {}
    
    # Test 1: Generation loading
    print("\n1. Testing generation data loading...")
    count, duration = test_generation_loading()
    results['generation'] = {'count': count, 'time': duration}
    print(f"   ✅ Loaded {count:,} records in {duration:.2f}s")
    
    # Test 2: Price loading
    print("\n2. Testing price data loading...")
    count, duration = test_price_loading()
    results['price'] = {'count': count, 'time': duration}
    print(f"   ✅ Loaded {count:,} records in {duration:.2f}s")
    
    # Test 3: Price analysis integration
    print("\n3. Testing price analysis integration...")
    count, duration = test_price_analysis_integration()
    results['integration'] = {'count': count, 'time': duration}
    print(f"   ✅ Integrated {count:,} records in {duration:.2f}s")
    
    # Test 4: Station analysis
    print("\n4. Testing station analysis...")
    success, duration = test_station_analysis()
    results['station'] = {'success': success, 'time': duration}
    print(f"   ✅ Station analysis completed in {duration:.2f}s")
    
    # Final memory
    final_memory = process.memory_info().rss / 1024 / 1024
    memory_increase = final_memory - initial_memory
    
    print("\n" + "=" * 70)
    print("PERFORMANCE SUMMARY")
    print("=" * 70)
    print(f"Initial memory: {initial_memory:.1f} MB")
    print(f"Final memory: {final_memory:.1f} MB")
    print(f"Memory increase: {memory_increase:.1f} MB")
    print(f"\nTotal execution time: {sum(r['time'] for r in results.values()):.2f}s")
    
    print("\nDetailed results:")
    for test, result in results.items():
        print(f"  {test}: {result['time']:.2f}s")
    
    print("\n" + "=" * 70)
    print("CONCLUSION")
    print("=" * 70)
    
    if final_memory < 500:  # Less than 500MB is good
        print("✅ Memory usage is excellent (< 500 MB)")
    elif final_memory < 1000:  # Less than 1GB is acceptable
        print("✅ Memory usage is good (< 1 GB)")
    else:
        print("⚠️  Memory usage is high (> 1 GB)")
    
    total_time = sum(r['time'] for r in results.values())
    if total_time < 5:
        print("✅ Performance is excellent (< 5s total)")
    elif total_time < 10:
        print("✅ Performance is good (< 10s total)")
    else:
        print("⚠️  Performance could be improved (> 10s total)")
    
    print("=" * 70)

if __name__ == "__main__":
    main()