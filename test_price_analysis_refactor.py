#!/usr/bin/env python3
"""
Test the refactored PriceAnalysisMotor with hybrid query manager
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

from aemo_dashboard.analysis.price_analysis import PriceAnalysisMotor
from aemo_dashboard.shared.logging_config import setup_logging

# Set up logging
setup_logging()


def get_memory_usage():
    """Get current memory usage in MB"""
    process = psutil.Process()
    return process.memory_info().rss / 1024 / 1024


def test_price_analysis_motor():
    """Test the refactored price analysis motor"""
    print("="*60)
    print("TESTING REFACTORED PRICE ANALYSIS MOTOR")
    print("="*60)
    
    start_memory = get_memory_usage()
    print(f"Initial memory: {start_memory:.1f} MB")
    
    # Create motor
    print("\n1. Creating PriceAnalysisMotor...")
    t1 = time.time()
    motor = PriceAnalysisMotor()
    t1_duration = time.time() - t1
    print(f"   ✓ Motor created in {t1_duration:.2f}s")
    
    # Test data loading (metadata check)
    print("\n2. Loading data (metadata check)...")
    t2 = time.time()
    if motor.load_data(use_30min_data=True):
        t2_duration = time.time() - t2
        print(f"   ✓ Data available in {t2_duration:.2f}s")
        print(f"   ✓ Resolution: {motor.resolution}")
        print(f"   ✓ Date ranges loaded: {list(motor.date_ranges.keys())}")
    else:
        print("   ✗ Failed to load data")
        return
    
    # Test data integration with date range
    print("\n3. Integrating data for last 7 days...")
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    t3 = time.time()
    if motor.integrate_data(
        start_date=start_date.strftime('%Y-%m-%d'),
        end_date=end_date.strftime('%Y-%m-%d')
    ):
        t3_duration = time.time() - t3
        print(f"   ✓ Data integrated in {t3_duration:.2f}s")
        print(f"   ✓ Rows loaded: {len(motor.integrated_data):,}")
        print(f"   ✓ Columns: {list(motor.integrated_data.columns[:5])}...")
        
        # Check memory after integration
        current_memory = get_memory_usage()
        print(f"   ✓ Memory after integration: {current_memory:.1f} MB (Δ{current_memory-start_memory:+.1f} MB)")
    else:
        print("   ✗ Failed to integrate data")
        return
    
    # Test cache hit
    print("\n4. Testing cache performance...")
    t4 = time.time()
    if motor.integrate_data(
        start_date=start_date.strftime('%Y-%m-%d'),
        end_date=end_date.strftime('%Y-%m-%d')
    ):
        t4_duration = time.time() - t4
        print(f"   ✓ Cache hit in {t4_duration:.2f}s (speedup: {t3_duration/t4_duration:.1f}x)")
    
    # Test aggregation
    print("\n5. Testing aggregation...")
    hierarchies = motor.get_available_hierarchies()
    print(f"   ✓ Available hierarchies: {list(hierarchies.keys())}")
    
    if 'Fuel Type' in hierarchies:
        t5 = time.time()
        result = motor.calculate_aggregated_prices(hierarchies['Fuel Type'])
        t5_duration = time.time() - t5
        
        print(f"   ✓ Aggregated by fuel type in {t5_duration:.2f}s")
        print(f"   ✓ Result shape: {result.shape}")
        print(f"   ✓ Fuel types: {result['fuel_type'].nunique()}")
        
        # Show top 5 by revenue
        print("\n   Top 5 fuel types by revenue:")
        top_5 = result.head(5)[['fuel_type', 'generation_mwh', 'total_revenue_dollars', 'average_price_per_mwh']]
        for _, row in top_5.iterrows():
            print(f"     {row['fuel_type']:<15} {row['generation_mwh']:>10,.0f} MWh  ${row['total_revenue_dollars']:>12,.0f}  ${row['average_price_per_mwh']:>6.2f}/MWh")
    
    # Test date range query
    print("\n6. Testing date range query...")
    start_range, end_range = motor.get_available_date_range()
    print(f"   ✓ Available date range: {start_range} to {end_range}")
    
    # Test larger date range
    print("\n7. Testing larger date range (30 days)...")
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    t6 = time.time()
    if motor.integrate_data(
        start_date=start_date.strftime('%Y-%m-%d'),
        end_date=end_date.strftime('%Y-%m-%d'),
        force_reload=True
    ):
        t6_duration = time.time() - t6
        print(f"   ✓ 30-day data loaded in {t6_duration:.2f}s")
        print(f"   ✓ Rows: {len(motor.integrated_data):,}")
        
        final_memory = get_memory_usage()
        print(f"   ✓ Final memory: {final_memory:.1f} MB (Δ{final_memory-start_memory:+.1f} MB)")
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Total memory used: {final_memory - start_memory:.1f} MB")
    print(f"Cache statistics: {motor.query_manager.get_statistics()}")
    
    if final_memory - start_memory < 1000:  # Less than 1GB
        print("\n✅ Memory usage is excellent!")
    else:
        print("\n⚠️  Memory usage is higher than expected")
    
    print("\n✅ PriceAnalysisMotor refactoring successful!")


if __name__ == "__main__":
    test_price_analysis_motor()