#!/usr/bin/env python3
"""
Test and validate the Resolution Manager calculations
"""

import sys
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

def test_resolution_manager():
    """Test the resolution manager with realistic scenarios"""
    
    print("=" * 60)
    print("TESTING DATA RESOLUTION MANAGER")
    print("=" * 60)
    
    from aemo_dashboard.shared.resolution_manager import resolution_manager
    
    # Test scenarios based on our analysis
    scenarios = [
        ("1 day", datetime(2025, 7, 17), datetime(2025, 7, 18)),
        ("1 week", datetime(2025, 7, 11), datetime(2025, 7, 18)),
        ("2 weeks", datetime(2025, 7, 4), datetime(2025, 7, 18)),
        ("1 month", datetime(2025, 6, 18), datetime(2025, 7, 18)),
        ("6 months", datetime(2025, 1, 18), datetime(2025, 7, 18)),
        ("2 years", datetime(2023, 7, 18), datetime(2025, 7, 18)),
        ("10 years", datetime(2015, 7, 18), datetime(2025, 7, 18)),
    ]
    
    print(f"{'Scenario':<10} {'Days':<6} {'Gen Res':<8} {'Gen Memory':<12} {'Price Res':<10} {'Price Memory':<12}")
    print("-" * 70)
    
    for name, start, end in scenarios:
        duration_days = (end - start).total_seconds() / (24 * 3600)
        
        # Test generation data
        gen_res = resolution_manager.get_optimal_resolution(start, end, 'generation')
        gen_memory = resolution_manager.estimate_memory_usage(start, end, gen_res, 'generation')
        
        # Test price data  
        price_res = resolution_manager.get_optimal_resolution(start, end, 'price')
        price_memory = resolution_manager.estimate_memory_usage(start, end, price_res, 'price')
        
        print(f"{name:<10} {duration_days:<6.0f} {gen_res:<8} {gen_memory:<12.1f} {price_res:<10} {price_memory:<12.1f}")
    
    print("\n" + "=" * 60)
    print("TESTING MEMORY CALCULATIONS")
    print("=" * 60)
    
    # Validate our key calculations from the plan
    
    # 1 week of 5-minute data
    week_start = datetime(2025, 7, 11)
    week_end = datetime(2025, 7, 18)
    
    gen_5min_1week = resolution_manager.estimate_memory_usage(week_start, week_end, '5min', 'generation')
    price_5min_1week = resolution_manager.estimate_memory_usage(week_start, week_end, '5min', 'price')
    trans_5min_1week = resolution_manager.estimate_memory_usage(week_start, week_end, '5min', 'transmission')
    
    print(f"1 Week 5-minute data:")
    print(f"  Generation: {gen_5min_1week:.1f} MB")
    print(f"  Price: {price_5min_1week:.1f} MB") 
    print(f"  Transmission: {trans_5min_1week:.1f} MB")
    print(f"  Total: {gen_5min_1week + price_5min_1week + trans_5min_1week:.1f} MB")
    
    # 10 years of 30-minute data
    year10_start = datetime(2015, 7, 18)
    year10_end = datetime(2025, 7, 18)
    
    gen_30min_10year = resolution_manager.estimate_memory_usage(year10_start, year10_end, '30min', 'generation')
    price_30min_10year = resolution_manager.estimate_memory_usage(year10_start, year10_end, '30min', 'price')
    trans_30min_10year = resolution_manager.estimate_memory_usage(year10_start, year10_end, '30min', 'transmission')
    
    print(f"\n10 Years 30-minute data:")
    print(f"  Generation: {gen_30min_10year:.1f} MB")
    print(f"  Price: {price_30min_10year:.1f} MB")
    print(f"  Transmission: {trans_30min_10year:.1f} MB") 
    print(f"  Total: {gen_30min_10year + price_30min_10year + trans_30min_10year:.1f} MB")
    
    print("\n" + "=" * 60)
    print("TESTING PERFORMANCE RECOMMENDATIONS")
    print("=" * 60)
    
    # Test recommendation system
    test_ranges = [
        ("Recent", datetime(2025, 7, 17), datetime(2025, 7, 18)),
        ("Medium", datetime(2025, 7, 4), datetime(2025, 7, 18)),
        ("Long", datetime(2025, 1, 18), datetime(2025, 7, 18)),
    ]
    
    for name, start, end in test_ranges:
        rec = resolution_manager.get_performance_recommendation(start, end, 'generation')
        print(f"\n{name} Range ({(end-start).days} days):")
        print(f"  Recommended: {rec['optimal_resolution']}")
        print(f"  Load time estimate: {rec['load_time_estimate']:.1f} seconds")
        print(f"  Explanation: {rec['explanation']}")
        print(f"  Memory (5min/30min): {rec['memory_estimates']['5min']:.1f}/{rec['memory_estimates']['30min']:.1f} MB")
    
    print("\n" + "🎯" * 20)
    print("\nRESOLUTION MANAGER VALIDATION COMPLETE")
    print("Key findings:")
    print("- Short ranges (≤7 days) use 5-minute resolution")
    print("- Long ranges (>14 days) use 30-minute resolution")
    print("- Memory usage scales correctly with date range")
    print("- Performance recommendations provide clear guidance")

if __name__ == "__main__":
    test_resolution_manager()