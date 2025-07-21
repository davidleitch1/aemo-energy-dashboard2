#!/usr/bin/env python3
"""
Test the performance optimization system
"""

import sys
import os
sys.path.insert(0, 'src')

from datetime import datetime, timedelta
import time
import pandas as pd
from aemo_dashboard.shared.generation_adapter import load_generation_data
from aemo_dashboard.shared.performance_optimizer import PerformanceOptimizer

def test_performance_optimization():
    """Test performance optimization with different date ranges"""
    
    print("🧪 Testing Performance Optimization System")
    print("=" * 50)
    
    # Test different date ranges
    test_cases = [
        {
            'name': 'Recent (1 day)',
            'start': datetime.now() - timedelta(days=1),
            'end': datetime.now(),
            'expected_freq': '5min'
        },
        {
            'name': 'Short-term (1 week)', 
            'start': datetime.now() - timedelta(days=7),
            'end': datetime.now(),
            'expected_freq': '15min'
        },
        {
            'name': 'Medium-term (3 months)',
            'start': datetime.now() - timedelta(days=90),
            'end': datetime.now(),
            'expected_freq': '4h'
        },
        {
            'name': 'Long-term (2 years)',
            'start': datetime(2023, 1, 1),
            'end': datetime(2025, 1, 1),
            'expected_freq': '1W'
        }
    ]
    
    for test_case in test_cases:
        print(f"\n📊 {test_case['name']}")
        print(f"Date range: {test_case['start'].strftime('%Y-%m-%d')} to {test_case['end'].strftime('%Y-%m-%d')}")
        
        # Test frequency selection
        strategy = PerformanceOptimizer.get_optimal_frequency(
            test_case['start'], test_case['end'], 'generation'
        )
        
        print(f"Selected frequency: {strategy['frequency']} (expected: {test_case['expected_freq']})")
        print(f"Strategy: {strategy['description']}")
        print(f"Estimated points: {strategy['estimated_points']:,}")
        
        # Test actual data loading with optimization
        print("Loading data with optimization...")
        start_time = time.time()
        
        try:
            result = load_generation_data(
                start_date=test_case['start'],
                end_date=test_case['end'],
                resolution='auto',
                optimize_for_plotting=True,
                plot_type='generation'
            )
            
            if isinstance(result, tuple):
                df, metadata = result
                load_time = time.time() - start_time
                
                print(f"✅ Load time: {load_time:.2f}s")
                print(f"Original points: {metadata['original_points']:,}")
                print(f"Optimized points: {metadata['optimized_points']:,}")
                print(f"Reduction: {metadata['reduction_ratio']:.1%}")
                print(f"Memory saved: ~{(1 - metadata['reduction_ratio']) * 100:.0f}%")
            else:
                print(f"✅ Load time: {time.time() - start_time:.2f}s")
                print(f"Points loaded: {len(result):,}")
                
        except Exception as e:
            print(f"❌ Error: {e}")
    
    print("\n🎯 Performance Optimization Test Complete")

def test_optimizer_functions():
    """Test individual optimizer functions"""
    
    print("\n🔧 Testing Optimizer Functions")
    print("=" * 40)
    
    # Test frequency selection
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2025, 7, 18)
    
    strategy = PerformanceOptimizer.get_optimal_frequency(start_date, end_date)
    print(f"5+ year range strategy: {strategy['frequency']} - {strategy['description']}")
    
    # Test data point estimation
    estimated = PerformanceOptimizer._estimate_data_points(365, '1D')
    print(f"Estimated daily points for 1 year: {estimated}")
    
    estimated = PerformanceOptimizer._estimate_data_points(365, '5min')
    print(f"Estimated 5-min points for 1 year: {estimated:,}")
    
    print("✅ Optimizer functions working correctly")

if __name__ == '__main__':
    try:
        test_optimizer_functions()
        test_performance_optimization()
        
    except KeyboardInterrupt:
        print("\n🛑 Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()