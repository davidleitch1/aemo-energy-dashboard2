#!/usr/bin/env python3
"""
Test startup performance of original vs optimized dashboard
"""

import time
import sys
import os
from pathlib import Path

# Set environment
os.environ['USE_DUCKDB'] = 'true'
sys.path.insert(0, str(Path(__file__).parent / 'src'))

def test_original_dashboard():
    """Test original dashboard startup time"""
    print("Testing original dashboard...")
    start = time.time()
    
    try:
        from aemo_dashboard.generation.gen_dash import EnergyDashboard
        dashboard = EnergyDashboard()
        init_time = time.time() - start
        
        # Test dashboard creation
        start_create = time.time()
        app = dashboard.create_dashboard()
        create_time = time.time() - start_create
        
        print(f"  ✓ Original init: {init_time:.2f}s")
        print(f"  ✓ Original create: {create_time:.2f}s")
        print(f"  ✓ Original total: {init_time + create_time:.2f}s")
        
        return init_time + create_time
        
    except Exception as e:
        print(f"  ✗ Original failed: {e}")
        return None

def test_optimized_dashboard():
    """Test optimized dashboard startup time"""
    print("Testing optimized dashboard...")
    start = time.time()
    
    try:
        from aemo_dashboard.generation.gen_dash_optimized import OptimizedEnergyDashboard
        dashboard = OptimizedEnergyDashboard()
        init_time = time.time() - start
        
        # Test dashboard creation
        start_create = time.time()
        app = dashboard.create_dashboard()
        create_time = time.time() - start_create
        
        print(f"  ✓ Optimized init: {init_time:.2f}s")
        print(f"  ✓ Optimized create: {create_time:.2f}s")
        print(f"  ✓ Optimized total: {init_time + create_time:.2f}s")
        
        return init_time + create_time
        
    except Exception as e:
        print(f"  ✗ Optimized failed: {e}")
        return None

def main():
    """Run performance comparison"""
    print("AEMO Dashboard Startup Performance Test")
    print("=" * 50)
    
    # Test original
    original_time = test_original_dashboard()
    print()
    
    # Test optimized  
    optimized_time = test_optimized_dashboard()
    print()
    
    # Compare results
    if original_time and optimized_time:
        improvement = (original_time - optimized_time) / original_time * 100
        speedup = original_time / optimized_time
        
        print("Performance Comparison:")
        print(f"  Original:   {original_time:.2f}s")
        print(f"  Optimized:  {optimized_time:.2f}s")
        print(f"  Improvement: {improvement:.1f}% faster")
        print(f"  Speedup:     {speedup:.1f}x faster")
        
        # Expected targets
        print("\nTargets:")
        if optimized_time < 1.0:
            print("  ✓ Sub-second startup achieved!")
        else:
            print(f"  ⚠ Target: <1s, Actual: {optimized_time:.2f}s")
            
        if optimized_time < original_time * 0.2:
            print("  ✓ 80%+ improvement achieved!")
        else:
            print(f"  ⚠ Target: 80%+ improvement, Actual: {improvement:.1f}%")
    
    print("\nOptimizations implemented:")
    print("  ✓ Lazy tab loading")
    print("  ✓ Shared query managers")
    print("  ✓ Minimal initial data loading")
    print("  ✓ Progressive component loading")
    print("  ✓ Background initialization")

if __name__ == "__main__":
    main()