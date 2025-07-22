#!/usr/bin/env python3
"""
Direct test of optimized data service memory usage
"""

import sys
import os
import gc
import psutil
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from datetime import datetime, timedelta

def get_memory_usage():
    """Get current process memory usage in MB"""
    process = psutil.Process()
    return process.memory_info().rss / 1024 / 1024

def test_original_service():
    """Test original service memory usage"""
    print("\n" + "="*60)
    print("TESTING ORIGINAL DATA SERVICE")
    print("="*60)
    
    # Get initial memory
    gc.collect()
    initial_memory = get_memory_usage()
    print(f"Initial memory: {initial_memory:.1f} MB")
    
    # Import and initialize original service
    from data_service.shared_data import SharedDataService
    service = SharedDataService()
    
    # Get memory after loading
    loaded_memory = get_memory_usage()
    memory_used = loaded_memory - initial_memory
    
    print(f"Memory after loading: {loaded_memory:.1f} MB")
    print(f"Memory used by service: {memory_used:.1f} MB")
    print(f"Service reported memory: {service.get_memory_usage():.1f} MB")
    
    # Test a query
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    try:
        data = service.get_generation_by_fuel(start_date, end_date, resolution='30min')
        print(f"Query returned {len(data)} rows")
    except Exception as e:
        print(f"Query failed: {e}")
    
    # Final memory
    final_memory = get_memory_usage()
    print(f"Final memory: {final_memory:.1f} MB")
    
    # Clean up
    del service
    gc.collect()
    
    return memory_used

def test_optimized_service():
    """Test optimized service memory usage"""
    print("\n" + "="*60)
    print("TESTING OPTIMIZED DATA SERVICE")
    print("="*60)
    
    # Get initial memory
    gc.collect()
    initial_memory = get_memory_usage()
    print(f"Initial memory: {initial_memory:.1f} MB")
    
    # Import and initialize optimized service
    from data_service.shared_data_optimized import OptimizedSharedDataService
    service = OptimizedSharedDataService()
    
    # Get memory after loading
    loaded_memory = get_memory_usage()
    memory_used = loaded_memory - initial_memory
    
    print(f"Memory after loading: {loaded_memory:.1f} MB")
    print(f"Memory used by service: {memory_used:.1f} MB")
    print(f"Service reported memory: {service.get_memory_usage():.1f} MB")
    
    # Test a query
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    try:
        data = service.get_generation_by_fuel(start_date, end_date, resolution='30min')
        print(f"Query returned {len(data)} rows")
    except Exception as e:
        print(f"Query failed: {e}")
    
    # Test 5-minute data loading
    print("\nTesting 5-minute data loading...")
    service.load_5min_data_on_demand('generation')
    after_5min_memory = get_memory_usage()
    print(f"Memory after loading 5min data: {after_5min_memory:.1f} MB")
    print(f"Additional memory for 5min data: {after_5min_memory - loaded_memory:.1f} MB")
    
    # Final memory
    final_memory = get_memory_usage()
    print(f"Final memory: {final_memory:.1f} MB")
    
    return memory_used

def main():
    """Main test function"""
    print("="*60)
    print("AEMO DATA SERVICE MEMORY COMPARISON")
    print("="*60)
    
    # Install psutil if needed
    try:
        import psutil
    except ImportError:
        print("Installing psutil for memory monitoring...")
        os.system(f"{sys.executable} -m pip install psutil")
        import psutil
    
    # Test optimized service first (to avoid memory fragmentation)
    print("\nStarting with optimized service...")
    optimized_memory = test_optimized_service()
    
    # Force garbage collection
    gc.collect()
    
    # Test original service
    print("\nNow testing original service...")
    print("(This may take a while and use significant memory)")
    try:
        original_memory = test_original_service()
    except Exception as e:
        print(f"Error testing original service: {e}")
        original_memory = None
    
    # Summary
    print("\n" + "="*60)
    print("MEMORY USAGE SUMMARY")
    print("="*60)
    
    if original_memory:
        print(f"Original service:  {original_memory:8.1f} MB")
        print(f"Optimized service: {optimized_memory:8.1f} MB")
        print(f"Memory reduction:  {original_memory - optimized_memory:8.1f} MB")
        print(f"Reduction %:       {(1 - optimized_memory/original_memory)*100:8.1f}%")
    else:
        print(f"Optimized service: {optimized_memory:8.1f} MB")
        print("Original service test failed or skipped")
    
    print("="*60)

if __name__ == "__main__":
    main()