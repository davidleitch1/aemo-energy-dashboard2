"""
Memory profiling script to compare original vs optimized data service

This script loads both services and profiles their memory usage.
"""

import sys
import os
import gc
import tracemalloc
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import datetime, timedelta
from aemo_dashboard.shared.logging_config import get_logger

logger = get_logger(__name__)

def profile_original_service():
    """Profile the original shared data service"""
    logger.info("\n" + "="*60)
    logger.info("PROFILING ORIGINAL DATA SERVICE")
    logger.info("="*60)
    
    # Start memory tracking
    tracemalloc.start()
    gc.collect()
    
    # Get initial memory
    initial_memory = tracemalloc.get_traced_memory()[0] / 1024 / 1024  # MB
    logger.info(f"Initial memory: {initial_memory:.1f} MB")
    
    # Import and initialize original service
    from shared_data import SharedDataService
    service = SharedDataService()
    
    # Get memory after loading
    current_memory = tracemalloc.get_traced_memory()[0] / 1024 / 1024  # MB
    memory_used = current_memory - initial_memory
    
    logger.info(f"Memory after loading: {current_memory:.1f} MB")
    logger.info(f"Memory used by service: {memory_used:.1f} MB")
    logger.info(f"Service reported memory: {service.get_memory_usage():.1f} MB")
    
    # Test a query
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    try:
        data = service.get_generation_by_fuel(start_date, end_date, resolution='30min')
        logger.info(f"Query returned {len(data)} rows")
    except Exception as e:
        logger.error(f"Query failed: {e}")
    
    # Final memory
    final_memory = tracemalloc.get_traced_memory()[0] / 1024 / 1024  # MB
    logger.info(f"Final memory: {final_memory:.1f} MB")
    
    tracemalloc.stop()
    
    # Clean up
    del service
    gc.collect()
    
    return memory_used

def profile_optimized_service():
    """Profile the optimized shared data service"""
    logger.info("\n" + "="*60)
    logger.info("PROFILING OPTIMIZED DATA SERVICE")
    logger.info("="*60)
    
    # Start memory tracking
    tracemalloc.start()
    gc.collect()
    
    # Get initial memory
    initial_memory = tracemalloc.get_traced_memory()[0] / 1024 / 1024  # MB
    logger.info(f"Initial memory: {initial_memory:.1f} MB")
    
    # Import and initialize optimized service
    from shared_data_optimized import OptimizedSharedDataService
    service = OptimizedSharedDataService()
    
    # Get memory after loading
    current_memory = tracemalloc.get_traced_memory()[0] / 1024 / 1024  # MB
    memory_used = current_memory - initial_memory
    
    logger.info(f"Memory after loading: {current_memory:.1f} MB")
    logger.info(f"Memory used by service: {memory_used:.1f} MB")
    logger.info(f"Service reported memory: {service.get_memory_usage():.1f} MB")
    
    # Test a query
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    try:
        data = service.get_generation_by_fuel(start_date, end_date, resolution='30min')
        logger.info(f"Query returned {len(data)} rows")
    except Exception as e:
        logger.error(f"Query failed: {e}")
    
    # Final memory
    final_memory = tracemalloc.get_traced_memory()[0] / 1024 / 1024  # MB
    logger.info(f"Final memory: {final_memory:.1f} MB")
    
    tracemalloc.stop()
    
    # Clean up
    del service
    gc.collect()
    
    return memory_used

def main():
    """Main profiling function"""
    logger.info("Starting memory profiling...")
    
    # Profile original service
    original_memory = profile_original_service()
    
    # Force garbage collection between tests
    gc.collect()
    
    # Profile optimized service
    optimized_memory = profile_optimized_service()
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("MEMORY USAGE SUMMARY")
    logger.info("="*60)
    logger.info(f"Original service: {original_memory:.1f} MB")
    logger.info(f"Optimized service: {optimized_memory:.1f} MB")
    logger.info(f"Memory reduction: {(1 - optimized_memory/original_memory)*100:.1f}%")
    logger.info("="*60)

if __name__ == "__main__":
    main()