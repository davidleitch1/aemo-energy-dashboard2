#!/usr/bin/env python3
"""
Diagnose memory usage issue with parquet file loading
"""

import sys
import gc
import os
import psutil
import pandas as pd
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from aemo_dashboard.shared.config import config

def get_memory_mb():
    """Get current process memory in MB"""
    return psutil.Process().memory_info().rss / 1024 / 1024

def test_basic_parquet_load():
    """Test basic parquet loading to understand memory usage"""
    print("="*60)
    print("TESTING BASIC PARQUET FILE LOADING")
    print("="*60)
    
    # File paths
    gen_30_path = str(config.gen_output_file).replace('scada5.parquet', 'scada30.parquet')
    price_30_path = str(config.spot_hist_file).replace('prices5.parquet', 'prices30.parquet')
    
    # Initial memory
    gc.collect()
    initial_mem = get_memory_mb()
    print(f"\nInitial memory: {initial_mem:.1f} MB")
    
    # Test 1: Load generation data without optimization
    print("\nTest 1: Loading generation data (no optimization)...")
    df1 = pd.read_parquet(gen_30_path)
    mem_after_gen = get_memory_mb()
    df1_memory = df1.memory_usage(deep=True).sum() / 1024 / 1024
    print(f"  DataFrame shape: {df1.shape}")
    print(f"  DataFrame memory (reported): {df1_memory:.1f} MB")
    print(f"  Process memory increase: {mem_after_gen - initial_mem:.1f} MB")
    print(f"  Overhead ratio: {(mem_after_gen - initial_mem) / df1_memory:.1f}x")
    
    # Check data types
    print(f"\n  Data types:")
    for col in df1.columns:
        dtype = str(df1[col].dtype)
        mem_mb = df1[col].memory_usage(deep=True) / 1024 / 1024
        print(f"    {col}: {dtype} ({mem_mb:.1f} MB)")
    
    # Clean up
    del df1
    gc.collect()
    
    # Test 2: Load with optimization
    print("\n\nTest 2: Loading generation data (with optimization)...")
    mem_before_opt = get_memory_mb()
    
    df2 = pd.read_parquet(gen_30_path)
    # Apply optimizations
    df2['duid'] = df2['duid'].astype('category')
    df2['scadavalue'] = pd.to_numeric(df2['scadavalue'], downcast='float')
    
    mem_after_opt = get_memory_mb()
    df2_memory = df2.memory_usage(deep=True).sum() / 1024 / 1024
    print(f"  DataFrame shape: {df2.shape}")
    print(f"  DataFrame memory (reported): {df2_memory:.1f} MB")
    print(f"  Process memory increase: {mem_after_opt - mem_before_opt:.1f} MB")
    print(f"  Overhead ratio: {(mem_after_opt - mem_before_opt) / df2_memory:.1f}x")
    
    # Check optimized data types
    print(f"\n  Optimized data types:")
    for col in df2.columns:
        dtype = str(df2[col].dtype)
        mem_mb = df2[col].memory_usage(deep=True) / 1024 / 1024
        print(f"    {col}: {dtype} ({mem_mb:.1f} MB)")
    
    # Test 3: Check file size on disk
    print("\n\nTest 3: File sizes on disk...")
    for name, path in [("Generation 30min", gen_30_path), ("Prices 30min", price_30_path)]:
        if os.path.exists(path):
            size_mb = os.path.getsize(path) / 1024 / 1024
            print(f"  {name}: {size_mb:.1f} MB on disk")
    
    # Test 4: Memory-mapped reading
    print("\n\nTest 4: Testing memory-mapped reading...")
    mem_before_mmap = get_memory_mb()
    
    # Try reading with memory mapping
    df3 = pd.read_parquet(gen_30_path, memory_map=True)
    mem_after_mmap = get_memory_mb()
    print(f"  Memory increase with memory mapping: {mem_after_mmap - mem_before_mmap:.1f} MB")
    
    # Final memory
    final_mem = get_memory_mb()
    print(f"\nFinal memory: {final_mem:.1f} MB")
    print(f"Total memory increase: {final_mem - initial_mem:.1f} MB")

def test_pyarrow_vs_pandas():
    """Test different parquet engines"""
    print("\n\n" + "="*60)
    print("TESTING PARQUET ENGINES")
    print("="*60)
    
    gen_30_path = str(config.gen_output_file).replace('scada5.parquet', 'scada30.parquet')
    
    # Test with pandas engine (if available)
    gc.collect()
    initial = get_memory_mb()
    
    try:
        import pyarrow.parquet as pq
        
        print("\nUsing PyArrow directly...")
        table = pq.read_table(gen_30_path)
        pyarrow_mem = get_memory_mb() - initial
        print(f"  Memory with PyArrow: {pyarrow_mem:.1f} MB")
        
        # Convert to pandas
        df = table.to_pandas()
        pandas_mem = get_memory_mb() - initial
        print(f"  Memory after to_pandas: {pandas_mem:.1f} MB")
        
        del table, df
        gc.collect()
        
    except ImportError:
        print("PyArrow not available")

if __name__ == "__main__":
    test_basic_parquet_load()
    test_pyarrow_vs_pandas()
    
    print("\n\nDiagnosis complete!")