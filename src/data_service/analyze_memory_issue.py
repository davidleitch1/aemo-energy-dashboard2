"""
Analyze memory issue in the shared data service

This script performs detailed analysis to understand why the service
is using 21GB instead of expected 200MB.
"""

import sys
import os
import gc
import pandas as pd
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from aemo_dashboard.shared.config import config
from aemo_dashboard.shared.logging_config import get_logger

logger = get_logger(__name__)

def analyze_file_sizes():
    """Check actual file sizes on disk"""
    logger.info("\n" + "="*60)
    logger.info("ANALYZING FILE SIZES ON DISK")
    logger.info("="*60)
    
    files_to_check = [
        ('Generation 5min', config.gen_output_file),
        ('Generation 30min', str(config.gen_output_file).replace('scada5.parquet', 'scada30.parquet')),
        ('Prices 5min', config.spot_hist_file),
        ('Prices 30min', str(config.spot_hist_file).replace('prices5.parquet', 'prices30.parquet')),
        ('Transmission 5min', config.transmission_output_file),
        ('Transmission 30min', str(config.transmission_output_file).replace('transmission5.parquet', 'transmission30.parquet')),
        ('Rooftop Solar', config.rooftop_solar_file),
        ('DUID Mapping', config.gen_info_file)
    ]
    
    total_size_mb = 0
    
    for name, file_path in files_to_check:
        try:
            if os.path.exists(file_path):
                size_bytes = os.path.getsize(file_path)
                size_mb = size_bytes / 1024 / 1024
                total_size_mb += size_mb
                logger.info(f"{name:20s}: {size_mb:8.1f} MB ({file_path})")
            else:
                logger.warning(f"{name:20s}: FILE NOT FOUND ({file_path})")
        except Exception as e:
            logger.error(f"Error checking {name}: {e}")
    
    logger.info(f"\nTotal file size on disk: {total_size_mb:.1f} MB")
    return total_size_mb

def analyze_dataframe_memory():
    """Analyze memory usage of loaded DataFrames"""
    logger.info("\n" + "="*60)
    logger.info("ANALYZING DATAFRAME MEMORY USAGE")
    logger.info("="*60)
    
    # Load and analyze each file
    files_to_analyze = [
        ('Generation 30min', str(config.gen_output_file).replace('scada5.parquet', 'scada30.parquet')),
        ('Generation 5min', config.gen_output_file),
        ('Prices 30min', str(config.spot_hist_file).replace('prices5.parquet', 'prices30.parquet')),
        ('Prices 5min', config.spot_hist_file),
    ]
    
    total_memory_mb = 0
    
    for name, file_path in files_to_analyze:
        try:
            if os.path.exists(file_path):
                logger.info(f"\nAnalyzing {name}...")
                
                # Load the dataframe
                df = pd.read_parquet(file_path)
                
                # Get basic info
                rows, cols = df.shape
                logger.info(f"  Shape: {rows:,} rows x {cols} columns")
                
                # Memory usage
                memory_bytes = df.memory_usage(deep=True).sum()
                memory_mb = memory_bytes / 1024 / 1024
                total_memory_mb += memory_mb
                logger.info(f"  Memory usage: {memory_mb:.1f} MB")
                
                # Memory per column
                logger.info("  Memory by column:")
                for col in df.columns:
                    col_memory = df[col].memory_usage(deep=True) / 1024 / 1024
                    dtype = str(df[col].dtype)
                    logger.info(f"    {col:20s}: {col_memory:8.1f} MB ({dtype})")
                
                # Check for duplicates
                if 'settlementdate' in df.columns and 'duid' in df.columns:
                    duplicates = df.duplicated(subset=['settlementdate', 'duid']).sum()
                    if duplicates > 0:
                        logger.warning(f"  Found {duplicates:,} duplicate rows!")
                
                # Clean up
                del df
                gc.collect()
                
        except Exception as e:
            logger.error(f"Error analyzing {name}: {e}")
    
    logger.info(f"\nTotal DataFrame memory: {total_memory_mb:.1f} MB")
    return total_memory_mb

def analyze_merge_operations():
    """Analyze memory impact of merge operations"""
    logger.info("\n" + "="*60)
    logger.info("ANALYZING MERGE OPERATIONS")
    logger.info("="*60)
    
    try:
        # Load generation and DUID mapping
        gen_30_path = str(config.gen_output_file).replace('scada5.parquet', 'scada30.parquet')
        gen_df = pd.read_parquet(gen_30_path)
        
        import pickle
        with open(config.gen_info_file, 'rb') as f:
            duid_mapping = pickle.load(f)
        
        if not isinstance(duid_mapping, pd.DataFrame):
            duid_mapping = pd.DataFrame(duid_mapping)
        
        # Memory before merge
        gen_memory = gen_df.memory_usage(deep=True).sum() / 1024 / 1024
        duid_memory = duid_mapping.memory_usage(deep=True).sum() / 1024 / 1024
        
        logger.info(f"Generation data: {gen_memory:.1f} MB")
        logger.info(f"DUID mapping: {duid_memory:.1f} MB")
        logger.info(f"Total before merge: {gen_memory + duid_memory:.1f} MB")
        
        # Perform merge
        merged_df = gen_df.merge(
            duid_mapping,
            left_on='duid',
            right_on='DUID',
            how='left'
        )
        
        # Memory after merge
        merged_memory = merged_df.memory_usage(deep=True).sum() / 1024 / 1024
        logger.info(f"Merged data: {merged_memory:.1f} MB")
        logger.info(f"Memory increase: {merged_memory - gen_memory:.1f} MB ({(merged_memory/gen_memory - 1)*100:.1f}%)")
        
        # Check for column duplication
        logger.info(f"\nOriginal columns: {list(gen_df.columns)}")
        logger.info(f"DUID mapping columns: {list(duid_mapping.columns)}")
        logger.info(f"Merged columns: {list(merged_df.columns)}")
        
    except Exception as e:
        logger.error(f"Error analyzing merge: {e}")

def analyze_data_types():
    """Analyze potential for data type optimization"""
    logger.info("\n" + "="*60)
    logger.info("ANALYZING DATA TYPE OPTIMIZATION POTENTIAL")
    logger.info("="*60)
    
    # Load a sample file
    gen_30_path = str(config.gen_output_file).replace('scada5.parquet', 'scada30.parquet')
    
    try:
        df = pd.read_parquet(gen_30_path)
        
        # Current memory
        current_memory = df.memory_usage(deep=True).sum() / 1024 / 1024
        logger.info(f"Current memory usage: {current_memory:.1f} MB")
        
        # Optimize data types
        optimized_df = df.copy()
        
        # Convert object columns to category if low cardinality
        for col in optimized_df.select_dtypes(include=['object']).columns:
            unique_ratio = len(optimized_df[col].unique()) / len(optimized_df)
            if unique_ratio < 0.5:  # Less than 50% unique values
                optimized_df[col] = optimized_df[col].astype('category')
                logger.info(f"  {col}: object -> category (unique ratio: {unique_ratio:.2%})")
        
        # Downcast numeric columns
        for col in optimized_df.select_dtypes(include=['float64']).columns:
            optimized_df[col] = pd.to_numeric(optimized_df[col], downcast='float')
            logger.info(f"  {col}: float64 -> float32")
        
        # Optimized memory
        optimized_memory = optimized_df.memory_usage(deep=True).sum() / 1024 / 1024
        logger.info(f"\nOptimized memory usage: {optimized_memory:.1f} MB")
        logger.info(f"Memory reduction: {(1 - optimized_memory/current_memory)*100:.1f}%")
        
    except Exception as e:
        logger.error(f"Error analyzing data types: {e}")

def main():
    """Main analysis function"""
    logger.info("Starting memory issue analysis...")
    
    # Check file sizes
    disk_size = analyze_file_sizes()
    
    # Analyze DataFrame memory
    df_memory = analyze_dataframe_memory()
    
    # Analyze merge operations
    analyze_merge_operations()
    
    # Analyze optimization potential
    analyze_data_types()
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("ANALYSIS SUMMARY")
    logger.info("="*60)
    logger.info(f"Total file size on disk: {disk_size:.1f} MB")
    logger.info(f"Total DataFrame memory (loaded individually): {df_memory:.1f} MB")
    logger.info("\nThe 21GB memory usage is likely due to:")
    logger.info("1. Loading both 5min and 30min data (unnecessary duplication)")
    logger.info("2. Full DataFrame merge creating copies of all columns")
    logger.info("3. Pre-calculated aggregations storing additional copies")
    logger.info("4. Inefficient data types (float64 vs float32, object vs category)")
    logger.info("5. Possible memory fragmentation from multiple operations")
    logger.info("="*60)

if __name__ == "__main__":
    main()