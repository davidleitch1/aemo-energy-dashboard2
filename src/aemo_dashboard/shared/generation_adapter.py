"""
Enhanced Generation Data Adapter with Resolution Support

Provides intelligent data loading for generation data with automatic
resolution selection based on date range and performance requirements.
"""

import pandas as pd
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Union, List, Tuple, Dict, Any
from .logging_config import get_logger
from .resolution_manager import resolution_manager
from .performance_optimizer import PerformanceOptimizer
from .config import config
from .performance_logging import PerformanceLogger, performance_monitor

logger = get_logger(__name__)
perf_logger = PerformanceLogger(__name__)


def load_generation_data(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    resolution: str = 'auto',
    region: str = 'NEM',
    duids: Optional[List[str]] = None,
    file_path: Optional[str] = None,
    optimize_for_plotting: bool = False,
    plot_type: str = 'generation'
) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[str, Any]]]:
    """
    Load generation data with adaptive resolution selection and performance optimization
    
    Args:
        start_date: Start of date range (None for all data)
        end_date: End of date range (None for all data)
        resolution: 'auto', '5min', '30min' 
        region: Region filter ('NEM' for all regions)
        duids: Optional list of specific DUIDs to include
        file_path: Optional override for file path
        optimize_for_plotting: Apply performance optimization for plotting
        plot_type: Type of plot ('generation', 'time_of_day', etc.)
        
    Returns:
        DataFrame or (DataFrame, metadata) if optimize_for_plotting=True
        DataFrame columns: ['settlementdate', 'duid', 'scadavalue']
    """
    
    try:
        # Determine optimal resolution with fallback strategy
        if resolution == 'auto' and start_date and end_date:
            resolution_strategy = resolution_manager.get_optimal_resolution_with_fallback(
                start_date, end_date, 'generation'
            )
            resolution = resolution_strategy['primary_resolution']
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Resolution strategy: {resolution_strategy['reasoning']}")
            
            # Handle fallback if needed
            if resolution_strategy['strategy'] == 'hybrid':
                # Load data using hybrid approach
                with perf_logger.timer("hybrid_generation_load", threshold=1.0):
                    df = _load_generation_with_hybrid_fallback(
                        start_date, end_date, resolution_strategy, region, duids
                    )
                if not df.empty:
                    # Apply filters and return
                    df = _apply_generation_filters(df, start_date, end_date, region, duids)
                    perf_logger.log_data_operation(
                        "Loaded generation data (hybrid)",
                        len(df),
                        metadata={"strategy": "hybrid"}
                    )
                    
                    if optimize_for_plotting and start_date and end_date:
                        optimized_df, metadata = PerformanceOptimizer.optimize_for_plotting(
                            df, start_date, end_date, plot_type
                        )
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(f"Performance optimization: {metadata['description']}")
                        return optimized_df, metadata
                    return df
            
        elif resolution == 'auto':
            resolution = '5min'  # Default for no date range
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Defaulting to {resolution} resolution (no date range specified)")
        
        # Standard single-resolution loading
        if file_path:
            data_file = file_path
        else:
            data_file = resolution_manager.get_file_path('generation', resolution)
        
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Loading generation data from {data_file}")
        
        # Load the data
        with perf_logger.timer("generation_data_load", threshold=0.5):
            df = pd.read_parquet(data_file)
        
        if df.empty:
            logger.warning(f"No data found in {data_file}")
            return pd.DataFrame(columns=['settlementdate', 'duid', 'scadavalue'])
        
        # Ensure consistent column format
        df = _standardize_generation_format(df)
        
        # Apply filters
        df = _apply_generation_filters(df, start_date, end_date, region, duids)
        
        perf_logger.log_data_operation(
            "Loaded generation data",
            len(df),
            metadata={"resolution": resolution}
        )
        
        # Apply performance optimization if requested
        if optimize_for_plotting and start_date and end_date:
            optimized_df, metadata = PerformanceOptimizer.optimize_for_plotting(
                df, start_date, end_date, plot_type
            )
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Performance optimization: {metadata['description']}")
                logger.debug(f"Data reduced from {metadata['original_points']:,} to {metadata['optimized_points']:,} points")
            return optimized_df, metadata
        
        return df
        
    except Exception as e:
        logger.error(f"Error loading generation data: {e}")
        return pd.DataFrame(columns=['settlementdate', 'duid', 'scadavalue'])


def _standardize_generation_format(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure consistent column format for generation data
    
    Both 5-minute and 30-minute files should have:
    ['settlementdate', 'duid', 'scadavalue']
    """
    
    expected_columns = ['settlementdate', 'duid', 'scadavalue']
    
    # Check if we have the expected columns
    if not all(col in df.columns for col in expected_columns):
        logger.error(f"Missing expected columns. Found: {list(df.columns)}")
        raise ValueError(f"Generation data missing required columns: {expected_columns}")
    
    # Ensure datetime format
    if not pd.api.types.is_datetime64_any_dtype(df['settlementdate']):
        df['settlementdate'] = pd.to_datetime(df['settlementdate'])
    
    # Ensure numeric format for scadavalue
    if not pd.api.types.is_numeric_dtype(df['scadavalue']):
        df['scadavalue'] = pd.to_numeric(df['scadavalue'], errors='coerce')
    
    # Return only the expected columns in the correct order
    return df[expected_columns].copy()


def _apply_generation_filters(
    df: pd.DataFrame,
    start_date: Optional[datetime],
    end_date: Optional[datetime], 
    region: str,
    duids: Optional[List[str]]
) -> pd.DataFrame:
    """
    Apply filters to generation data
    """
    
    # Date range filter
    if start_date:
        df = df[df['settlementdate'] >= start_date]
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Filtered to start_date >= {start_date}: {len(df):,} records")
    
    if end_date:
        df = df[df['settlementdate'] <= end_date]
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Filtered to end_date <= {end_date}: {len(df):,} records")
    
    # DUID filter
    if duids:
        df = df[df['duid'].isin(duids)]
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Filtered to DUIDs {duids}: {len(df):,} records")
    
    # Region filter (requires DUID mapping - simplified for now)
    if region != 'NEM':
        logger.warning(f"Region filtering for '{region}' not yet implemented")
    
    return df


def _load_generation_with_hybrid_fallback(
    start_date: datetime,
    end_date: datetime,
    resolution_strategy: Dict[str, Any],
    region: str = 'NEM',
    duids: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Load generation data using hybrid fallback strategy for data gaps
    
    This function handles periods where optimal resolution data is unavailable
    by seamlessly falling back to alternative resolution.
    """
    try:
        primary_res = resolution_strategy['primary_resolution']
        fallback_res = resolution_strategy['fallback_resolution']
        data_availability = resolution_strategy['data_availability']
        
        logger.info(f"Loading generation data with hybrid strategy: {primary_res} primary, {fallback_res} fallback")
        
        all_data = []
        
        # Get availability info for primary resolution
        primary_info = data_availability.get(primary_res, {})
        available_from = primary_info.get('available_from')
        
        # Split date range based on data availability
        if available_from and start_date < available_from:
            # Period 1: Before primary data availability - use fallback
            fallback_end = min(available_from, end_date)
            logger.info(f"Loading fallback data ({fallback_res}) for {start_date.date()} to {fallback_end.date()}")
            
            fallback_file = resolution_manager.get_file_path('generation', fallback_res)
            fallback_df = pd.read_parquet(fallback_file)
            
            if not fallback_df.empty:
                fallback_df = _standardize_generation_format(fallback_df)
                fallback_filtered = _apply_generation_filters(
                    fallback_df, start_date, fallback_end, region, duids
                )
                if not fallback_filtered.empty:
                    all_data.append(fallback_filtered)
                    logger.info(f"Loaded {len(fallback_filtered):,} records from {fallback_res} for early period")
            
            # Period 2: Primary data availability - use primary resolution
            if available_from < end_date:
                primary_start = available_from
                logger.info(f"Loading primary data ({primary_res}) for {primary_start.date()} to {end_date.date()}")
                
                primary_file = resolution_manager.get_file_path('generation', primary_res)
                primary_df = pd.read_parquet(primary_file)
                
                if not primary_df.empty:
                    primary_df = _standardize_generation_format(primary_df)
                    primary_filtered = _apply_generation_filters(
                        primary_df, primary_start, end_date, region, duids
                    )
                    if not primary_filtered.empty:
                        all_data.append(primary_filtered)
                        logger.info(f"Loaded {len(primary_filtered):,} records from {primary_res} for recent period")
        else:
            # Entire period has primary data available - use primary resolution
            primary_file = resolution_manager.get_file_path('generation', primary_res)
            primary_df = pd.read_parquet(primary_file)
            
            if not primary_df.empty:
                primary_df = _standardize_generation_format(primary_df)
                primary_filtered = _apply_generation_filters(
                    primary_df, start_date, end_date, region, duids
                )
                if not primary_filtered.empty:
                    all_data.append(primary_filtered)
                    logger.info(f"Loaded {len(primary_filtered):,} records from {primary_res} for full period")
        
        # Combine all data
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            combined_df = combined_df.sort_values('settlementdate').reset_index(drop=True)
            logger.info(f"Hybrid loading complete: {len(combined_df):,} total records")
            return combined_df
        else:
            logger.warning("No data loaded from hybrid strategy")
            return pd.DataFrame(columns=['settlementdate', 'duid', 'scadavalue'])
            
    except Exception as e:
        logger.error(f"Error in hybrid fallback loading: {e}")
        return pd.DataFrame(columns=['settlementdate', 'duid', 'scadavalue'])


def get_generation_summary(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    resolution: str = 'auto'
) -> dict:
    """
    Get summary statistics for generation data
    
    Returns:
        Dictionary with summary information
    """
    
    df = load_generation_data(start_date, end_date, resolution)
    
    if df.empty:
        return {
            'total_records': 0,
            'unique_duids': 0,
            'date_range': None,
            'resolution_used': resolution,
            'total_generation_mw': 0
        }
    
    summary = {
        'total_records': len(df),
        'unique_duids': df['duid'].nunique(),
        'date_range': {
            'start': df['settlementdate'].min(),
            'end': df['settlementdate'].max()
        },
        'resolution_used': resolution,
        'total_generation_mw': df['scadavalue'].sum(),
        'average_generation_mw': df['scadavalue'].mean(),
        'max_generation_mw': df['scadavalue'].max()
    }
    
    return summary


def get_available_duids(resolution: str = '5min') -> List[str]:
    """
    Get list of available DUIDs in the generation data
    
    Args:
        resolution: Which resolution file to check ('5min' or '30min')
        
    Returns:
        Sorted list of unique DUIDs
    """
    
    try:
        file_path = resolution_manager.get_file_path('generation', resolution)
        df = pd.read_parquet(file_path, columns=['duid'])
        duids = sorted(df['duid'].unique().tolist())
        logger.info(f"Found {len(duids)} unique DUIDs in {resolution} generation data")
        return duids
        
    except Exception as e:
        logger.error(f"Error getting available DUIDs: {e}")
        return []


# Legacy compatibility function
def load_gen_data(*args, **kwargs):
    """Legacy compatibility wrapper"""
    logger.warning("load_gen_data() is deprecated, use load_generation_data()")
    return load_generation_data(*args, **kwargs)