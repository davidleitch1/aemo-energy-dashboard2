"""
Enhanced Price Data Adapter with Resolution Support

Provides intelligent data loading for price data with automatic
resolution selection based on date range and performance requirements.
"""

import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any
from .logging_config import get_logger
from .resolution_manager import resolution_manager
from .config import config

logger = get_logger(__name__)


def load_price_data(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    resolution: str = 'auto',
    regions: Optional[List[str]] = None,
    file_path: Optional[str] = None
) -> pd.DataFrame:
    """
    Load price data with adaptive resolution selection
    
    Args:
        start_date: Start of date range (None for all data)
        end_date: End of date range (None for all data)
        resolution: 'auto', '5min', '30min'
        regions: Optional list of specific regions to include
        file_path: Optional override for file path
        
    Returns:
        DataFrame with consistent format regardless of source resolution
        Index: datetime (SETTLEMENTDATE)
        Columns: REGIONID, RRP (plus settlementdate if not index)
    """
    
    try:
        # Determine optimal resolution with fallback strategy
        if resolution == 'auto' and start_date and end_date:
            resolution_strategy = resolution_manager.get_optimal_resolution_with_fallback(
                start_date, end_date, 'price'
            )
            resolution = resolution_strategy['primary_resolution']
            logger.info(f"Price resolution strategy: {resolution_strategy['reasoning']}")
            
            # Handle fallback if needed
            if resolution_strategy['strategy'] == 'hybrid':
                # Load data using hybrid approach
                df = _load_price_with_hybrid_fallback(
                    start_date, end_date, resolution_strategy, regions
                )
                if not df.empty:
                    logger.info(f"Loaded {len(df):,} price records using hybrid strategy")
                    return df
            
        elif resolution == 'auto':
            resolution = '5min'  # Default for no date range
            logger.info(f"Defaulting to {resolution} resolution (no date range specified)")
        
        # Standard single-resolution loading
        if file_path:
            data_file = file_path
        else:
            data_file = resolution_manager.get_file_path('price', resolution)
        
        logger.info(f"Loading price data from {data_file}")
        
        # Load the data
        df = pd.read_parquet(data_file)
        
        if df.empty:
            logger.warning(f"No data found in {data_file}")
            return pd.DataFrame()
        
        # Ensure consistent format
        df = _standardize_price_format(df)
        
        # Apply filters
        df = _apply_price_filters(df, start_date, end_date, regions)
        
        logger.info(f"Loaded {len(df):,} price records from {resolution} data")
        return df
        
    except Exception as e:
        logger.error(f"Error loading price data: {e}")
        return pd.DataFrame()


def _load_price_with_hybrid_fallback(
    start_date: datetime,
    end_date: datetime,
    resolution_strategy: Dict[str, Any],
    regions: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Load price data using hybrid fallback strategy for data gaps
    """
    try:
        primary_res = resolution_strategy['primary_resolution']
        fallback_res = resolution_strategy['fallback_resolution']
        data_availability = resolution_strategy['data_availability']
        
        logger.info(f"Loading price data with hybrid strategy: {primary_res} primary, {fallback_res} fallback")
        
        all_data = []
        
        # Get availability info for primary resolution
        primary_info = data_availability.get(primary_res, {})
        available_from = primary_info.get('available_from')
        
        # Split date range based on data availability
        if available_from and start_date < available_from:
            # Period 1: Before primary data availability - use fallback
            fallback_end = min(available_from, end_date)
            logger.info(f"Loading fallback price data ({fallback_res}) for {start_date.date()} to {fallback_end.date()}")
            
            fallback_file = resolution_manager.get_file_path('price', fallback_res)
            fallback_df = pd.read_parquet(fallback_file)
            
            if not fallback_df.empty:
                fallback_df = _standardize_price_format(fallback_df)
                fallback_filtered = _apply_price_filters(
                    fallback_df, start_date, fallback_end, regions
                )
                if not fallback_filtered.empty:
                    all_data.append(fallback_filtered)
                    logger.info(f"Loaded {len(fallback_filtered):,} price records from {fallback_res} for early period")
            
            # Period 2: Primary data availability - use primary resolution
            if available_from < end_date:
                primary_start = available_from
                logger.info(f"Loading primary price data ({primary_res}) for {primary_start.date()} to {end_date.date()}")
                
                primary_file = resolution_manager.get_file_path('price', primary_res)
                primary_df = pd.read_parquet(primary_file)
                
                if not primary_df.empty:
                    primary_df = _standardize_price_format(primary_df)
                    primary_filtered = _apply_price_filters(
                        primary_df, primary_start, end_date, regions
                    )
                    if not primary_filtered.empty:
                        all_data.append(primary_filtered)
                        logger.info(f"Loaded {len(primary_filtered):,} price records from {primary_res} for recent period")
        else:
            # Entire period has primary data available - use primary resolution
            primary_file = resolution_manager.get_file_path('price', primary_res)
            primary_df = pd.read_parquet(primary_file)
            
            if not primary_df.empty:
                primary_df = _standardize_price_format(primary_df)
                primary_filtered = _apply_price_filters(
                    primary_df, start_date, end_date, regions
                )
                if not primary_filtered.empty:
                    all_data.append(primary_filtered)
                    logger.info(f"Loaded {len(primary_filtered):,} price records from {primary_res} for full period")
        
        # Combine all data
        if all_data:
            # Reset index to column before concatenating to preserve datetime information
            data_with_columns = []
            for df in all_data:
                if isinstance(df.index, pd.DatetimeIndex):
                    df = df.reset_index()
                data_with_columns.append(df)
            
            combined_df = pd.concat(data_with_columns, ignore_index=True)
            
            # Sort by settlementdate column
            if 'SETTLEMENTDATE' in combined_df.columns:
                combined_df = combined_df.sort_values('SETTLEMENTDATE').reset_index(drop=True)
            elif 'settlementdate' in combined_df.columns:
                combined_df = combined_df.sort_values('settlementdate').reset_index(drop=True)
                
            logger.info(f"Hybrid price loading complete: {len(combined_df):,} total records")
            return combined_df
        else:
            logger.warning("No price data loaded from hybrid strategy")
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"Error in price hybrid fallback loading: {e}")
        return pd.DataFrame()


def _standardize_price_format(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure consistent format for price data
    
    Converts new format (regionid, rrp) to old format (REGIONID, RRP)
    """
    
    # Check if this is new format (lowercase columns)
    if 'regionid' in df.columns and 'rrp' in df.columns:
        logger.debug("Detected new price data format, applying adapter")
        
        # Apply adaptations
        df = df.rename(columns={
            'regionid': 'REGIONID',
            'rrp': 'RRP'
        })
        
        # Set datetime as index if it's a column
        if 'settlementdate' in df.columns:
            df = df.set_index('settlementdate')
            df.index.name = 'SETTLEMENTDATE'  # Match expected uppercase format
            df = df.sort_index()
    
    # Old format - return as is
    elif 'REGIONID' in df.columns and 'RRP' in df.columns:
        logger.debug("Using existing price data format")
    
    else:
        raise ValueError(f"Unknown price data format. Columns: {list(df.columns)}")
    
    # Ensure datetime index
    if not pd.api.types.is_datetime64_any_dtype(df.index):
        if 'SETTLEMENTDATE' in df.columns:
            df = df.set_index('SETTLEMENTDATE')
        else:
            raise ValueError("No datetime column found for price data")
    
    # Ensure numeric RRP
    if not pd.api.types.is_numeric_dtype(df['RRP']):
        df['RRP'] = pd.to_numeric(df['RRP'], errors='coerce')
    
    return df


def _apply_price_filters(
    df: pd.DataFrame,
    start_date: Optional[datetime],
    end_date: Optional[datetime],
    regions: Optional[List[str]]
) -> pd.DataFrame:
    """
    Apply filters to price data
    """
    
    # Date range filter
    if start_date:
        df = df[df.index >= start_date]
        logger.debug(f"Filtered to start_date >= {start_date}: {len(df):,} records")
    
    if end_date:
        df = df[df.index <= end_date]
        logger.debug(f"Filtered to end_date <= {end_date}: {len(df):,} records")
    
    # Region filter
    if regions:
        df = df[df['REGIONID'].isin(regions)]
        logger.debug(f"Filtered to regions {regions}: {len(df):,} records")
    
    return df


def get_price_summary(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    resolution: str = 'auto'
) -> dict:
    """
    Get summary statistics for price data
    
    Returns:
        Dictionary with summary information
    """
    
    df = load_price_data(start_date, end_date, resolution)
    
    if df.empty:
        return {
            'total_records': 0,
            'unique_regions': 0,
            'date_range': None,
            'resolution_used': resolution,
            'average_price': 0,
            'max_price': 0,
            'min_price': 0
        }
    
    summary = {
        'total_records': len(df),
        'unique_regions': df['REGIONID'].nunique(),
        'date_range': {
            'start': df.index.min(),
            'end': df.index.max()
        },
        'resolution_used': resolution,
        'average_price': df['RRP'].mean(),
        'max_price': df['RRP'].max(),
        'min_price': df['RRP'].min(),
        'price_volatility': df['RRP'].std()
    }
    
    return summary


def get_available_regions(resolution: str = '5min') -> List[str]:
    """
    Get list of available regions in the price data
    
    Args:
        resolution: Which resolution file to check ('5min' or '30min')
        
    Returns:
        Sorted list of unique regions
    """
    
    try:
        file_path = resolution_manager.get_file_path('price', resolution)
        df = pd.read_parquet(file_path, columns=['regionid'] if 'regionid' in pd.read_parquet(file_path, nrows=1).columns else ['REGIONID'])
        
        # Handle both old and new formats
        region_col = 'regionid' if 'regionid' in df.columns else 'REGIONID'
        regions = sorted(df[region_col].unique().tolist())
        logger.info(f"Found {len(regions)} unique regions in {resolution} price data")
        return regions
        
    except Exception as e:
        logger.error(f"Error getting available regions: {e}")
        return []


def convert_to_pivot_format(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert price data to pivot table format (regions as columns)
    
    Args:
        df: Price data in long format
        
    Returns:
        DataFrame with regions as columns, datetime as index
    """
    
    if df.empty:
        return pd.DataFrame()
    
    try:
        # Reset index to get datetime as column for pivot
        df_pivot = df.reset_index()
        
        # Pivot with regions as columns
        pivot_df = df_pivot.pivot(
            index='SETTLEMENTDATE', 
            columns='REGIONID', 
            values='RRP'
        )
        
        # Clean up column names
        pivot_df.columns.name = None
        
        logger.debug(f"Converted to pivot format: {len(pivot_df)} rows, {len(pivot_df.columns)} regions")
        return pivot_df
        
    except Exception as e:
        logger.error(f"Error converting to pivot format: {e}")
        return pd.DataFrame()


# Legacy compatibility function (original function name)
def load_price_data_legacy(file_path=None):
    """Legacy compatibility wrapper for original function"""
    logger.warning("load_price_data_legacy() is deprecated, use load_price_data()")
    return load_price_data(file_path=file_path)
