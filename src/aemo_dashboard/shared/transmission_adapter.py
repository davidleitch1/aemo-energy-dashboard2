"""
Enhanced Transmission Data Adapter with Resolution Support

Provides intelligent data loading for transmission data with automatic
resolution selection based on date range and performance requirements.
"""

import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Any, Union
from .logging_config import get_logger
from .resolution_manager import resolution_manager
from .performance_optimizer import PerformanceOptimizer
from .config import config

logger = get_logger(__name__)


def load_transmission_data(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    resolution: str = 'auto',
    interconnectors: Optional[List[str]] = None,
    file_path: Optional[str] = None,
    optimize_for_plotting: bool = False,
    plot_type: str = 'transmission'
) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[str, Any]]]:
    """
    Load transmission data with adaptive resolution selection
    
    Args:
        start_date: Start of date range (None for all data)
        end_date: End of date range (None for all data)
        resolution: 'auto', '5min', '30min'
        interconnectors: Optional list of specific interconnectors to include
        file_path: Optional override for file path
        
    Returns:
        DataFrame with consistent format regardless of source resolution
        Columns: ['settlementdate', 'interconnectorid', 'meteredmwflow', 'mwflow', 
                 'exportlimit', 'importlimit', 'mwlosses']
    """
    
    try:
        # Determine optimal resolution
        if resolution == 'auto' and start_date and end_date:
            resolution = resolution_manager.get_optimal_resolution(
                start_date, end_date, 'transmission'
            )
            logger.info(f"Auto-selected {resolution} resolution for transmission data")
        elif resolution == 'auto':
            resolution = '5min'  # Default for no date range
            logger.info(f"Defaulting to {resolution} resolution (no date range specified)")
        
        # Get appropriate file path
        if file_path:
            data_file = file_path
        else:
            data_file = resolution_manager.get_file_path('transmission', resolution)
        
        logger.info(f"Loading transmission data from {data_file}")
        
        # Load the data
        df = pd.read_parquet(data_file)
        
        if df.empty:
            logger.warning(f"No data found in {data_file}")
            return pd.DataFrame(columns=[
                'settlementdate', 'interconnectorid', 'meteredmwflow', 
                'mwflow', 'exportlimit', 'importlimit', 'mwlosses'
            ])
        
        # Ensure consistent format
        df = _standardize_transmission_format(df)
        
        # Apply filters
        df = _apply_transmission_filters(df, start_date, end_date, interconnectors)
        
        logger.info(f"Loaded {len(df):,} transmission records from {resolution} data")
        
        # Apply performance optimization if requested
        if optimize_for_plotting and start_date and end_date:
            optimized_df, metadata = PerformanceOptimizer.optimize_for_plotting(
                df, start_date, end_date, plot_type
            )
            logger.info(f"Transmission performance optimization: {metadata['description']}")
            logger.info(f"Data reduced from {metadata['original_points']:,} to {metadata['optimized_points']:,} points")
            return optimized_df, metadata
        
        return df
        
    except Exception as e:
        logger.error(f"Error loading transmission data: {e}")
        return pd.DataFrame(columns=[
            'settlementdate', 'interconnectorid', 'meteredmwflow',
            'mwflow', 'exportlimit', 'importlimit', 'mwlosses'
        ])


def _standardize_transmission_format(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure consistent format for transmission data
    
    5-minute files have: ['settlementdate', 'interconnectorid', 'meteredmwflow', 'mwflow', 
                         'exportlimit', 'importlimit', 'mwlosses']
    30-minute files have: ['settlementdate', 'interconnectorid', 'meteredmwflow']
    
    We'll standardize to the common columns and add NaN for missing columns.
    """
    
    required_columns = ['settlementdate', 'interconnectorid', 'meteredmwflow']
    full_columns = [
        'settlementdate', 'interconnectorid', 'meteredmwflow', 
        'mwflow', 'exportlimit', 'importlimit', 'mwlosses'
    ]
    
    # Check if we have the minimum required columns
    if not all(col in df.columns for col in required_columns):
        logger.error(f"Missing required columns. Found: {list(df.columns)}")
        raise ValueError(f"Transmission data missing required columns: {required_columns}")
    
    # Ensure datetime format
    if not pd.api.types.is_datetime64_any_dtype(df['settlementdate']):
        df['settlementdate'] = pd.to_datetime(df['settlementdate'])
    
    # Create standardized DataFrame with all expected columns
    result_df = df.copy()
    
    # Add missing columns with NaN if they don't exist (for 30-minute data)
    for col in full_columns:
        if col not in result_df.columns:
            result_df[col] = pd.NA
            logger.debug(f"Added missing column '{col}' with NaN values")
    
    # Ensure numeric format for flow/limit columns
    numeric_columns = ['meteredmwflow', 'mwflow', 'exportlimit', 'importlimit', 'mwlosses']
    for col in numeric_columns:
        if col in result_df.columns and not pd.api.types.is_numeric_dtype(result_df[col]):
            result_df[col] = pd.to_numeric(result_df[col], errors='coerce')
    
    # Return only the expected columns in the correct order
    return result_df[full_columns].copy()


def _apply_transmission_filters(
    df: pd.DataFrame,
    start_date: Optional[datetime],
    end_date: Optional[datetime],
    interconnectors: Optional[List[str]]
) -> pd.DataFrame:
    """
    Apply filters to transmission data
    """
    
    # Date range filter
    if start_date:
        df = df[df['settlementdate'] >= start_date]
        logger.debug(f"Filtered to start_date >= {start_date}: {len(df):,} records")
    
    if end_date:
        df = df[df['settlementdate'] <= end_date]
        logger.debug(f"Filtered to end_date <= {end_date}: {len(df):,} records")
    
    # Interconnector filter
    if interconnectors:
        df = df[df['interconnectorid'].isin(interconnectors)]
        logger.debug(f"Filtered to interconnectors {interconnectors}: {len(df):,} records")
    
    return df


def get_transmission_summary(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    resolution: str = 'auto'
) -> dict:
    """
    Get summary statistics for transmission data
    
    Returns:
        Dictionary with summary information
    """
    
    df = load_transmission_data(start_date, end_date, resolution)
    
    if df.empty:
        return {
            'total_records': 0,
            'unique_interconnectors': 0,
            'date_range': None,
            'resolution_used': resolution,
            'total_flow_mw': 0,
            'average_flow_mw': 0,
            'max_flow_mw': 0,
            'total_losses_mw': 0
        }
    
    summary = {
        'total_records': len(df),
        'unique_interconnectors': df['interconnectorid'].nunique(),
        'date_range': {
            'start': df['settlementdate'].min(),
            'end': df['settlementdate'].max()
        },
        'resolution_used': resolution,
        'total_flow_mw': df['meteredmwflow'].sum(),
        'average_flow_mw': df['meteredmwflow'].mean(),
        'max_flow_mw': df['meteredmwflow'].max(),
        'min_flow_mw': df['meteredmwflow'].min(),
        'total_losses_mw': df['mwlosses'].sum(),
        'average_losses_mw': df['mwlosses'].mean()
    }
    
    return summary


def get_available_interconnectors(resolution: str = '5min') -> List[str]:
    """
    Get list of available interconnectors in the transmission data
    
    Args:
        resolution: Which resolution file to check ('5min' or '30min')
        
    Returns:
        Sorted list of unique interconnectors
    """
    
    try:
        file_path = resolution_manager.get_file_path('transmission', resolution)
        df = pd.read_parquet(file_path, columns=['interconnectorid'])
        interconnectors = sorted(df['interconnectorid'].unique().tolist())
        logger.info(f"Found {len(interconnectors)} unique interconnectors in {resolution} transmission data")
        return interconnectors
        
    except Exception as e:
        logger.error(f"Error getting available interconnectors: {e}")
        return []


def calculate_regional_flows(
    df: pd.DataFrame,
    target_resolution: str = '30min'
) -> pd.DataFrame:
    """
    Calculate aggregated regional flows from interconnector data
    
    Args:
        df: Transmission data with interconnector flows
        target_resolution: Resolution for aggregation ('30min', '1h', etc.)
        
    Returns:
        DataFrame with regional flow summaries
    """
    
    if df.empty:
        return pd.DataFrame()
    
    try:
        # Map interconnectors to regions (simplified mapping)
        interconnector_mapping = {
            'NSW1-QLD1': {'from': 'NSW1', 'to': 'QLD1'},
            'QLD1-NSW1': {'from': 'QLD1', 'to': 'NSW1'},
            'VIC1-NSW1': {'from': 'VIC1', 'to': 'NSW1'},
            'NSW1-VIC1': {'from': 'NSW1', 'to': 'VIC1'},
            'VIC1-SA1': {'from': 'VIC1', 'to': 'SA1'},
            'SA1-VIC1': {'from': 'SA1', 'to': 'VIC1'},
            'TAS1-VIC1': {'from': 'TAS1', 'to': 'VIC1'},
            'VIC1-TAS1': {'from': 'VIC1', 'to': 'TAS1'},
        }
        
        # Add regional information
        df_with_regions = df.copy()
        df_with_regions['from_region'] = df_with_regions['interconnectorid'].map(
            lambda x: interconnector_mapping.get(x, {}).get('from', 'UNKNOWN')
        )
        df_with_regions['to_region'] = df_with_regions['interconnectorid'].map(
            lambda x: interconnector_mapping.get(x, {}).get('to', 'UNKNOWN')
        )
        
        # Set datetime as index for resampling
        df_with_regions = df_with_regions.set_index('settlementdate')
        
        # Aggregate by target resolution
        agg_funcs = {
            'meteredmwflow': 'mean',
            'mwflow': 'mean', 
            'exportlimit': 'mean',
            'importlimit': 'mean',
            'mwlosses': 'sum'
        }
        
        aggregated = df_with_regions.groupby([
            pd.Grouper(freq=target_resolution),
            'from_region',
            'to_region',
            'interconnectorid'
        ]).agg(agg_funcs).reset_index()
        
        logger.info(f"Calculated regional flows: {len(aggregated)} aggregated records")
        return aggregated
        
    except Exception as e:
        logger.error(f"Error calculating regional flows: {e}")
        return pd.DataFrame()


def get_interconnector_utilization(
    df: pd.DataFrame,
    interconnector: str
) -> dict:
    """
    Calculate utilization statistics for a specific interconnector
    
    Args:
        df: Transmission data
        interconnector: Interconnector ID to analyze
        
    Returns:
        Dictionary with utilization statistics
    """
    
    interconnector_data = df[df['interconnectorid'] == interconnector]
    
    if interconnector_data.empty:
        return {
            'interconnector': interconnector,
            'data_points': 0,
            'utilization_stats': {}
        }
    
    try:
        # Calculate utilization metrics
        flows = interconnector_data['meteredmwflow']
        export_limits = interconnector_data['exportlimit']
        import_limits = interconnector_data['importlimit']
        
        # Utilization as percentage of limits
        export_utilization = (flows / export_limits * 100).where(flows > 0)
        import_utilization = (abs(flows) / import_limits * 100).where(flows < 0)
        
        stats = {
            'interconnector': interconnector,
            'data_points': len(interconnector_data),
            'utilization_stats': {
                'avg_flow_mw': flows.mean(),
                'max_export_mw': flows.max(),
                'max_import_mw': flows.min(),
                'avg_export_utilization_pct': export_utilization.mean(),
                'avg_import_utilization_pct': import_utilization.mean(),
                'max_export_utilization_pct': export_utilization.max(),
                'max_import_utilization_pct': import_utilization.max(),
                'total_losses_mwh': interconnector_data['mwlosses'].sum() / 12,  # Convert to MWh
                'avg_losses_pct': (interconnector_data['mwlosses'] / abs(flows) * 100).mean()
            },
            'date_range': {
                'start': interconnector_data['settlementdate'].min(),
                'end': interconnector_data['settlementdate'].max()
            }
        }
        
        return stats
        
    except Exception as e:
        logger.error(f"Error calculating utilization for {interconnector}: {e}")
        return {
            'interconnector': interconnector,
            'data_points': len(interconnector_data),
            'utilization_stats': {},
            'error': str(e)
        }


# Legacy compatibility function
def load_transmission_flows(*args, **kwargs):
    """Legacy compatibility wrapper"""
    logger.warning("load_transmission_flows() is deprecated, use load_transmission_data()")
    return load_transmission_data(*args, **kwargs)