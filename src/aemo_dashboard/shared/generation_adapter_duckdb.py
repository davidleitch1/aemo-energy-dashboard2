"""
DuckDB-based Generation Data Adapter

This adapter uses DuckDB to query generation data directly from parquet files
without loading everything into memory. It maintains the same API as the
original generation_adapter.py for seamless migration.
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
from .performance_logging import PerformanceLogger

# Import DuckDB service
import sys
sys.path.append(str(Path(__file__).parent.parent.parent))
from data_service.shared_data_duckdb import duckdb_data_service

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
    Load generation data using DuckDB for efficient memory usage
    
    This function maintains the same API as the original but uses DuckDB
    to query data directly from parquet files.
    
    Args:
        start_date: Start of date range (None for all data)
        end_date: End of date range (None for all data) 
        resolution: 'auto', '5min', '30min'
        region: Region filter ('NEM' for all regions)
        duids: Optional list of specific DUIDs to include
        file_path: Optional override for file path (ignored - DuckDB uses registered views)
        optimize_for_plotting: Apply performance optimization for plotting
        plot_type: Type of plot ('generation', 'time_of_day', etc.)
        
    Returns:
        DataFrame or (DataFrame, metadata) if optimize_for_plotting=True
        DataFrame columns: ['settlementdate', 'duid', 'scadavalue']
    """
    
    try:
        # Handle date defaults
        if start_date is None:
            # Get earliest available date from metadata
            date_ranges = duckdb_data_service.get_date_ranges()
            if 'generation' in date_ranges:
                start_date = date_ranges['generation']['start']
            else:
                start_date = datetime(2020, 1, 1)  # Fallback
                
        if end_date is None:
            # Get latest available date from metadata
            date_ranges = duckdb_data_service.get_date_ranges()
            if 'generation' in date_ranges:
                end_date = date_ranges['generation']['end']
                # Convert pd.Timestamp to datetime if needed
                if hasattr(end_date, 'to_pydatetime'):
                    end_date = end_date.to_pydatetime()
                # Always use end of day for consistency
                end_date = datetime.combine(end_date.date(), datetime.max.time())
            else:
                # Use end of current day, not current time
                end_date = datetime.combine(datetime.now().date(), datetime.max.time())
        
        # Determine optimal resolution
        if resolution == 'auto':
            resolution_strategy = resolution_manager.get_optimal_resolution_with_fallback(
                start_date, end_date, 'generation'
            )
            resolution = resolution_strategy['primary_resolution']
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Auto-selected resolution: {resolution} - {resolution_strategy['reasoning']}")
        
        # Build the query based on whether we need raw data or fuel-aggregated data
        if duids:
            # Query raw generation data filtered by DUIDs
            with perf_logger.timer("duckdb_generation_query", threshold=0.5):
                df = _query_generation_by_duids(
                    start_date, end_date, resolution, duids
                )
        else:
            # For general queries without specific DUIDs, get raw data
            # (The original adapter returns raw DUID-level data, not aggregated by fuel)
            with perf_logger.timer("duckdb_generation_query", threshold=0.5):
                df = _query_raw_generation(
                    start_date, end_date, resolution, region
                )
        
        if df.empty:
            logger.warning(f"No generation data found for date range {start_date} to {end_date}")
            return pd.DataFrame(columns=['settlementdate', 'duid', 'scadavalue'])
        
        perf_logger.log_data_operation(
            "Loaded generation data via DuckDB",
            len(df),
            metadata={"resolution": resolution, "memory_efficient": True}
        )
        
        # Apply performance optimization if requested
        if optimize_for_plotting:
            optimized_df, metadata = PerformanceOptimizer.optimize_for_plotting(
                df, start_date, end_date, plot_type
            )
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Performance optimization: {metadata['description']}")
                logger.debug(f"Data reduced from {metadata['original_points']:,} to {metadata['optimized_points']:,} points")
            return optimized_df, metadata
        
        return df
        
    except Exception as e:
        logger.error(f"Error loading generation data via DuckDB: {e}")
        return pd.DataFrame(columns=['settlementdate', 'duid', 'scadavalue'])


def _query_raw_generation(
    start_date: datetime,
    end_date: datetime,
    resolution: str,
    region: str = 'NEM'
) -> pd.DataFrame:
    """
    Query raw generation data (DUID-level) from DuckDB
    """
    # Select appropriate table based on resolution
    table = 'generation_5min' if resolution == '5min' else 'generation_30min'
    
    # Build base query
    if region == 'NEM':
        # No region filter - return all data
        query = f"""
            SELECT 
                settlementdate,
                duid,
                scadavalue
            FROM {table}
            WHERE settlementdate >= '{start_date.isoformat()}'
              AND settlementdate <= '{end_date.isoformat()}'
            ORDER BY settlementdate, duid
        """
    else:
        # Need to join with DUID mapping to filter by region
        query = f"""
            SELECT 
                g.settlementdate,
                g.duid,
                g.scadavalue
            FROM {table} g
            JOIN duid_mapping d ON g.duid = d.DUID
            WHERE g.settlementdate >= '{start_date.isoformat()}'
              AND g.settlementdate <= '{end_date.isoformat()}'
              AND d.Region = '{region}'
            ORDER BY g.settlementdate, g.duid
        """
    
    # Execute query
    df = duckdb_data_service.conn.execute(query).df()
    
    # Ensure consistent data types
    df['settlementdate'] = pd.to_datetime(df['settlementdate'])
    df['scadavalue'] = pd.to_numeric(df['scadavalue'], errors='coerce')
    
    return df


def _query_generation_by_duids(
    start_date: datetime,
    end_date: datetime,
    resolution: str,
    duids: List[str]
) -> pd.DataFrame:
    """
    Query generation data for specific DUIDs
    """
    # Select appropriate table based on resolution
    table = 'generation_5min' if resolution == '5min' else 'generation_30min'
    
    # Convert DUID list to SQL format
    duid_list = ','.join([f"'{d}'" for d in duids])
    
    # Build query
    query = f"""
        SELECT 
            settlementdate,
            duid,
            scadavalue
        FROM {table}
        WHERE settlementdate >= '{start_date.isoformat()}'
          AND settlementdate <= '{end_date.isoformat()}'
          AND duid IN ({duid_list})
        ORDER BY settlementdate, duid
    """
    
    # Execute query
    df = duckdb_data_service.conn.execute(query).df()
    
    # Ensure consistent data types
    df['settlementdate'] = pd.to_datetime(df['settlementdate'])
    df['scadavalue'] = pd.to_numeric(df['scadavalue'], errors='coerce')
    
    return df


def get_generation_summary(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    resolution: str = 'auto'
) -> dict:
    """
    Get summary statistics for generation data using DuckDB
    
    Returns:
        Dictionary with summary information
    """
    try:
        # Handle date defaults
        if start_date is None or end_date is None:
            date_ranges = duckdb_data_service.get_date_ranges()
            if 'generation' in date_ranges:
                if start_date is None:
                    start_date = date_ranges['generation']['start']
                if end_date is None:
                    end_date = date_ranges['generation']['end']
        
        # Determine resolution
        if resolution == 'auto':
            resolution_strategy = resolution_manager.get_optimal_resolution_with_fallback(
                start_date, end_date, 'generation'
            )
            resolution = resolution_strategy['primary_resolution']
        
        table = 'generation_5min' if resolution == '5min' else 'generation_30min'
        
        # Query summary statistics
        query = f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT duid) as unique_duids,
                MIN(settlementdate) as min_date,
                MAX(settlementdate) as max_date,
                SUM(scadavalue) as total_generation_mw,
                AVG(scadavalue) as average_generation_mw,
                MAX(scadavalue) as max_generation_mw
            FROM {table}
            WHERE settlementdate >= '{start_date.isoformat()}'
              AND settlementdate <= '{end_date.isoformat()}'
        """
        
        result = duckdb_data_service.conn.execute(query).fetchone()
        
        if result and result[0] > 0:
            return {
                'total_records': result[0],
                'unique_duids': result[1],
                'date_range': {
                    'start': pd.Timestamp(result[2]),
                    'end': pd.Timestamp(result[3])
                },
                'resolution_used': resolution,
                'total_generation_mw': float(result[4]) if result[4] else 0,
                'average_generation_mw': float(result[5]) if result[5] else 0,
                'max_generation_mw': float(result[6]) if result[6] else 0
            }
        else:
            return {
                'total_records': 0,
                'unique_duids': 0,
                'date_range': None,
                'resolution_used': resolution,
                'total_generation_mw': 0
            }
            
    except Exception as e:
        logger.error(f"Error getting generation summary: {e}")
        return {
            'total_records': 0,
            'unique_duids': 0,
            'date_range': None,
            'resolution_used': resolution,
            'total_generation_mw': 0
        }


def get_available_duids(resolution: str = '5min') -> List[str]:
    """
    Get list of available DUIDs in the generation data using DuckDB
    
    Args:
        resolution: Which resolution to check ('5min' or '30min')
        
    Returns:
        Sorted list of unique DUIDs
    """
    try:
        table = 'generation_5min' if resolution == '5min' else 'generation_30min'
        
        query = f"""
            SELECT DISTINCT duid
            FROM {table}
            ORDER BY duid
        """
        
        df = duckdb_data_service.conn.execute(query).df()
        duids = df['duid'].tolist()
        
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