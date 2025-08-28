"""
DuckDB-based Price Data Adapter

This adapter uses DuckDB to query price data directly from parquet files
without loading everything into memory. It maintains the same API as the
original price_adapter.py for seamless migration.
"""

import pandas as pd
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any

from .logging_config import get_logger
from .resolution_manager import resolution_manager
from .config import config
from .performance_logging import PerformanceLogger

# Import DuckDB service
import sys
sys.path.append(str(Path(__file__).parent.parent.parent))
from data_service.shared_data_duckdb import duckdb_data_service

logger = get_logger(__name__)
perf_logger = PerformanceLogger(__name__)


def load_price_data(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    resolution: str = 'auto',
    regions: Optional[List[str]] = None,
    file_path: Optional[str] = None
) -> pd.DataFrame:
    """
    Load price data using DuckDB for efficient memory usage
    
    This function maintains the same API as the original but uses DuckDB
    to query data directly from parquet files.
    
    Args:
        start_date: Start of date range (None for all data)
        end_date: End of date range (None for all data)
        resolution: 'auto', '5min', '30min'
        regions: Optional list of specific regions to include
        file_path: Optional override for file path (ignored - DuckDB uses registered views)
        
    Returns:
        DataFrame with consistent format matching the original adapter
        Index: datetime (SETTLEMENTDATE)
        Columns: REGIONID, RRP
    """
    
    try:
        # Add detailed logging for Safari refresh debugging
        logger.info(f"load_price_data called - start: {start_date} (type: {type(start_date)}), end: {end_date} (type: {type(end_date)})")
        
        # Handle date defaults
        if start_date is None or end_date is None:
            logger.warning("Date(s) are None, fetching date ranges from DuckDB service")
            date_ranges = duckdb_data_service.get_date_ranges()
            if 'prices' in date_ranges:
                if start_date is None:
                    # Convert pd.Timestamp to datetime and set to start of day
                    start_date = date_ranges['prices']['start']
                    if hasattr(start_date, 'to_pydatetime'):
                        start_date = start_date.to_pydatetime()
                    # Always use start of day for consistency
                    start_date = datetime.combine(start_date.date(), datetime.min.time())
                    logger.info(f"Using DuckDB start date: {start_date}")
                if end_date is None:
                    # Convert pd.Timestamp to datetime and set to end of day
                    end_date = date_ranges['prices']['end']
                    if hasattr(end_date, 'to_pydatetime'):
                        end_date = end_date.to_pydatetime()
                    # Always use end of day for consistency
                    end_date = datetime.combine(end_date.date(), datetime.max.time())
                    logger.info(f"Using DuckDB end date: {end_date}")
            else:
                # Fallback defaults - use proper day boundaries
                logger.warning("No price date ranges from DuckDB, using fallback defaults")
                if start_date is None:
                    start_date = datetime(2020, 1, 1, 0, 0, 0)  # Start of day
                if end_date is None:
                    # Use end of current day, not current time
                    end_date = datetime.combine(datetime.now().date(), datetime.max.time())
                logger.info(f"Fallback dates - start: {start_date}, end: {end_date}")
        
        # Log the date range that will be queried
        duration = end_date - start_date
        logger.info(f"Date range to query: {start_date} to {end_date} (duration: {duration})")
        
        # Determine optimal resolution
        if resolution == 'auto':
            logger.info("Determining optimal resolution...")
            resolution_strategy = resolution_manager.get_optimal_resolution_with_fallback(
                start_date, end_date, 'price'
            )
            resolution = resolution_strategy['primary_resolution']
            logger.info(f"Auto-selected resolution: {resolution} - {resolution_strategy['reasoning']}")
        
        # Query price data using DuckDB
        logger.info(f"Querying DuckDB for {resolution} price data...")
        with perf_logger.timer("duckdb_price_query", threshold=0.5):
            df = duckdb_data_service.get_regional_prices(
                start_date=start_date,
                end_date=end_date,
                regions=regions,
                resolution=resolution
            )
        
        logger.info(f"DuckDB query returned {len(df)} records")
        
        if df.empty:
            logger.warning(f"No price data found for date range {start_date} to {end_date}")
            return pd.DataFrame()
        
        # Convert to expected format (uppercase columns, datetime index)
        df = _convert_to_legacy_format(df)
        
        perf_logger.log_data_operation(
            "Loaded price data via DuckDB",
            len(df),
            metadata={"resolution": resolution, "memory_efficient": True}
        )
        
        logger.info(f"Loaded {len(df):,} price records from {resolution} data")
        return df
        
    except Exception as e:
        logger.error(f"Error loading price data via DuckDB: {e}")
        return pd.DataFrame()


def _convert_to_legacy_format(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert DuckDB output to match the legacy adapter format
    
    DuckDB returns: lowercase columns (settlementdate, regionid, rrp)
    Legacy expects: uppercase columns (REGIONID, RRP) with SETTLEMENTDATE as index
    """
    if df.empty:
        return df
    
    # Create a copy to avoid modifying the original
    result = df.copy()
    
    # Convert column names to uppercase
    result = result.rename(columns={
        'regionid': 'REGIONID',
        'rrp': 'RRP',
        'settlementdate': 'SETTLEMENTDATE'
    })
    
    # Set SETTLEMENTDATE as index if it exists
    if 'SETTLEMENTDATE' in result.columns:
        result = result.set_index('SETTLEMENTDATE')
        result.index.name = 'SETTLEMENTDATE'
        result = result.sort_index()
    
    # Ensure RRP is numeric
    if 'RRP' in result.columns:
        result['RRP'] = pd.to_numeric(result['RRP'], errors='coerce')
    
    return result


def get_price_summary(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    resolution: str = 'auto',
    regions: Optional[List[str]] = None
) -> dict:
    """
    Get summary statistics for price data using DuckDB
    
    Returns:
        Dictionary with summary information by region
    """
    try:
        # Handle date defaults
        if start_date is None or end_date is None:
            date_ranges = duckdb_data_service.get_date_ranges()
            if 'prices' in date_ranges:
                if start_date is None:
                    start_date = date_ranges['prices']['start']
                if end_date is None:
                    end_date = date_ranges['prices']['end']
        
        # Determine resolution
        if resolution == 'auto':
            resolution_strategy = resolution_manager.get_optimal_resolution_with_fallback(
                start_date, end_date, 'price'
            )
            resolution = resolution_strategy['primary_resolution']
        
        table = 'prices_5min' if resolution == '5min' else 'prices_30min'
        
        # Build region filter
        region_filter = ""
        if regions:
            region_list = ','.join([f"'{r}'" for r in regions])
            region_filter = f"AND regionid IN ({region_list})"
        
        # Query summary statistics by region
        query = f"""
            SELECT 
                regionid,
                COUNT(*) as total_records,
                MIN(settlementdate) as min_date,
                MAX(settlementdate) as max_date,
                AVG(rrp) as average_price,
                MIN(rrp) as min_price,
                MAX(rrp) as max_price,
                STDDEV(rrp) as price_volatility
            FROM {table}
            WHERE settlementdate >= '{start_date.isoformat()}'
              AND settlementdate <= '{end_date.isoformat()}'
              {region_filter}
            GROUP BY regionid
            ORDER BY regionid
        """
        
        df = duckdb_data_service.conn.execute(query).df()
        
        # Convert to summary dictionary
        summary = {
            'date_range': {
                'start': start_date,
                'end': end_date
            },
            'resolution_used': resolution,
            'regions': {}
        }
        
        for _, row in df.iterrows():
            summary['regions'][row['regionid']] = {
                'total_records': int(row['total_records']),
                'average_price': float(row['average_price']),
                'min_price': float(row['min_price']),
                'max_price': float(row['max_price']),
                'volatility': float(row['price_volatility']) if row['price_volatility'] else 0
            }
        
        return summary
        
    except Exception as e:
        logger.error(f"Error getting price summary: {e}")
        return {
            'date_range': {'start': start_date, 'end': end_date},
            'resolution_used': resolution,
            'regions': {}
        }


def get_available_regions(resolution: str = '30min') -> List[str]:
    """
    Get list of available regions in the price data using DuckDB
    
    Args:
        resolution: Which resolution to check ('5min' or '30min')
        
    Returns:
        Sorted list of unique regions
    """
    try:
        table = 'prices_5min' if resolution == '5min' else 'prices_30min'
        
        query = f"""
            SELECT DISTINCT regionid
            FROM {table}
            ORDER BY regionid
        """
        
        df = duckdb_data_service.conn.execute(query).df()
        regions = df['regionid'].tolist()
        
        logger.info(f"Found {len(regions)} unique regions in {resolution} price data")
        return regions
        
    except Exception as e:
        logger.error(f"Error getting available regions: {e}")
        return []


def get_price_statistics(
    start_date: datetime,
    end_date: datetime,
    region: str,
    resolution: str = '30min'
) -> Dict[str, float]:
    """
    Get detailed price statistics for a specific region and period
    
    Args:
        start_date: Start of period
        end_date: End of period  
        region: Region ID (e.g., 'NSW1')
        resolution: Data resolution
        
    Returns:
        Dictionary with statistical measures
    """
    try:
        table = 'prices_5min' if resolution == '5min' else 'prices_30min'
        
        query = f"""
            SELECT 
                AVG(rrp) as mean_price,
                MEDIAN(rrp) as median_price,
                MIN(rrp) as min_price,
                MAX(rrp) as max_price,
                STDDEV(rrp) as std_dev,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY rrp) as q1,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY rrp) as q3,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY rrp) as p95,
                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY rrp) as p99,
                COUNT(*) as count,
                COUNT(CASE WHEN rrp < 0 THEN 1 END) as negative_count,
                COUNT(CASE WHEN rrp > 300 THEN 1 END) as high_price_count
            FROM {table}
            WHERE settlementdate >= '{start_date.isoformat()}'
              AND settlementdate <= '{end_date.isoformat()}'
              AND regionid = '{region}'
        """
        
        result = duckdb_data_service.conn.execute(query).fetchone()
        
        if result:
            return {
                'mean': float(result[0]) if result[0] else 0,
                'median': float(result[1]) if result[1] else 0,
                'min': float(result[2]) if result[2] else 0,
                'max': float(result[3]) if result[3] else 0,
                'std_dev': float(result[4]) if result[4] else 0,
                'q1': float(result[5]) if result[5] else 0,
                'q3': float(result[6]) if result[6] else 0,
                'p95': float(result[7]) if result[7] else 0,
                'p99': float(result[8]) if result[8] else 0,
                'count': int(result[9]) if result[9] else 0,
                'negative_price_periods': int(result[10]) if result[10] else 0,
                'high_price_periods': int(result[11]) if result[11] else 0
            }
        else:
            return {}
            
    except Exception as e:
        logger.error(f"Error getting price statistics: {e}")
        return {}


# Legacy compatibility function
def load_spot_data(*args, **kwargs):
    """Legacy compatibility wrapper"""
    logger.warning("load_spot_data() is deprecated, use load_price_data()")
    return load_price_data(*args, **kwargs)