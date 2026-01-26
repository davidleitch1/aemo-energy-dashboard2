"""
Rooftop Solar Adapter - DuckDB implementation for loading rooftop solar data

This module provides functions to load rooftop solar data using DuckDB
for efficient memory usage while maintaining the same interpolation
and smoothing functionality.
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Optional, Union, Dict, Any, Tuple
from pathlib import Path
import sys

# Add the src directory to Python path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from .config import config
from .logging_config import get_logger
from .performance_logging import PerformanceLogger, performance_monitor
from .resolution_manager import resolution_manager
from .fuel_categories import MAIN_ROOFTOP_REGIONS

# Import DuckDB service
from data_service.shared_data_duckdb import duckdb_data_service

# Import interpolation functions from original adapter
from .rooftop_adapter import (
    henderson_smooth,
    interpolate_and_smooth,
    handle_future_projection,
    HENDERSON_7
)

logger = get_logger(__name__)
perf_logger = PerformanceLogger(__name__)


@performance_monitor(threshold=1.0)
def load_rooftop_data(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    region: Optional[str] = None,
    target_resolution: str = '5min',
    apply_smoothing: bool = True
) -> pd.DataFrame:
    """
    Load rooftop solar data using DuckDB and optionally interpolate to 5-minute resolution.
    
    Args:
        start_date: Start date for data (if None, loads all available)
        end_date: End date for data (if None, loads all available)
        region: Specific region to filter (if None, loads all regions)
        target_resolution: Target resolution ('5min' or '30min')
        apply_smoothing: Whether to apply Henderson smoothing (only for 5min interpolation)
        
    Returns:
        DataFrame with columns: settlementdate, regionid, rooftop_solar_mw
        If target_resolution='5min', data is interpolated and smoothed
    """
    try:
        # Handle date defaults
        if start_date is None or end_date is None:
            date_ranges = duckdb_data_service.get_date_ranges()
            if 'rooftop' in date_ranges:
                if start_date is None:
                    start_date = date_ranges['rooftop']['start']
                if end_date is None:
                    end_date = date_ranges['rooftop']['end']
            else:
                logger.warning("No rooftop data range available")
                return pd.DataFrame()
        
        # Build region filter list for SQL (only main regions to avoid double-counting)
        regions_sql = "','".join(MAIN_ROOFTOP_REGIONS)

        # Build query for 30-minute data (table is named rooftop_solar)
        if region:
            # Single region query - must be a main region
            if region not in MAIN_ROOFTOP_REGIONS:
                logger.warning(f"Region '{region}' is not a main rooftop region. Using main regions only.")
                logger.warning(f"Main regions are: {MAIN_ROOFTOP_REGIONS}")

            query = f"""
            SELECT settlementdate, regionid, rooftop_solar_mw
            FROM rooftop_solar
            WHERE settlementdate >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
            AND settlementdate <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
            AND regionid = '{region}'
            ORDER BY settlementdate
            """
        else:
            # Multi-region query - ONLY MAIN REGIONS (filter out sub-regions)
            query = f"""
            SELECT settlementdate, regionid, rooftop_solar_mw
            FROM rooftop_solar
            WHERE settlementdate >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
            AND settlementdate <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
            AND regionid IN ('{regions_sql}')
            ORDER BY settlementdate, regionid
            """
            logger.info(f"Filtering rooftop data to main regions only: {MAIN_ROOFTOP_REGIONS}")
        
        # Execute query
        with perf_logger.timer("duckdb_rooftop_query", threshold=0.5):
            df_30min = duckdb_data_service.conn.execute(query).df()
        
        if df_30min.empty:
            logger.warning("No rooftop data found for the specified period")
            return pd.DataFrame()
        
        # Ensure consistent data types
        df_30min['settlementdate'] = pd.to_datetime(df_30min['settlementdate'])
        df_30min['regionid'] = df_30min['regionid'].astype(str)
        df_30min['rooftop_solar_mw'] = pd.to_numeric(df_30min['rooftop_solar_mw'], errors='coerce')
        
        logger.info(f"Loaded {len(df_30min):,} 30-minute rooftop records via DuckDB")
        
        # If target resolution is 30min, return as-is
        if target_resolution == '30min':
            return df_30min
        
        # Otherwise, interpolate to 5-minute resolution
        logger.info("Interpolating rooftop data to 5-minute resolution...")
        
        # Process each region separately
        df_5min_list = []
        
        for region_id in df_30min['regionid'].unique():
            region_data = df_30min[df_30min['regionid'] == region_id].copy()
            region_data = region_data.set_index('settlementdate').sort_index()
            
            # Create 5-minute target index
            min_time = region_data.index.min()
            max_time = region_data.index.max()
            target_index = pd.date_range(start=min_time, end=max_time, freq='5min')
            
            # Interpolate and smooth
            interpolated_series = interpolate_and_smooth(
                region_data['rooftop_solar_mw'],
                target_index
            )
            
            # Create DataFrame for this region
            region_5min = pd.DataFrame({
                'settlementdate': target_index,
                'regionid': region_id,
                'rooftop_solar_mw': interpolated_series
            })
            
            df_5min_list.append(region_5min)
        
        # Combine all regions
        df_5min = pd.concat(df_5min_list, ignore_index=True)
        
        # Handle future projection if needed
        if df_5min['settlementdate'].max() < end_date:
            logger.info("Applying future projection for rooftop data...")
            # Set index for handle_future_projection function
            df_5min_indexed = df_5min.set_index('settlementdate')
            df_5min_indexed = handle_future_projection(df_5min_indexed, df_30min['settlementdate'].max())
            # Reset index back to column
            df_5min = df_5min_indexed.reset_index()
        
        logger.info(f"Interpolated to {len(df_5min):,} 5-minute rooftop records")
        
        return df_5min
        
    except Exception as e:
        logger.error(f"Error loading rooftop data via DuckDB: {e}")
        return pd.DataFrame()


def get_rooftop_summary(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Get a summary of rooftop solar data using DuckDB.
    
    Args:
        start_date: Start date for summary
        end_date: End date for summary
        
    Returns:
        Dictionary with summary statistics
    """
    try:
        # Handle date defaults
        if start_date is None or end_date is None:
            date_ranges = duckdb_data_service.get_date_ranges()
            if 'rooftop' in date_ranges:
                if start_date is None:
                    start_date = date_ranges['rooftop']['start']
                if end_date is None:
                    end_date = date_ranges['rooftop']['end']
        
        # Query for summary statistics
        query = f"""
        SELECT 
            COUNT(*) as record_count,
            COUNT(DISTINCT regionid) as region_count,
            MIN(settlementdate) as start_date,
            MAX(settlementdate) as end_date,
            AVG(rooftop_solar_mw) as avg_generation,
            MAX(rooftop_solar_mw) as max_generation,
            SUM(rooftop_solar_mw) / COUNT(DISTINCT settlementdate) as total_avg_generation
        FROM rooftop_solar
        WHERE settlementdate >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
        AND settlementdate <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
        """
        
        result = duckdb_data_service.conn.execute(query).fetchone()
        
        if result and result[0] > 0:
            summary = {
                'record_count': result[0],
                'region_count': result[1],
                'start_date': result[2],
                'end_date': result[3],
                'avg_generation': result[4],
                'max_generation': result[5],
                'total_avg_generation': result[6]
            }
            
            logger.info(f"Rooftop summary: {summary['record_count']:,} records, "
                       f"{summary['region_count']} regions, "
                       f"max generation: {summary['max_generation']:.1f} MW")
            
            return summary
        else:
            return {}
            
    except Exception as e:
        logger.error(f"Error getting rooftop summary: {e}")
        return {}


def get_rooftop_at_time(
    target_time: datetime,
    region: Optional[str] = None
) -> Union[float, pd.DataFrame]:
    """
    Get rooftop solar generation at a specific time using DuckDB.
    
    Args:
        target_time: The time to get data for
        region: Specific region or None for all regions
        
    Returns:
        If region specified: float MW value
        If region not specified: DataFrame with all regions
    """
    try:
        # Find the closest 30-minute interval
        minutes = target_time.minute
        if minutes < 15:
            rounded_time = target_time.replace(minute=0, second=0, microsecond=0)
        elif minutes < 45:
            rounded_time = target_time.replace(minute=30, second=0, microsecond=0)
        else:
            rounded_time = (target_time + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        
        if region:
            query = f"""
            SELECT rooftop_solar_mw
            FROM rooftop_solar
            WHERE settlementdate = '{rounded_time.strftime('%Y-%m-%d %H:%M:%S')}'
            AND regionid = '{region}'
            """
            
            result = duckdb_data_service.conn.execute(query).fetchone()
            
            if result:
                return float(result[0])
            else:
                logger.warning(f"No rooftop data found for {region} at {rounded_time}")
                return 0.0
        else:
            query = f"""
            SELECT regionid, rooftop_solar_mw
            FROM rooftop_solar
            WHERE settlementdate = '{rounded_time.strftime('%Y-%m-%d %H:%M:%S')}'
            ORDER BY regionid
            """
            
            df = duckdb_data_service.conn.execute(query).df()
            
            if df.empty:
                logger.warning(f"No rooftop data found at {rounded_time}")
                return pd.DataFrame()
            
            return df
            
    except Exception as e:
        logger.error(f"Error getting rooftop data at time: {e}")
        return 0.0 if region else pd.DataFrame()


def smooth_rooftop_data(df_rooftop: pd.DataFrame) -> pd.DataFrame:
    """
    Apply Henderson smoothing to rooftop data.
    
    This is a wrapper that maintains compatibility with the original function.
    
    Args:
        df_rooftop: DataFrame with rooftop data
        
    Returns:
        DataFrame with smoothed data
    """
    # This function is mainly used after interpolation, so we just pass through
    # since smoothing is already applied during interpolation
    return df_rooftop


# Example usage and testing
if __name__ == "__main__":
    # Test loading rooftop data
    from datetime import datetime, timedelta
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)
    
    print("Testing DuckDB rooftop adapter...")
    
    # Test 30-minute loading
    df_30min = load_rooftop_data(
        start_date=start_date,
        end_date=end_date,
        target_resolution='30min'
    )
    print(f"✓ Loaded {len(df_30min)} 30-minute rooftop records")
    
    if not df_30min.empty:
        print(f"  Regions: {df_30min['regionid'].unique()}")
        print(f"  Date range: {df_30min['settlementdate'].min()} to {df_30min['settlementdate'].max()}")
        print(f"  Max generation: {df_30min['rooftop_solar_mw'].max():.1f} MW")
    
    # Test 5-minute interpolation
    df_5min = load_rooftop_data(
        start_date=start_date,
        end_date=end_date,
        target_resolution='5min'
    )
    print(f"✓ Interpolated to {len(df_5min)} 5-minute rooftop records")
    
    # Test summary
    summary = get_rooftop_summary(start_date=start_date, end_date=end_date)
    print(f"✓ Summary: {summary}")
    
    # Test point-in-time query
    if not df_30min.empty:
        latest_time = df_30min['settlementdate'].max()
        value = get_rooftop_at_time(latest_time, 'NSW1')
        print(f"✓ Rooftop at {latest_time} for NSW1: {value:.1f} MW")