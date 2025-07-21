"""
Transmission Adapter - DuckDB implementation for loading transmission flow data

This module provides functions to load transmission/interconnector flow data
using DuckDB for efficient memory usage.
"""

import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Optional, Union, Dict, Any
from pathlib import Path
import sys

# Add the src directory to Python path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from .config import config
from .logging_config import get_logger
from .performance_logging import PerformanceLogger, performance_monitor
from .resolution_manager import resolution_manager

# Import DuckDB service
from data_service.shared_data_duckdb import duckdb_data_service

logger = get_logger(__name__)
perf_logger = PerformanceLogger(__name__)


@performance_monitor(threshold=1.0)
def load_transmission_data(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    interconnector_id: Optional[str] = None,
    resolution: str = 'auto'
) -> pd.DataFrame:
    """
    Load transmission flow data using DuckDB for efficient memory usage.
    
    Args:
        start_date: Start date for data (if None, loads all available)
        end_date: End date for data (if None, loads all available)
        interconnector_id: Specific interconnector to filter (if None, loads all)
        resolution: Data resolution ('5min', '30min', or 'auto')
        
    Returns:
        DataFrame with columns: settlementdate, interconnectorid, meteredmwflow, 
                               exportlimit, importlimit, mwlosses
    """
    try:
        # Handle date defaults
        if start_date is None or end_date is None:
            date_ranges = duckdb_data_service.get_date_ranges()
            if 'transmission' in date_ranges:
                if start_date is None:
                    start_date = date_ranges['transmission']['start']
                if end_date is None:
                    end_date = date_ranges['transmission']['end']
            else:
                logger.warning("No transmission data range available")
                return pd.DataFrame()
        
        # For now, transmission data is only available in 30-minute resolution
        # TODO: Add 5-minute transmission when available
        use_5min = False
        table = 'transmission_flows_30min'
        
        if resolution == 'auto':
            logger.info("Transmission data only available in 30-minute resolution")
        
        if interconnector_id:
            query = f"""
            SELECT settlementdate, interconnectorid, meteredmwflow, 
                   exportlimit, importlimit, mwlosses
            FROM {table}
            WHERE settlementdate >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
            AND settlementdate <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
            AND interconnectorid = '{interconnector_id}'
            ORDER BY settlementdate
            """
        else:
            query = f"""
            SELECT settlementdate, interconnectorid, meteredmwflow,
                   exportlimit, importlimit, mwlosses
            FROM {table}
            WHERE settlementdate >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
            AND settlementdate <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
            ORDER BY settlementdate, interconnectorid
            """
        
        # Execute query
        with perf_logger.timer("duckdb_transmission_query", threshold=0.5):
            df = duckdb_data_service.conn.execute(query).df()
        
        # Ensure consistent data types
        df['settlementdate'] = pd.to_datetime(df['settlementdate'])
        df['interconnectorid'] = df['interconnectorid'].astype(str)
        
        # Convert numeric columns
        numeric_cols = ['meteredmwflow', 'exportlimit', 'importlimit', 'mwlosses']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        resolution_str = '5min' if use_5min else '30min'
        logger.info(f"Loaded transmission data via DuckDB: {len(df):,} records "
                   f"[resolution={resolution_str}, memory_efficient=True]")
        
        return df
        
    except Exception as e:
        logger.error(f"Error loading transmission data via DuckDB: {e}")
        return pd.DataFrame()


def get_transmission_summary(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Get a summary of transmission data using DuckDB.
    
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
            if 'transmission' in date_ranges:
                if start_date is None:
                    start_date = date_ranges['transmission']['start']
                if end_date is None:
                    end_date = date_ranges['transmission']['end']
        
        # Query for summary statistics
        query = f"""
        SELECT 
            COUNT(*) as record_count,
            COUNT(DISTINCT interconnectorid) as interconnector_count,
            MIN(settlementdate) as start_date,
            MAX(settlementdate) as end_date,
            AVG(meteredmwflow) as avg_flow,
            MAX(ABS(meteredmwflow)) as max_abs_flow
        FROM transmission_flows_30min
        WHERE settlementdate >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
        AND settlementdate <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
        """
        
        result = duckdb_data_service.conn.execute(query).fetchone()
        
        if result and result[0] > 0:
            summary = {
                'record_count': result[0],
                'interconnector_count': result[1],
                'start_date': result[2],
                'end_date': result[3],
                'avg_flow': result[4],
                'max_abs_flow': result[5]
            }
            
            logger.info(f"Transmission summary: {summary['record_count']:,} records, "
                       f"{summary['interconnector_count']} interconnectors")
            
            return summary
        else:
            return {}
            
    except Exception as e:
        logger.error(f"Error getting transmission summary: {e}")
        return {}


def get_available_interconnectors() -> list:
    """
    Get list of available interconnectors from the data using DuckDB.
    
    Returns:
        List of interconnector IDs
    """
    try:
        query = """
        SELECT DISTINCT interconnectorid
        FROM transmission_flows_30min
        ORDER BY interconnectorid
        """
        
        df = duckdb_data_service.conn.execute(query).df()
        interconnectors = df['interconnectorid'].tolist()
        
        logger.info(f"Found {len(interconnectors)} unique interconnectors")
        return interconnectors
        
    except Exception as e:
        logger.error(f"Error getting available interconnectors: {e}")
        return []


def get_flow_statistics(
    interconnector_id: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Get flow statistics for a specific interconnector using DuckDB.
    
    Args:
        interconnector_id: The interconnector to analyze
        start_date: Start date for analysis
        end_date: End date for analysis
        
    Returns:
        Dictionary with flow statistics
    """
    try:
        # Handle date defaults
        if start_date is None or end_date is None:
            date_ranges = duckdb_data_service.get_date_ranges()
            if 'transmission' in date_ranges:
                if start_date is None:
                    start_date = date_ranges['transmission']['start']
                if end_date is None:
                    end_date = date_ranges['transmission']['end']
        
        # Query for statistics
        query = f"""
        SELECT 
            COUNT(*) as record_count,
            AVG(meteredmwflow) as avg_flow,
            MIN(meteredmwflow) as min_flow,
            MAX(meteredmwflow) as max_flow,
            STDDEV(meteredmwflow) as std_flow,
            SUM(CASE WHEN meteredmwflow > 0 THEN 1 ELSE 0 END) as positive_flow_count,
            SUM(CASE WHEN meteredmwflow < 0 THEN 1 ELSE 0 END) as negative_flow_count,
            AVG(exportlimit) as avg_export_limit,
            AVG(importlimit) as avg_import_limit
        FROM transmission_flows_30min
        WHERE interconnectorid = '{interconnector_id}'
        AND settlementdate >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
        AND settlementdate <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
        """
        
        result = duckdb_data_service.conn.execute(query).fetchone()
        
        if result and result[0] > 0:
            stats = {
                'interconnector_id': interconnector_id,
                'record_count': result[0],
                'avg_flow': result[1],
                'min_flow': result[2],
                'max_flow': result[3],
                'std_flow': result[4],
                'positive_flow_pct': (result[5] / result[0]) * 100 if result[0] > 0 else 0,
                'negative_flow_pct': (result[6] / result[0]) * 100 if result[0] > 0 else 0,
                'avg_export_limit': result[7],
                'avg_import_limit': result[8]
            }
            
            logger.info(f"Flow statistics for {interconnector_id}: "
                       f"avg={stats['avg_flow']:.1f}MW, "
                       f"range=[{stats['min_flow']:.1f}, {stats['max_flow']:.1f}]MW")
            
            return stats
        else:
            return {}
            
    except Exception as e:
        logger.error(f"Error getting flow statistics: {e}")
        return {}


# Example usage and testing
if __name__ == "__main__":
    # Test loading transmission data
    from datetime import datetime, timedelta
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)
    
    print("Testing DuckDB transmission adapter...")
    
    # Test basic loading
    df = load_transmission_data(start_date=start_date, end_date=end_date)
    print(f"✓ Loaded {len(df)} transmission records")
    
    if not df.empty:
        print(f"  Interconnectors: {df['interconnectorid'].unique()}")
        print(f"  Date range: {df['settlementdate'].min()} to {df['settlementdate'].max()}")
        print(f"  Avg flow: {df['meteredmwflow'].mean():.1f} MW")
    
    # Test summary
    summary = get_transmission_summary(start_date=start_date, end_date=end_date)
    print(f"✓ Summary: {summary}")
    
    # Test available interconnectors
    interconnectors = get_available_interconnectors()
    print(f"✓ Available interconnectors: {interconnectors}")
    
    # Test flow statistics
    if interconnectors:
        stats = get_flow_statistics(interconnectors[0], start_date=start_date, end_date=end_date)
        print(f"✓ Statistics for {interconnectors[0]}: {stats}")