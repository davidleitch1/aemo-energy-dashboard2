"""
Generation Query Manager - Specialized query manager for generation dashboard

This module provides optimized queries for the generation dashboard using DuckDB
views to aggregate data by fuel type, dramatically reducing data volume and
improving performance.
"""

import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple

from ..shared.logging_config import get_logger
from ..shared.performance_logging import PerformanceLogger, performance_monitor
from ..shared.hybrid_query_manager import HybridQueryManager
from ..shared.duckdb_views import view_manager

logger = get_logger(__name__)
perf_logger = PerformanceLogger(__name__)


class GenerationQueryManager:
    """Specialized query manager for generation dashboard"""
    
    def __init__(self):
        """Initialize with hybrid query manager and ensure views exist"""
        self.query_manager = HybridQueryManager(cache_size_mb=200, cache_ttl=300)
        
        # Ensure all views are created
        view_manager.create_all_views()
        
        logger.info("GenerationQueryManager initialized with 200MB cache")
    
    @performance_monitor(threshold=1.0)
    def query_generation_by_fuel(
        self,
        start_date: datetime,
        end_date: datetime,
        region: str = 'NEM',
        resolution: str = 'auto'
    ) -> pd.DataFrame:
        """
        Query generation data pre-aggregated by fuel type.
        
        Args:
            start_date: Start of date range
            end_date: End of date range
            region: Region filter ('NEM' for all regions)
            resolution: 'auto', '5min', or '30min'
            
        Returns:
            DataFrame with columns: settlementdate, fuel_type, total_generation_mw
        """
        try:
            # Determine resolution
            if resolution == 'auto':
                days_diff = (end_date - start_date).days
                if days_diff > 365:
                    resolution = 'daily'
                    view_name = 'daily_generation_by_fuel'
                    logger.info(f"Auto-selected daily aggregation for {days_diff} day range")
                elif days_diff <= 7:
                    resolution = '5min'
                    view_name = 'generation_by_fuel_5min'
                    logger.info(f"Auto-selected 5min resolution for {days_diff} day range")
                else:
                    resolution = '30min'
                    view_name = 'generation_by_fuel_30min'
                    logger.info(f"Auto-selected 30min resolution for {days_diff} day range")
            else:
                view_name = f'generation_by_fuel_{resolution}'
            
            # Build query based on region and resolution
            if resolution == 'daily':
                # Daily aggregation has different columns
                if region == 'NEM':
                    query = f"""
                        SELECT 
                            date as settlementdate,
                            fuel_type,
                            SUM(avg_generation_mw) as total_generation_mw
                        FROM {view_name}
                        WHERE date >= '{start_date.strftime('%Y-%m-%d')}'
                        AND date <= '{end_date.strftime('%Y-%m-%d')}'
                        GROUP BY date, fuel_type
                        ORDER BY date, fuel_type
                    """
                else:
                    query = f"""
                        SELECT 
                            date as settlementdate,
                            fuel_type,
                            avg_generation_mw as total_generation_mw
                        FROM {view_name}
                        WHERE date >= '{start_date.strftime('%Y-%m-%d')}'
                        AND date <= '{end_date.strftime('%Y-%m-%d')}'
                        AND region = '{region}'
                        ORDER BY date, fuel_type
                    """
            elif region == 'NEM':
                # For NEM, sum across all regions
                query = f"""
                    SELECT 
                        settlementdate,
                        fuel_type,
                        SUM(total_generation_mw) as total_generation_mw,
                        SUM(total_capacity_mw) as total_capacity_mw,
                        COUNT(DISTINCT region) as region_count
                    FROM {view_name}
                    WHERE settlementdate >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
                    AND settlementdate <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
                    GROUP BY settlementdate, fuel_type
                    ORDER BY settlementdate, fuel_type
                """
            else:
                # For specific region
                query = f"""
                    SELECT 
                        settlementdate,
                        fuel_type,
                        total_generation_mw,
                        total_capacity_mw,
                        unit_count
                    FROM {view_name}
                    WHERE settlementdate >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
                    AND settlementdate <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
                    AND region = '{region}'
                    ORDER BY settlementdate, fuel_type
                """
            
            # Create cache key
            actual_resolution = resolution if resolution != 'auto' else ('daily' if days_diff > 365 else ('5min' if days_diff <= 7 else '30min'))
            cache_key = f"gen_by_fuel_{region}_{start_date.date()}_{end_date.date()}_{actual_resolution}"
            
            # Check cache first
            cached_result = self.query_manager.cache.get(cache_key)
            if cached_result is not None:
                logger.debug(f"Cache hit for {cache_key}")
                return cached_result
            
            # Query directly without chunking for better performance
            with perf_logger.timer("query_generation_by_fuel", threshold=0.5):
                # For large aggregated queries, use direct execution instead of chunking
                result = self.query_manager.conn.execute(query).df()
            
            # Cache the result
            self.query_manager.cache.put(cache_key, result)
            
            logger.info(f"Loaded {len(result):,} aggregated records for {region} "
                       f"({start_date.date()} to {end_date.date()})")
            
            return result
            
        except Exception as e:
            logger.error(f"Error querying generation by fuel: {e}")
            return pd.DataFrame()
    
    @performance_monitor(threshold=1.0)
    def query_capacity_utilization(
        self,
        start_date: datetime,
        end_date: datetime,
        region: str = 'NEM',
        resolution: str = '30min'
    ) -> pd.DataFrame:
        """
        Query capacity utilization data by fuel type.
        
        Args:
            start_date: Start of date range
            end_date: End of date range
            region: Region filter
            resolution: Data resolution (only 30min supported currently)
            
        Returns:
            DataFrame with columns: settlementdate, fuel_type, utilization_pct
        """
        try:
            view_name = 'capacity_utilization_30min'
            
            if region == 'NEM':
                # For NEM, calculate weighted average utilization
                query = f"""
                    SELECT 
                        settlementdate,
                        fuel_type,
                        SUM(total_generation_mw) / NULLIF(SUM(total_capacity_mw), 0) * 100 as utilization_pct
                    FROM {view_name}
                    WHERE settlementdate >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
                    AND settlementdate <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
                    GROUP BY settlementdate, fuel_type
                    ORDER BY settlementdate, fuel_type
                """
            else:
                query = f"""
                    SELECT 
                        settlementdate,
                        fuel_type,
                        utilization_pct
                    FROM {view_name}
                    WHERE settlementdate >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
                    AND settlementdate <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
                    AND region = '{region}'
                    ORDER BY settlementdate, fuel_type
                """
            
            cache_key = f"capacity_util_{region}_{start_date.date()}_{end_date.date()}"
            
            # Check cache first
            cached_result = self.query_manager.cache.get(cache_key)
            if cached_result is not None:
                logger.debug(f"Cache hit for {cache_key}")
                return cached_result
            
            with perf_logger.timer("query_capacity_utilization"):
                # Direct query for better performance
                result = self.query_manager.conn.execute(query).df()
            
            # Cache the result
            self.query_manager.cache.put(cache_key, result)
            
            return result
            
        except Exception as e:
            logger.error(f"Error querying capacity utilization: {e}")
            return pd.DataFrame()
    
    @performance_monitor(threshold=0.5)
    def query_fuel_capacities(self, region: str = 'NEM') -> Dict[str, float]:
        """
        Get total capacity by fuel type for a region.
        
        Args:
            region: Region filter
            
        Returns:
            Dictionary of fuel_type -> capacity_mw
        """
        try:
            if region == 'NEM':
                query = """
                    SELECT 
                        d.Fuel as fuel_type,
                        SUM(d."Capacity(MW)") as total_capacity_mw
                    FROM duid_mapping d
                    WHERE d.Fuel IS NOT NULL
                    GROUP BY d.Fuel
                """
            else:
                query = f"""
                    SELECT 
                        d.Fuel as fuel_type,
                        SUM(d."Capacity(MW)") as total_capacity_mw
                    FROM duid_mapping d
                    WHERE d.Fuel IS NOT NULL
                    AND d.Region = '{region}'
                    GROUP BY d.Fuel
                """
            
            result = self.query_manager.conn.execute(query).df()
            
            # Convert to dictionary
            capacities = {}
            for _, row in result.iterrows():
                capacities[row['fuel_type']] = row['total_capacity_mw']
            
            logger.info(f"Loaded capacities for {len(capacities)} fuel types in {region}")
            return capacities
            
        except Exception as e:
            logger.error(f"Error querying fuel capacities: {e}")
            return {}
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get query manager statistics"""
        return self.query_manager.get_statistics()
    
    def clear_cache(self) -> None:
        """Clear the query cache"""
        self.query_manager.clear_cache()
        logger.info("Generation query cache cleared")


# Example usage and testing
if __name__ == "__main__":
    import time
    
    print("Testing GenerationQueryManager...")
    
    manager = GenerationQueryManager()
    
    # Test 1: Query 24 hours
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)
    
    print(f"\n1. Querying 24 hours of data...")
    t1 = time.time()
    data = manager.query_generation_by_fuel(start_date, end_date, 'NSW1')
    t1_duration = time.time() - t1
    
    print(f"✓ Query completed in {t1_duration:.2f}s")
    print(f"✓ Records: {len(data):,}")
    if not data.empty:
        print(f"✓ Fuel types: {sorted(data['fuel_type'].unique())}")
        print(f"✓ Total generation: {data['total_generation_mw'].sum():,.0f} MW")
    
    # Test 2: Query 1 year (NEM)
    print(f"\n2. Querying 1 year of NEM data...")
    year_start = end_date - timedelta(days=365)
    
    t2 = time.time()
    data_year = manager.query_generation_by_fuel(year_start, end_date, 'NEM')
    t2_duration = time.time() - t2
    
    print(f"✓ Query completed in {t2_duration:.2f}s")
    print(f"✓ Records: {len(data_year):,}")
    
    # Test 3: Cache test
    print(f"\n3. Testing cache...")
    t3 = time.time()
    data_cached = manager.query_generation_by_fuel(start_date, end_date, 'NSW1')
    t3_duration = time.time() - t3
    
    print(f"✓ Cached query completed in {t3_duration:.2f}s (vs {t1_duration:.2f}s original)")
    
    # Show statistics
    print(f"\n4. Cache statistics:")
    stats = manager.get_statistics()
    for key, value in stats.items():
        print(f"  {key}: {value}")