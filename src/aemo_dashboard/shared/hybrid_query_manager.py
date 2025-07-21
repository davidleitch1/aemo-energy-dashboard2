"""
Hybrid Query Manager - Bridge between DuckDB queries and pandas operations

This module provides a hybrid approach to data loading that uses DuckDB for
efficient querying while maintaining compatibility with existing pandas operations.
It includes smart caching, progressive loading, and memory management.
"""

import time
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Callable, Tuple, Iterator
from collections import OrderedDict
import threading
from functools import wraps

from .logging_config import get_logger
from .performance_logging import PerformanceLogger, performance_monitor
from data_service.shared_data_duckdb import duckdb_data_service

logger = get_logger(__name__)
perf_logger = PerformanceLogger(__name__)


class SmartCache:
    """LRU cache with TTL and size limits for query results"""
    
    def __init__(self, max_size_mb: int = 100, default_ttl: int = 300):
        """
        Initialize smart cache with size and TTL limits.
        
        Args:
            max_size_mb: Maximum cache size in MB
            default_ttl: Default time-to-live in seconds
        """
        self.max_size_bytes = max_size_mb * 1024 * 1024
        self.default_ttl = default_ttl
        self.cache = OrderedDict()
        self.size_tracker = {}
        self.current_size = 0
        self._lock = threading.Lock()
        
        logger.info(f"SmartCache initialized: max_size={max_size_mb}MB, ttl={default_ttl}s")
    
    def _estimate_dataframe_size(self, df: pd.DataFrame) -> int:
        """Estimate memory usage of a DataFrame in bytes"""
        return df.memory_usage(deep=True).sum()
    
    def _evict_lru(self, required_space: int) -> None:
        """Evict least recently used items to make space"""
        while self.current_size + required_space > self.max_size_bytes and self.cache:
            # Remove oldest item
            key, (_, _, size) = self.cache.popitem(last=False)
            self.current_size -= size
            del self.size_tracker[key]
            logger.debug(f"Evicted cache entry: {key}, freed {size/1024/1024:.1f}MB")
    
    def get(self, key: str) -> Optional[pd.DataFrame]:
        """Get item from cache if valid"""
        with self._lock:
            if key not in self.cache:
                return None
            
            timestamp, data, size = self.cache[key]
            
            # Check TTL
            if time.time() - timestamp > self.default_ttl:
                # Expired
                self.current_size -= size
                del self.cache[key]
                del self.size_tracker[key]
                logger.debug(f"Cache entry expired: {key}")
                return None
            
            # Move to end (most recently used)
            self.cache.move_to_end(key)
            return data.copy()  # Return copy to prevent external modifications
    
    def put(self, key: str, data: pd.DataFrame, ttl: Optional[int] = None) -> None:
        """Store item in cache with TTL"""
        with self._lock:
            # Estimate size
            size = self._estimate_dataframe_size(data)
            
            # Check if we need to evict
            if size > self.max_size_bytes:
                logger.warning(f"DataFrame too large for cache: {size/1024/1024:.1f}MB")
                return
            
            # Evict if necessary
            self._evict_lru(size)
            
            # Store
            timestamp = time.time()
            self.cache[key] = (timestamp, data.copy(), size)
            self.size_tracker[key] = size
            self.current_size += size
            
            logger.debug(f"Cached {key}: {size/1024/1024:.1f}MB, total cache: {self.current_size/1024/1024:.1f}MB")
    
    def clear(self) -> None:
        """Clear all cache entries"""
        with self._lock:
            self.cache.clear()
            self.size_tracker.clear()
            self.current_size = 0
            logger.info("Cache cleared")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        with self._lock:
            return {
                'entries': len(self.cache),
                'size_mb': self.current_size / 1024 / 1024,
                'max_size_mb': self.max_size_bytes / 1024 / 1024,
                'utilization': self.current_size / self.max_size_bytes * 100
            }


class HybridQueryManager:
    """
    Hybrid manager that uses DuckDB for queries but returns processed DataFrames.
    
    This manager provides:
    - Smart caching of query results
    - Progressive loading with chunk support
    - Integration with existing pandas workflows
    - Memory-efficient data loading
    """
    
    def __init__(self, cache_size_mb: int = 100, cache_ttl: int = 300):
        """
        Initialize the hybrid query manager.
        
        Args:
            cache_size_mb: Maximum cache size in MB
            cache_ttl: Default cache TTL in seconds
        """
        self.conn = duckdb_data_service.conn
        self.cache = SmartCache(max_size_mb=cache_size_mb, default_ttl=cache_ttl)
        self._query_count = 0
        self._cache_hits = 0
        
        logger.info("HybridQueryManager initialized")
    
    def _build_cache_key(self, *args, **kwargs) -> str:
        """Build cache key from query parameters"""
        # Convert args and kwargs to a stable string representation
        key_parts = [str(arg) for arg in args]
        key_parts.extend(f"{k}={v}" for k, v in sorted(kwargs.items()))
        return "|".join(key_parts)
    
    @performance_monitor(threshold=1.0)
    def query_integrated_data(
        self,
        start_date: datetime,
        end_date: datetime,
        columns: Optional[List[str]] = None,
        resolution: str = '30min',
        use_cache: bool = True
    ) -> pd.DataFrame:
        """
        Query integrated data (generation + price + DUID info) with smart caching.
        
        Args:
            start_date: Start date for query
            end_date: End date for query
            columns: Specific columns to return (None = all)
            resolution: Data resolution ('5min' or '30min')
            use_cache: Whether to use cache
            
        Returns:
            DataFrame with integrated data
        """
        # Build cache key
        cache_key = self._build_cache_key(
            'integrated_data', start_date, end_date, columns, resolution
        )
        
        # Check cache
        if use_cache:
            cached_result = self.cache.get(cache_key)
            if cached_result is not None:
                self._cache_hits += 1
                logger.debug(f"Cache hit for integrated data: {start_date} to {end_date}")
                return cached_result
        
        self._query_count += 1
        
        # Build column list
        if columns is None:
            select_columns = "*"
        else:
            select_columns = ", ".join(columns)
        
        # Build query based on resolution
        if resolution == '5min':
            query = f"""
            SELECT {select_columns}
            FROM (
                SELECT 
                    g.settlementdate,
                    g.duid,
                    g.scadavalue,
                    d."Site Name" as station_name,
                    d.Owner as owner,
                    d.Fuel as fuel_type,
                    d.Region as region,
                    d."Capacity(MW)" as nameplate_capacity,
                    p.rrp,
                    g.scadavalue * p.rrp * (5.0/60.0) as revenue
                FROM generation_5min g
                LEFT JOIN duid_mapping d ON g.duid = d.DUID
                LEFT JOIN prices_5min p 
                    ON g.settlementdate = p.settlementdate 
                    AND d.Region = p.regionid
                WHERE g.settlementdate >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
                  AND g.settlementdate <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
            ) t
            """
        else:
            query = f"""
            SELECT {select_columns}
            FROM (
                SELECT 
                    g.settlementdate,
                    g.duid,
                    g.scadavalue,
                    d."Site Name" as station_name,
                    d.Owner as owner,
                    d.Fuel as fuel_type,
                    d.Region as region,
                    d."Capacity(MW)" as nameplate_capacity,
                    p.rrp,
                    g.scadavalue * p.rrp / 2 as revenue
                FROM generation_30min g
                LEFT JOIN duid_mapping d ON g.duid = d.DUID
                LEFT JOIN prices_30min p 
                    ON g.settlementdate = p.SETTLEMENTDATE 
                    AND d.Region = p.REGIONID
                WHERE g.settlementdate >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
                  AND g.settlementdate <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
            ) t
            """
        
        # Execute query
        with perf_logger.timer("duckdb_integrated_query", threshold=0.5):
            result = self.conn.execute(query).df()
        
        # Ensure proper data types
        if not result.empty:
            result['settlementdate'] = pd.to_datetime(result['settlementdate'])
            
            # Convert numeric columns
            numeric_cols = ['scadavalue', 'nameplate_capacity', 'rrp', 'revenue']
            for col in numeric_cols:
                if col in result.columns:
                    result[col] = pd.to_numeric(result[col], errors='coerce')
        
        # Cache result
        if use_cache and not result.empty:
            self.cache.put(cache_key, result)
        
        logger.info(f"Loaded {len(result):,} integrated records for {start_date} to {end_date}")
        
        return result
    
    def query_with_progress(
        self,
        query: str,
        chunk_size: int = 50000,
        progress_callback: Optional[Callable[[int], None]] = None
    ) -> pd.DataFrame:
        """
        Execute query with progress updates, loading in chunks.
        
        Args:
            query: SQL query to execute
            chunk_size: Number of rows per chunk
            progress_callback: Function to call with progress percentage
            
        Returns:
            Complete DataFrame result
        """
        # First, get total row count
        count_query = f"SELECT COUNT(*) FROM ({query}) t"
        total_rows = self.conn.execute(count_query).fetchone()[0]
        
        if total_rows == 0:
            return pd.DataFrame()
        
        logger.info(f"Loading {total_rows:,} rows in chunks of {chunk_size:,}")
        
        chunks = []
        rows_loaded = 0
        
        # Load in chunks
        for offset in range(0, total_rows, chunk_size):
            chunk_query = f"{query} LIMIT {chunk_size} OFFSET {offset}"
            
            with perf_logger.timer("chunk_load", threshold=0.1):
                chunk = self.conn.execute(chunk_query).df()
            
            chunks.append(chunk)
            rows_loaded += len(chunk)
            
            # Report progress
            if progress_callback:
                progress = int(rows_loaded / total_rows * 100)
                progress_callback(progress)
            
            # Log progress
            if rows_loaded % (chunk_size * 10) == 0:
                logger.info(f"Progress: {rows_loaded:,}/{total_rows:,} rows ({rows_loaded/total_rows*100:.1f}%)")
        
        # Combine chunks
        result = pd.concat(chunks, ignore_index=True)
        
        logger.info(f"Completed loading {len(result):,} rows")
        
        return result
    
    def query_chunks(
        self,
        query: str,
        chunk_size: int = 100000
    ) -> Iterator[pd.DataFrame]:
        """
        Stream query results in chunks for memory-efficient processing.
        
        Args:
            query: SQL query to execute
            chunk_size: Number of rows per chunk
            
        Yields:
            DataFrame chunks
        """
        offset = 0
        
        while True:
            chunk_query = f"{query} LIMIT {chunk_size} OFFSET {offset}"
            chunk = self.conn.execute(chunk_query).df()
            
            if chunk.empty:
                break
            
            yield chunk
            offset += chunk_size
            
            logger.debug(f"Yielded chunk: offset={offset}, size={len(chunk)}")
    
    def aggregate_by_group(
        self,
        start_date: datetime,
        end_date: datetime,
        group_by: List[str],
        aggregations: Dict[str, str],
        resolution: str = '30min',
        use_cache: bool = True
    ) -> pd.DataFrame:
        """
        Perform aggregation query with grouping.
        
        Args:
            start_date: Start date
            end_date: End date
            group_by: Columns to group by
            aggregations: Dict of column -> aggregation function
            resolution: Data resolution
            use_cache: Whether to use cache
            
        Returns:
            Aggregated DataFrame
        """
        # Build cache key
        cache_key = self._build_cache_key(
            'aggregate', start_date, end_date, group_by, aggregations, resolution
        )
        
        # Check cache
        if use_cache:
            cached_result = self.cache.get(cache_key)
            if cached_result is not None:
                self._cache_hits += 1
                return cached_result
        
        # Build aggregation expressions
        agg_expressions = []
        for col, func in aggregations.items():
            if func.upper() == 'SUM':
                agg_expressions.append(f"SUM({col}) as {col}_sum")
            elif func.upper() == 'AVG':
                agg_expressions.append(f"AVG({col}) as {col}_avg")
            elif func.upper() == 'MAX':
                agg_expressions.append(f"MAX({col}) as {col}_max")
            elif func.upper() == 'MIN':
                agg_expressions.append(f"MIN({col}) as {col}_min")
            elif func.upper() == 'COUNT':
                agg_expressions.append(f"COUNT({col}) as {col}_count")
        
        # Build query - use integrated_data view which has all fields
        table = f"integrated_data_{resolution}"
        query = f"""
        SELECT 
            {', '.join(group_by)},
            {', '.join(agg_expressions)}
        FROM {table}
        WHERE settlementdate >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
          AND settlementdate <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
        GROUP BY {', '.join(group_by)}
        ORDER BY {', '.join(group_by)}
        """
        
        # Execute
        result = self.conn.execute(query).df()
        
        # Cache if not empty
        if use_cache and not result.empty:
            self.cache.put(cache_key, result)
        
        return result
    
    def get_date_ranges(self) -> Dict[str, Dict[str, Any]]:
        """Get available date ranges from DuckDB service"""
        return duckdb_data_service.get_date_ranges()
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get query manager statistics"""
        cache_stats = self.cache.get_stats()
        
        hit_rate = (self._cache_hits / self._query_count * 100) if self._query_count > 0 else 0
        
        return {
            'query_count': self._query_count,
            'cache_hits': self._cache_hits,
            'cache_hit_rate': hit_rate,
            'cache_stats': cache_stats
        }
    
    def clear_cache(self) -> None:
        """Clear all cached data"""
        self.cache.clear()
        logger.info("Query manager cache cleared")


# Example usage and testing
if __name__ == "__main__":
    # Test the hybrid query manager
    manager = HybridQueryManager()
    
    # Test integrated data query
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    print("Testing HybridQueryManager...")
    
    # First query (cache miss)
    start_time = time.time()
    df1 = manager.query_integrated_data(start_date, end_date)
    query_time1 = time.time() - start_time
    print(f"First query: {len(df1)} rows in {query_time1:.2f}s")
    
    # Second query (cache hit)
    start_time = time.time()
    df2 = manager.query_integrated_data(start_date, end_date)
    query_time2 = time.time() - start_time
    print(f"Second query: {len(df2)} rows in {query_time2:.2f}s (cache hit)")
    
    # Show statistics
    stats = manager.get_statistics()
    print(f"\nStatistics:")
    print(f"  Queries: {stats['query_count']}")
    print(f"  Cache hits: {stats['cache_hits']}")
    print(f"  Hit rate: {stats['cache_hit_rate']:.1f}%")
    print(f"  Cache size: {stats['cache_stats']['size_mb']:.1f}MB")