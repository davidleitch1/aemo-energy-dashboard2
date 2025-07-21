"""
Fast Hybrid Query Manager with lazy initialization
"""
import os
import duckdb
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Union
import threading
from functools import lru_cache

from .logging_config import get_logger
from .performance_logger import PerformanceLogger
from .duckdb_views_lazy import create_lazy_views

logger = get_logger(__name__)
perf_logger = PerformanceLogger(__name__)

class FastHybridQueryManager:
    """Fast query manager with lazy initialization and smart caching"""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        """Singleton pattern for shared instance"""
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, cache_size_mb: int = 100, cache_ttl: int = 300):
        """Initialize with minimal overhead"""
        if hasattr(self, '_initialized'):
            return
            
        self.cache_size_mb = cache_size_mb
        self.cache_ttl = cache_ttl
        
        # Defer DuckDB connection
        self._conn = None
        self._views = None
        self._cache = {}
        self._cache_timestamps = {}
        self._initialized = True
        
        logger.info(f"FastHybridQueryManager initialized (lazy mode)")
    
    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        """Get or create DuckDB connection"""
        if self._conn is None:
            self._conn = duckdb.connect(':memory:', config={'threads': 4})
            logger.info("DuckDB connection created")
        return self._conn
    
    @property
    def views(self):
        """Get or create lazy views"""
        if self._views is None:
            self._views = create_lazy_views(self.conn)
            logger.info("Lazy views manager created")
        return self._views
    
    def query_with_progress(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute query with progress tracking"""
        # For fast startup, just execute directly
        with perf_logger.timer("query_execution"):
            if params:
                result = self.conn.execute(query, params).df()
            else:
                result = self.conn.execute(query).df()
            
        logger.debug(f"Query returned {len(result)} rows")
        return result
    
    def get_cached_or_query(self, 
                          cache_key: str,
                          query: str,
                          params: Optional[Dict] = None) -> pd.DataFrame:
        """Get from cache or execute query"""
        # Check cache
        if cache_key in self._cache:
            timestamp = self._cache_timestamps.get(cache_key, 0)
            if time.time() - timestamp < self.cache_ttl:
                logger.debug(f"Cache hit for {cache_key}")
                return self._cache[cache_key].copy()
        
        # Execute query
        result = self.query_with_progress(query, params)
        
        # Cache result
        self._cache[cache_key] = result
        self._cache_timestamps[cache_key] = time.time()
        
        # Simple cache eviction
        if len(self._cache) > 20:
            oldest_key = min(self._cache_timestamps, key=self._cache_timestamps.get)
            del self._cache[oldest_key]
            del self._cache_timestamps[oldest_key]
        
        return result
    
    def ensure_view(self, view_name: str):
        """Ensure a view exists (lazy creation)"""
        self.views.ensure_view(view_name)
    
    def query_generation_by_fuel(self,
                               start_date: datetime,
                               end_date: datetime,
                               region: Optional[str] = None,
                               resolution: str = 'auto') -> pd.DataFrame:
        """Query generation by fuel type with lazy view creation"""
        # Determine resolution
        if resolution == 'auto':
            days_diff = (end_date - start_date).days
            resolution = '5min' if days_diff < 7 else '30min'
        
        # Ensure view exists
        view_name = f'generation_by_fuel_{resolution}'
        self.ensure_view(view_name)
        
        # Build query
        query = f"""
        SELECT * FROM {view_name}
        WHERE settlementdate BETWEEN ? AND ?
        """
        params = [start_date, end_date]
        
        if region and region != 'NEM':
            query += " AND region = ?"
            params.append(region)
        
        query += " ORDER BY settlementdate, fuel_type"
        
        # Cache key
        cache_key = f"gen_fuel_{resolution}_{start_date}_{end_date}_{region}"
        
        return self.get_cached_or_query(cache_key, query, params)
    
    def query_prices(self,
                    start_date: datetime,
                    end_date: datetime,
                    region: Optional[str] = None,
                    resolution: str = 'auto') -> pd.DataFrame:
        """Query price data with lazy view creation"""
        # Determine resolution
        if resolution == 'auto':
            days_diff = (end_date - start_date).days
            resolution = '5min' if days_diff < 7 else '30min'
        
        # Ensure view exists
        view_name = f'prices_{resolution}'
        self.ensure_view(view_name)
        
        # Build query
        query = f"""
        SELECT * FROM {view_name}
        WHERE settlementdate BETWEEN ? AND ?
        """
        params = [start_date, end_date]
        
        if region:
            query += " AND regionid = ?"
            params.append(region)
        
        query += " ORDER BY settlementdate"
        
        # Cache key
        cache_key = f"prices_{resolution}_{start_date}_{end_date}_{region}"
        
        return self.get_cached_or_query(cache_key, query, params)


# Import time module
import time