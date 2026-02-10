"""
DuckDB-based Shared Data Service - Zero memory footprint approach

This service uses DuckDB to query parquet files directly without loading
them into memory. Only query results are kept in memory.

When AEMO_DUCKDB_PATH is set, queries use the external DuckDB database
with per-request read-only connections and retry on lock conflict. This
allows the collector to write to the same file without blocking dashboard
reads.
"""

import os
import time

import duckdb
import pandas as pd
import pickle
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from functools import lru_cache

from aemo_dashboard.shared.config import config
from aemo_dashboard.shared.logging_config import get_logger
from aemo_dashboard.shared.performance_logging import PerformanceLogger
from aemo_dashboard.shared.constants import MINUTES_5_TO_HOURS, MINUTES_30_TO_HOURS

logger = get_logger(__name__)
perf_logger = PerformanceLogger(__name__)


class _RetryQueryResult:
    """Wraps a DuckDB query result, closing the connection after data extraction."""

    def __init__(self, conn, result):
        self._conn = conn
        self._result = result

    def df(self):
        try:
            return self._result.df()
        finally:
            self._conn.close()

    def fetchone(self):
        try:
            return self._result.fetchone()
        finally:
            self._conn.close()

    def fetchall(self):
        try:
            return self._result.fetchall()
        finally:
            self._conn.close()


class _RetryConnection:
    """Opens a fresh read-only DuckDB connection per execute() with retry on lock conflict.

    The collector holds an exclusive write lock for ~5s every 4.5 minutes.
    This class transparently retries on lock conflict (IOException), so
    dashboard queries succeed after a brief delay (~200ms) in the rare
    case of a collision (~2% probability).
    """

    def __init__(self, db_path, max_retries=3, retry_delay=0.2):
        self._db_path = str(db_path)
        self._max_retries = max_retries
        self._retry_delay = retry_delay

    def execute(self, query):
        last_error = None
        for attempt in range(self._max_retries):
            try:
                conn = duckdb.connect(self._db_path, read_only=True)
                conn.execute("SET memory_limit='2GB'")
                conn.execute("SET threads=4")
                result = conn.execute(query)
                return _RetryQueryResult(conn, result)
            except duckdb.IOException as e:
                last_error = e
                if attempt < self._max_retries - 1:
                    delay = self._retry_delay * (attempt + 1)
                    logger.debug(f"DuckDB lock conflict (attempt {attempt + 1}/{self._max_retries}), "
                                 f"retrying in {delay:.1f}s...")
                    time.sleep(delay)
        raise last_error


class DuckDBDataService:
    """
    DuckDB-based data service that queries parquet files directly.

    Advantages:
    - Near-zero memory footprint (only query results in memory)
    - Fast parallel query execution
    - Handles 5+ years of data efficiently
    - No data duplication
    - Persistent DB file preserves views between restarts
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
            cls._instance._conn = None
            cls._instance._external_db_path = os.getenv('AEMO_DUCKDB_PATH')
        return cls._instance

    @property
    def conn(self):
        """Lazy initialization of DuckDB connection.

        When using external DuckDB (AEMO_DUCKDB_PATH set), returns a
        _RetryConnection that opens fresh read-only connections per query.
        Otherwise returns the persistent connection to aemo_cache.duckdb.
        """
        if self._conn is None:
            self._initialize_connection()
        return self._conn

    @property
    def _duid_join_table(self):
        """Table/view name for DUID joins in SQL queries.

        External DB has lowercase columns in duid_mapping table,
        but duid_info view has the aliased column names the dashboard expects.
        """
        return 'duid_info' if self._external_db_path else 'duid_mapping'

    def _initialize_connection(self):
        """Initialize the DuckDB connection and views"""
        if self._external_db_path:
            self._initialize_external_db()
        else:
            self._initialize_cache_db()

    def _initialize_external_db(self):
        """Initialize using external DuckDB file with per-request connections."""
        logger.info(f"Using external DuckDB: {self._external_db_path}")

        with perf_logger.timer("duckdb_init", threshold=1.0):
            # Load duid_mapping into memory (one-time direct connection)
            for attempt in range(3):
                try:
                    conn = duckdb.connect(self._external_db_path, read_only=True)
                    self.duid_mapping = conn.execute("SELECT * FROM duid_info").df()
                    conn.close()
                    logger.info(f"Loaded {len(self.duid_mapping)} DUID mappings from duid_info")
                    break
                except duckdb.IOException:
                    if attempt < 2:
                        time.sleep(0.3 * (attempt + 1))
                    else:
                        logger.error("Could not load DUID mapping — collector may be writing")
                        self.duid_mapping = pd.DataFrame()

            # Create retry connection for all subsequent queries
            self._conn = _RetryConnection(self._external_db_path)

        self._initialized = True
        logger.info("External DuckDB Data Service initialized")

    def _initialize_cache_db(self):
        """Initialize using persistent cache DB with parquet views (original behavior)."""
        logger.info("Initializing DuckDB Data Service...")

        with perf_logger.timer("duckdb_init", threshold=1.0):
            # Use persistent DuckDB file in data directory
            db_path = Path(config.data_dir) / 'aemo_cache.duckdb'
            self._conn = duckdb.connect(str(db_path))

            # Configure DuckDB for better performance
            self._conn.execute("SET memory_limit='2GB'")
            self._conn.execute("SET threads=4")

            # Check if views already exist (persistent DB)
            if self._views_exist():
                logger.info("Using existing views from persistent DB")
                # Still need DUID mapping in memory for quick access
                self._load_duid_mapping_from_db()
            else:
                logger.info("Creating views for first time...")
                # Register parquet files as views
                self._register_data_views()

                # Load DUID mapping (this is small, keep in memory)
                self._load_duid_mapping()

                # Create helper views for common joins
                self._create_helper_views()

        self._initialized = True
        logger.info("DuckDB Data Service initialized")

    def _views_exist(self) -> bool:
        """Check if core views already exist in persistent DB"""
        try:
            result = self._conn.execute("""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_name = 'generation_30min' AND table_type = 'VIEW'
            """).fetchone()
            return result[0] > 0
        except Exception:
            return False

    def _load_duid_mapping_from_db(self):
        """Load DUID mapping from existing DB table"""
        try:
            self.duid_mapping = self._conn.execute("SELECT * FROM duid_mapping").df()
            logger.info(f"Loaded {len(self.duid_mapping)} DUID mappings from DB")
        except Exception as e:
            logger.warning(f"Could not load DUID mapping from DB: {e}")
            self._load_duid_mapping()

    def __init__(self):
        """Initialize the DuckDB data service - actual work deferred to first access"""
        pass
    
    def _register_data_views(self):
        """Register parquet files as DuckDB views"""
        logger.info("Registering parquet files as views...")
        
        # Generation data
        gen_5_path = str(config.scada5_file)
        gen_30_path = str(config.scada30_file)
        
        self._conn.execute(f"""
            CREATE VIEW generation_30min AS
            SELECT * FROM read_parquet('{gen_30_path}')
        """)

        self._conn.execute(f"""
            CREATE VIEW generation_5min AS
            SELECT * FROM read_parquet('{gen_5_path}')
        """)

        # Price data
        price_30_path = str(config.spot_hist_file).replace('prices5.parquet', 'prices30.parquet')
        price_5_path = str(config.spot_hist_file)

        self._conn.execute(f"""
            CREATE VIEW prices_30min AS
            SELECT
                settlementdate,
                regionid,
                rrp
            FROM read_parquet('{price_30_path}')
        """)

        self._conn.execute(f"""
            CREATE VIEW prices_5min AS
            SELECT
                settlementdate,
                regionid,
                rrp
            FROM read_parquet('{price_5_path}')
        """)

        # Transmission data
        trans_30_path = str(config.transmission_output_file).replace('transmission5.parquet', 'transmission30.parquet')

        self._conn.execute(f"""
            CREATE VIEW transmission_30min AS
            SELECT * FROM read_parquet('{trans_30_path}')
        """)
        
        # Rooftop solar - rename 'power' column to 'rooftop_solar_mw' for consistency
        # Note: DuckDB may not see all columns due to parquet format compatibility
        # Only select columns that DuckDB can reliably read
        self._conn.execute(f"""
            CREATE VIEW rooftop_solar AS
            SELECT
                settlementdate,
                regionid,
                power AS rooftop_solar_mw
            FROM read_parquet('{config.rooftop_solar_file}')
        """)
        
        logger.info("All parquet files registered as views")
    
    def _load_duid_mapping(self):
        """Load DUID mapping into DuckDB"""
        try:
            # Load the pickle file
            with open(config.gen_info_file, 'rb') as f:
                duid_df = pickle.load(f)
            
            if not isinstance(duid_df, pd.DataFrame):
                duid_df = pd.DataFrame(duid_df)
            
            # Register as DuckDB table
            self._conn.register('duid_info', duid_df)

            # Create a permanent table for better performance
            self._conn.execute("""
                CREATE TABLE duid_mapping AS
                SELECT * FROM duid_info
            """)
            
            # Also keep in memory for quick access
            self.duid_mapping = duid_df
            
            logger.info(f"Loaded {len(duid_df)} DUID mappings into DuckDB")
            
        except Exception as e:
            logger.error(f"Error loading DUID mapping: {e}")
            self.duid_mapping = pd.DataFrame()
    
    def _create_helper_views(self):
        """Create helper views for common queries"""
        # Generation with fuel type and region
        self._conn.execute("""
            CREATE VIEW generation_enriched_30min AS
            SELECT 
                g.settlementdate,
                g.duid,
                g.scadavalue,
                d.Fuel as fuel_type,
                d.Region as region,
                d."Site Name" as station_name,
                d."Capacity(MW)" as nameplate_capacity
            FROM generation_30min g
            LEFT JOIN duid_mapping d ON g.duid = d.DUID
        """)
        
        logger.info("Created helper views for common joins")
    
    def get_memory_usage(self) -> float:
        """Get memory usage (mainly just DUID mapping)"""
        if hasattr(self, 'duid_mapping') and not self.duid_mapping.empty:
            return self.duid_mapping.memory_usage(deep=True).sum() / 1024 / 1024
        return 0.1  # Minimal memory for DuckDB connection
    
    def get_date_ranges(self) -> Dict[str, Dict[str, Any]]:
        """Get available date ranges for all data types"""
        ranges = {}
        
        # Query each data source for date ranges
        for name, table in [
            ('generation', 'generation_30min'),
            ('prices', 'prices_30min'),
            ('transmission', 'transmission_30min'),
            ('rooftop', 'rooftop_solar')
        ]:
            try:
                result = self.conn.execute(f"""
                    SELECT 
                        MIN(settlementdate) as start_date,
                        MAX(settlementdate) as end_date,
                        COUNT(*) as record_count
                    FROM {table}
                """).fetchone()
                
                if result:
                    ranges[name] = {
                        'start': pd.Timestamp(result[0]),
                        'end': pd.Timestamp(result[1]),
                        'records': result[2]
                    }
            except Exception as e:
                logger.error(f"Error getting date range for {name}: {e}")
        
        return ranges
    
    def get_regions(self) -> List[str]:
        """Get list of available regions"""
        if 'Region' in self.duid_mapping.columns:
            return sorted(self.duid_mapping['Region'].dropna().unique().tolist())
        return []
    
    def get_fuel_types(self) -> List[str]:
        """Get list of available fuel types"""
        if 'Fuel' in self.duid_mapping.columns:
            return sorted(self.duid_mapping['Fuel'].dropna().unique().tolist())
        return []
    
    @lru_cache(maxsize=128)
    def get_generation_by_fuel(
        self,
        start_date: datetime,
        end_date: datetime,
        regions: Optional[Tuple[str]] = None,
        resolution: str = '30min'
    ) -> pd.DataFrame:
        """
        Get generation data aggregated by fuel type.
        
        This executes a SQL query on the parquet files directly.
        """
        with perf_logger.timer("duckdb_generation_query", threshold=0.5):
            # Build the base query
            if resolution == '5min' and (end_date - start_date).days < 7:
                table = 'generation_5min'
                # Need to join with DUID mapping for 5min data
                base_query = """
                    SELECT 
                        g.settlementdate,
                        COALESCE(d.Fuel, 'Unknown') as fuel_type,
                        SUM(g.scadavalue) as scadavalue
                    FROM generation_5min g
                    LEFT JOIN {self._duid_join_table} d ON g.duid = d.DUID
                """
            else:
                table = 'generation_enriched_30min'
                base_query = """
                    SELECT 
                        settlementdate,
                        COALESCE(fuel_type, 'Unknown') as fuel_type,
                        SUM(scadavalue) as scadavalue
                    FROM generation_enriched_30min
                """
            
            # Add WHERE clause
            where_conditions = [
                f"settlementdate >= '{start_date.isoformat()}'",
                f"settlementdate <= '{end_date.isoformat()}'"
            ]
            
            if regions:
                # For 5min data, need to join to get region
                region_list = ','.join([f"'{r}'" for r in regions])
                if table == 'generation_5min':
                    where_conditions.append(f"d.Region IN ({region_list})")
                else:
                    where_conditions.append(f"region IN ({region_list})")
            
            where_clause = " WHERE " + " AND ".join(where_conditions)
            
            # Add GROUP BY based on resolution
            if resolution == 'hourly':
                # Use date_trunc for hourly aggregation
                if table == 'generation_5min':
                    query = base_query.replace(
                        'g.settlementdate,',
                        "date_trunc('hour', g.settlementdate) as settlementdate,"
                    )
                else:
                    query = base_query.replace(
                        'settlementdate,',
                        "date_trunc('hour', settlementdate) as settlementdate,"
                    )
                group_by = "GROUP BY date_trunc('hour', settlementdate), fuel_type"
            elif resolution == 'daily':
                # Use date_trunc for daily aggregation
                if table == 'generation_5min':
                    query = base_query.replace(
                        'g.settlementdate,',
                        "date_trunc('day', g.settlementdate) as settlementdate,"
                    )
                else:
                    query = base_query.replace(
                        'settlementdate,',
                        "date_trunc('day', settlementdate) as settlementdate,"
                    )
                group_by = "GROUP BY date_trunc('day', settlementdate), fuel_type"
            else:
                query = base_query
                group_by = "GROUP BY settlementdate, fuel_type"
            
            # Complete query
            if resolution in ['hourly', 'daily']:
                # query already built above
                query = f"{query}{where_clause} {group_by} ORDER BY settlementdate, fuel_type"
            else:
                query = f"{base_query}{where_clause} {group_by} ORDER BY settlementdate, fuel_type"
            
            # Execute query
            result = self.conn.execute(query).df()
            
            logger.debug(f"Generation query returned {len(result)} rows")
            return result
    
    def get_regional_prices(
        self,
        start_date: datetime,
        end_date: datetime,
        regions: Optional[List[str]] = None,
        resolution: str = '30min'
    ) -> pd.DataFrame:
        """Get price data by region"""
        with perf_logger.timer("duckdb_price_query", threshold=0.5):
            # Select appropriate table
            table = 'prices_5min' if resolution == '5min' and (end_date - start_date).days < 7 else 'prices_30min'
            
            # Build query
            query = f"""
                SELECT 
                    settlementdate,
                    regionid,
                    rrp
                FROM {table}
                WHERE settlementdate >= '{start_date.isoformat()}'
                  AND settlementdate <= '{end_date.isoformat()}'
            """
            
            if regions:
                region_list = ','.join([f"'{r}'" for r in regions])
                query += f" AND regionid IN ({region_list})"
            
            query += " ORDER BY settlementdate, regionid"
            
            # Execute query
            result = self.conn.execute(query).df()
            
            logger.debug(f"Price query returned {len(result)} rows")
            return result
    
    def calculate_revenue(
        self,
        start_date: datetime,
        end_date: datetime,
        group_by: List[str],
        resolution: str = '30min'
    ) -> pd.DataFrame:
        """
        Calculate revenue analysis with custom grouping.

        Args:
            start_date: Start date for analysis
            end_date: End date for analysis
            group_by: List of fields to group by
            resolution: Data resolution ('5min' or '30min')

        Returns:
            DataFrame with revenue calculations
        """
        with perf_logger.timer("duckdb_revenue_query", threshold=0.5):
            # Select appropriate time factor and tables based on resolution
            if resolution == '5min':
                time_factor = MINUTES_5_TO_HOURS
                gen_table = 'generation_5min'
                price_table = 'prices_5min'
                join_duid = f'LEFT JOIN {self._duid_join_table} d ON g.duid = d.DUID'
                region_col = 'd.Region'
            else:
                time_factor = MINUTES_30_TO_HOURS
                gen_table = 'generation_enriched_30min'
                price_table = 'prices_30min'
                join_duid = ''
                region_col = 'g.region'

            # Map group_by fields to SQL columns based on resolution
            sql_group_by = []
            select_fields = []

            for field in group_by:
                if field == 'fuel_type':
                    if resolution == '5min':
                        sql_group_by.append('d.Fuel')
                        select_fields.append('d.Fuel as fuel_type')
                    else:
                        sql_group_by.append('g.fuel_type')
                        select_fields.append('g.fuel_type')
                elif field == 'region':
                    sql_group_by.append(region_col)
                    select_fields.append(f'{region_col} as region')
                elif field == 'station_name' or field == 'site_name':
                    if resolution == '5min':
                        sql_group_by.append('d."Site Name"')
                        select_fields.append('d."Site Name" as station_name')
                    else:
                        sql_group_by.append('g.station_name')
                        select_fields.append('g.station_name')
                elif field == 'duid':
                    sql_group_by.append('g.duid')
                    select_fields.append('g.duid')

            # Build the revenue query with correct time factor
            # Revenue = MW × $/MWh × hours
            query = f"""
                SELECT
                    {', '.join(select_fields)},
                    SUM(g.scadavalue) as scadavalue,
                    SUM(g.scadavalue * p.rrp * {time_factor}) as revenue,
                    AVG(p.rrp) as rrp
                FROM {gen_table} g
                {join_duid}
                JOIN {price_table} p
                  ON g.settlementdate = p.settlementdate
                  AND {region_col} = p.regionid
                WHERE g.settlementdate >= '{start_date.isoformat()}'
                  AND g.settlementdate <= '{end_date.isoformat()}'
                GROUP BY {', '.join(sql_group_by)}
                ORDER BY revenue DESC
            """

            # Execute query
            result = self.conn.execute(query).df()

            logger.debug(f"Revenue query ({resolution}) returned {len(result)} rows")
            return result
    
    def get_station_data(
        self,
        station_name: str,
        start_date: datetime,
        end_date: datetime,
        resolution: str = '30min'
    ) -> pd.DataFrame:
        """
        Get detailed data for a specific station.

        Args:
            station_name: Name of the station
            start_date: Start date
            end_date: End date
            resolution: Data resolution ('5min' or '30min')

        Returns:
            DataFrame with station data including revenue
        """
        # Select appropriate time factor and tables based on resolution
        if resolution == '5min':
            time_factor = MINUTES_5_TO_HOURS
            gen_table = 'generation_5min'
            price_table = 'prices_5min'
            # For 5min, need to join with duid_mapping
            query = f"""
                SELECT
                    g.settlementdate,
                    g.duid,
                    g.scadavalue,
                    d.Fuel as fuel_type,
                    d.Region as region,
                    p.rrp,
                    g.scadavalue * p.rrp * {time_factor} as revenue
                FROM {gen_table} g
                LEFT JOIN {self._duid_join_table} d ON g.duid = d.DUID
                JOIN {price_table} p
                  ON g.settlementdate = p.settlementdate
                  AND d.Region = p.regionid
                WHERE d."Site Name" = '{station_name}'
                  AND g.settlementdate >= '{start_date.isoformat()}'
                  AND g.settlementdate <= '{end_date.isoformat()}'
                ORDER BY g.settlementdate
            """
        else:
            time_factor = MINUTES_30_TO_HOURS
            gen_table = 'generation_enriched_30min'
            price_table = 'prices_30min'
            query = f"""
                SELECT
                    g.settlementdate,
                    g.duid,
                    g.scadavalue,
                    g.fuel_type,
                    g.region,
                    p.rrp,
                    g.scadavalue * p.rrp * {time_factor} as revenue
                FROM {gen_table} g
                JOIN {price_table} p
                  ON g.settlementdate = p.settlementdate
                  AND g.region = p.regionid
                WHERE g.station_name = '{station_name}'
                  AND g.settlementdate >= '{start_date.isoformat()}'
                  AND g.settlementdate <= '{end_date.isoformat()}'
                ORDER BY g.settlementdate
            """

        return self.conn.execute(query).df()
    
    def get_transmission_flows(
        self,
        start_date: datetime,
        end_date: datetime,
        interconnector_id: Optional[str] = None
    ) -> pd.DataFrame:
        """Get transmission flow data"""
        query = f"""
            SELECT *
            FROM transmission_30min
            WHERE settlementdate >= '{start_date.isoformat()}'
              AND settlementdate <= '{end_date.isoformat()}'
        """
        
        if interconnector_id:
            query += f" AND interconnectorid = '{interconnector_id}'"
        
        query += " ORDER BY settlementdate"
        
        return self.conn.execute(query).df()
    
    def close(self):
        """Close DuckDB connection"""
        if self._conn is not None and not isinstance(self._conn, _RetryConnection):
            self._conn.close()
        self._conn = None

    def refresh_views(self):
        """Force recreation of all views (useful after data updates)"""
        if self._external_db_path:
            logger.info("External DB mode — views are managed in the DuckDB file")
            return
        if self._conn is not None:
            logger.info("Refreshing all views...")
            self._register_data_views()
            self._load_duid_mapping()
            self._create_helper_views()
            logger.info("Views refreshed")


# Lazy singleton - instance created but connection deferred
duckdb_data_service = DuckDBDataService()