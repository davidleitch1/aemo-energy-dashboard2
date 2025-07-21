"""
DuckDB-based Shared Data Service - Zero memory footprint approach

This service uses DuckDB to query parquet files directly without loading
them into memory. Only query results are kept in memory.
"""

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

logger = get_logger(__name__)
perf_logger = PerformanceLogger(__name__)


class DuckDBDataService:
    """
    DuckDB-based data service that queries parquet files directly.
    
    Advantages:
    - Near-zero memory footprint (only query results in memory)
    - Fast parallel query execution
    - Handles 5+ years of data efficiently
    - No data duplication
    """
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize the DuckDB data service"""
        if self._initialized:
            return
            
        logger.info("Initializing DuckDB Data Service...")
        
        with perf_logger.timer("duckdb_init", threshold=1.0):
            # Create in-memory DuckDB connection
            self.conn = duckdb.connect(':memory:')
            
            # Configure DuckDB for better performance
            self.conn.execute("SET memory_limit='2GB'")
            self.conn.execute("SET threads=4")
            
            # Register parquet files as views
            self._register_data_views()
            
            # Load DUID mapping (this is small, keep in memory)
            self._load_duid_mapping()
            
            # Create helper views for common joins
            self._create_helper_views()
        
        self._initialized = True
        logger.info("DuckDB Data Service initialized")
    
    def _register_data_views(self):
        """Register parquet files as DuckDB views"""
        logger.info("Registering parquet files as views...")
        
        # Generation data
        gen_30_path = str(config.gen_output_file).replace('scada5.parquet', 'scada30.parquet')
        gen_5_path = str(config.gen_output_file)
        
        self.conn.execute(f"""
            CREATE VIEW generation_30min AS 
            SELECT * FROM read_parquet('{gen_30_path}')
        """)
        
        self.conn.execute(f"""
            CREATE VIEW generation_5min AS 
            SELECT * FROM read_parquet('{gen_5_path}')
        """)
        
        # Price data
        price_30_path = str(config.spot_hist_file).replace('prices5.parquet', 'prices30.parquet')
        price_5_path = str(config.spot_hist_file)
        
        self.conn.execute(f"""
            CREATE VIEW prices_30min AS 
            SELECT 
                settlementdate,
                regionid,
                rrp
            FROM read_parquet('{price_30_path}')
        """)
        
        self.conn.execute(f"""
            CREATE VIEW prices_5min AS 
            SELECT 
                settlementdate,
                regionid,
                rrp
            FROM read_parquet('{price_5_path}')
        """)
        
        # Transmission data
        trans_30_path = str(config.transmission_output_file).replace('transmission5.parquet', 'transmission30.parquet')
        
        self.conn.execute(f"""
            CREATE VIEW transmission_30min AS 
            SELECT * FROM read_parquet('{trans_30_path}')
        """)
        
        # Rooftop solar
        self.conn.execute(f"""
            CREATE VIEW rooftop_solar AS 
            SELECT * FROM read_parquet('{config.rooftop_solar_file}')
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
            self.conn.register('duid_info', duid_df)
            
            # Create a permanent table for better performance
            self.conn.execute("""
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
        self.conn.execute("""
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
                    LEFT JOIN duid_mapping d ON g.duid = d.DUID
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
        group_by: List[str]
    ) -> pd.DataFrame:
        """Calculate revenue analysis with custom grouping"""
        with perf_logger.timer("duckdb_revenue_query", threshold=0.5):
            # Map group_by fields to SQL columns
            sql_group_by = []
            select_fields = []
            
            for field in group_by:
                if field == 'fuel_type':
                    sql_group_by.append('fuel_type')
                    select_fields.append('fuel_type')
                elif field == 'region':
                    sql_group_by.append('g.region')
                    select_fields.append('g.region')
                elif field == 'station_name' or field == 'site_name':
                    sql_group_by.append('station_name')
                    select_fields.append('station_name')
                elif field == 'duid':
                    sql_group_by.append('g.duid')
                    select_fields.append('g.duid')
            
            # Build the revenue query
            query = f"""
                SELECT 
                    {', '.join(select_fields)},
                    SUM(g.scadavalue) as scadavalue,
                    SUM(g.scadavalue * p.rrp / 2) as revenue,
                    AVG(p.rrp) as rrp
                FROM generation_enriched_30min g
                JOIN prices_30min p 
                  ON g.settlementdate = p.settlementdate 
                  AND g.region = p.regionid
                WHERE g.settlementdate >= '{start_date.isoformat()}'
                  AND g.settlementdate <= '{end_date.isoformat()}'
                GROUP BY {', '.join(sql_group_by)}
                ORDER BY revenue DESC
            """
            
            # Execute query
            result = self.conn.execute(query).df()
            
            logger.debug(f"Revenue query returned {len(result)} rows")
            return result
    
    def get_station_data(
        self,
        station_name: str,
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """Get detailed data for a specific station"""
        query = f"""
            SELECT 
                g.settlementdate,
                g.duid,
                g.scadavalue,
                g.fuel_type,
                g.region,
                p.rrp,
                g.scadavalue * p.rrp / 2 as revenue
            FROM generation_enriched_30min g
            JOIN prices_30min p 
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
        if hasattr(self, 'conn'):
            self.conn.close()


# Create singleton instance
duckdb_data_service = DuckDBDataService()