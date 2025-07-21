"""
Lazy-loading DuckDB views for fast startup
Only creates views when first accessed
"""
import os
import duckdb
import pandas as pd
from pathlib import Path
from ..shared.logging_config import get_logger

logger = get_logger(__name__)

class LazyDuckDBViews:
    """DuckDB views with lazy initialization"""
    
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn
        self._views_created = set()
        self._all_views_created = False
        self._duid_mapping_loaded = False
        
        # Get data paths from environment
        data_dir = os.getenv('DATA_DIRECTORY', str(Path.home() / "Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2"))
        gen_info_dir = str(Path.home() / "Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard/data")
        
        self.file_paths = {
            'scada5': os.path.join(data_dir, 'scada5.parquet'),
            'scada30': os.path.join(data_dir, 'scada30.parquet'),
            'prices5': os.path.join(data_dir, 'prices5.parquet'),
            'prices30': os.path.join(data_dir, 'prices30.parquet'),
            'transmission5': os.path.join(data_dir, 'transmission5.parquet'),
            'transmission30': os.path.join(data_dir, 'transmission30.parquet'),
            'rooftop30': os.path.join(data_dir, 'rooftop30.parquet'),
            'gen_info': os.path.join(gen_info_dir, 'gen_info.pkl')
        }
    
    def ensure_view(self, view_name: str):
        """Create a specific view if it doesn't exist"""
        if view_name in self._views_created:
            return
            
        if view_name == 'generation_by_fuel_30min':
            self._create_generation_by_fuel_30min()
        elif view_name == 'generation_by_fuel_5min':
            self._create_generation_by_fuel_5min()
        elif view_name == 'prices_5min':
            self._create_prices_5min()
        elif view_name == 'prices_30min':
            self._create_prices_30min()
        elif view_name == 'transmission_flows_5min':
            self._create_transmission_flows_5min()
        elif view_name == 'transmission_flows_30min':
            self._create_transmission_flows_30min()
        elif view_name == 'rooftop_solar_30min':
            self._create_rooftop_solar_30min()
        elif view_name == 'capacity_utilization_30min':
            self._create_capacity_utilization_30min()
        elif view_name == 'generation_with_prices_30min':
            self._create_generation_with_prices_30min()
        elif view_name == 'daily_generation_by_fuel':
            self._create_daily_generation_by_fuel()
        else:
            logger.warning(f"Unknown view requested: {view_name}")
            return
            
        self._views_created.add(view_name)
        logger.debug(f"Created view: {view_name}")
    
    def _ensure_duid_mapping(self):
        """Load DUID mapping table if not already loaded"""
        if self._duid_mapping_loaded:
            return
            
        try:
            # Load gen_info.pkl into a pandas DataFrame
            gen_info_df = pd.read_pickle(self.file_paths['gen_info'])
            
            # Create DuckDB table from DataFrame
            self.conn.execute("DROP TABLE IF EXISTS duid_mapping")
            self.conn.execute("""
                CREATE TABLE duid_mapping AS 
                SELECT * FROM gen_info_df
            """)
            
            self._duid_mapping_loaded = True
            logger.info("Loaded DUID mapping table")
            
        except Exception as e:
            logger.error(f"Failed to load DUID mapping: {e}")
            # Create empty table as fallback
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS duid_mapping (
                    duid VARCHAR,
                    fuel_source_descriptor VARCHAR,
                    technology_type_descriptor VARCHAR,
                    region VARCHAR,
                    station_name VARCHAR
                )
            """)
    
    def _create_generation_by_fuel_30min(self):
        """Create 30-minute generation by fuel view"""
        # Ensure DUID mapping is loaded first
        self._ensure_duid_mapping()
        
        query = f"""
        CREATE OR REPLACE VIEW generation_by_fuel_30min AS
        WITH gen_with_fuel AS (
            SELECT 
                g.settlementdate,
                g.duid,
                g.scadavalue,
                d.Fuel as fuel_type,
                d.Fuel as technology_type_descriptor,
                d.Region as region,
                d."Site Name" as station_name
            FROM read_parquet('{self.file_paths['scada30']}') g
            LEFT JOIN duid_mapping d
            ON UPPER(g.duid) = UPPER(d.DUID)
        )
        SELECT 
            settlementdate,
            region,
            fuel_type,
            technology_type_descriptor,
            SUM(scadavalue) as total_generation,
            COUNT(DISTINCT duid) as unit_count,
            COUNT(*) as record_count
        FROM gen_with_fuel
        WHERE fuel_type IS NOT NULL
        GROUP BY settlementdate, region, fuel_type, technology_type_descriptor
        """
        self.conn.execute(query)
    
    def _create_generation_by_fuel_5min(self):
        """Create 5-minute generation by fuel view"""
        # Ensure DUID mapping is loaded first
        self._ensure_duid_mapping()
        
        query = f"""
        CREATE OR REPLACE VIEW generation_by_fuel_5min AS
        WITH gen_with_fuel AS (
            SELECT 
                g.settlementdate,
                g.duid,
                g.scadavalue,
                d.Fuel as fuel_type,
                d.Fuel as technology_type_descriptor,
                d.Region as region,
                d."Site Name" as station_name
            FROM read_parquet('{self.file_paths['scada5']}') g
            LEFT JOIN duid_mapping d
            ON UPPER(g.duid) = UPPER(d.DUID)
        )
        SELECT 
            settlementdate,
            region,
            fuel_type,
            technology_type_descriptor,
            SUM(scadavalue) as total_generation,
            COUNT(DISTINCT duid) as unit_count
        FROM gen_with_fuel
        WHERE fuel_type IS NOT NULL
        GROUP BY settlementdate, region, fuel_type, technology_type_descriptor
        """
        self.conn.execute(query)
    
    def _create_prices_5min(self):
        """Create 5-minute prices view"""
        query = f"""
        CREATE OR REPLACE VIEW prices_5min AS
        SELECT * FROM read_parquet('{self.file_paths['prices5']}')
        """
        self.conn.execute(query)
    
    def _create_prices_30min(self):
        """Create 30-minute prices view"""
        query = f"""
        CREATE OR REPLACE VIEW prices_30min AS
        SELECT * FROM read_parquet('{self.file_paths['prices30']}')
        """
        self.conn.execute(query)
    
    def _create_transmission_flows_5min(self):
        """Create 5-minute transmission flows view"""
        query = f"""
        CREATE OR REPLACE VIEW transmission_flows_5min AS
        SELECT * FROM read_parquet('{self.file_paths['transmission5']}')
        """
        self.conn.execute(query)
    
    def _create_transmission_flows_30min(self):
        """Create 30-minute transmission flows view"""
        query = f"""
        CREATE OR REPLACE VIEW transmission_flows_30min AS
        SELECT * FROM read_parquet('{self.file_paths['transmission30']}')
        """
        self.conn.execute(query)
    
    def _create_rooftop_solar_30min(self):
        """Create rooftop solar view"""
        query = f"""
        CREATE OR REPLACE VIEW rooftop_solar_30min AS
        SELECT * FROM read_parquet('{self.file_paths['rooftop30']}')
        """
        self.conn.execute(query)
    
    def _create_capacity_utilization_30min(self):
        """Create capacity utilization view"""
        # First ensure the generation view exists
        self.ensure_view('generation_by_fuel_30min')
        
        query = """
        CREATE OR REPLACE VIEW capacity_utilization_30min AS
        SELECT 
            settlementdate,
            region,
            fuel_type,
            technology_type_descriptor,
            total_generation,
            unit_count,
            -- Add capacity calculation here if available
            total_generation as capacity_utilization
        FROM generation_by_fuel_30min
        """
        self.conn.execute(query)
    
    def _create_generation_with_prices_30min(self):
        """Create combined generation and prices view"""
        # Ensure dependencies exist
        self.ensure_view('generation_by_fuel_30min')
        self.ensure_view('prices_30min')
        
        query = """
        CREATE OR REPLACE VIEW generation_with_prices_30min AS
        SELECT 
            g.*,
            p.rrp as price
        FROM generation_by_fuel_30min g
        LEFT JOIN prices_30min p
        ON g.settlementdate = p.settlementdate
        AND g.region = p.regionid
        """
        self.conn.execute(query)
    
    def _create_daily_generation_by_fuel(self):
        """Create daily aggregated generation view"""
        # Ensure dependency exists
        self.ensure_view('generation_by_fuel_30min')
        
        query = """
        CREATE OR REPLACE VIEW daily_generation_by_fuel AS
        SELECT 
            DATE_TRUNC('day', settlementdate) as date,
            region,
            fuel_type,
            technology_type_descriptor,
            SUM(total_generation) * 0.5 as daily_generation_mwh,
            AVG(total_generation) as avg_generation_mw,
            MAX(total_generation) as max_generation_mw,
            MIN(total_generation) as min_generation_mw,
            COUNT(*) as interval_count
        FROM generation_by_fuel_30min
        GROUP BY DATE_TRUNC('day', settlementdate), region, fuel_type, technology_type_descriptor
        """
        self.conn.execute(query)


def create_lazy_views(conn: duckdb.DuckDBPyConnection) -> LazyDuckDBViews:
    """Create lazy-loading views manager"""
    return LazyDuckDBViews(conn)