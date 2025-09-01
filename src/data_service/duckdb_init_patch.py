"""
Patch for DuckDB initialization to add retry logic for concurrent file access
"""

import time
import logging
import os
from functools import wraps

logger = logging.getLogger(__name__)

def retry_on_file_error(max_retries=3, delay=2.0):
    """Decorator to retry operations that might fail due to file access"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    error_msg = str(e).lower()
                    
                    # Check if it's a file access error
                    if any(err in error_msg for err in ['magic bytes', 'cannot open', 'invalid input']):
                        if attempt < max_retries - 1:
                            logger.warning(f"File access error on attempt {attempt + 1}/{max_retries}: {str(e)[:100]}")
                            logger.info(f"Waiting {delay}s before retry...")
                            time.sleep(delay)
                        else:
                            logger.error(f"File access failed after {max_retries} attempts")
                    else:
                        # Non-recoverable error, don't retry
                        raise
            
            # If we get here, all retries failed
            raise last_error
        
        return wrapper
    return decorator

def create_view_with_fallback(conn, view_name, view_query, fallback_schema=None):
    """Create a view with fallback to empty view if file access fails"""
    try:
        # Try to create the view with retry
        @retry_on_file_error(max_retries=3, delay=2.0)
        def _create_view():
            conn.execute(f"CREATE VIEW {view_name} AS {view_query}")
            # Test the view
            conn.execute(f"SELECT * FROM {view_name} LIMIT 1").fetchone()
        
        _create_view()
        logger.info(f"Successfully created view: {view_name}")
        
    except Exception as e:
        logger.error(f"Failed to create view {view_name}: {e}")
        
        # Create empty fallback view
        if fallback_schema:
            try:
                conn.execute(f"""
                    CREATE VIEW {view_name} AS 
                    SELECT * FROM (
                        SELECT {fallback_schema} WHERE 1=0
                    ) empty_table
                """)
                logger.warning(f"Created empty fallback view for {view_name}")
            except Exception as e2:
                logger.error(f"Failed to create fallback view: {e2}")

def patch_duckdb_initialization():
    """Monkey patch the DuckDB initialization to add retry logic"""
    import data_service.shared_data_duckdb as duckdb_module
    
    # Store original method
    original_register = duckdb_module.DuckDBDataService._register_data_views
    
    def patched_register_data_views(self):
        """Patched version with retry logic"""
        logger.info("Registering data views with retry logic...")
        
        # Define fallback schemas
        schemas = {
            'generation_30min': "settlementdate TIMESTAMP, duid VARCHAR, scadavalue DOUBLE",
            'generation_5min': "settlementdate TIMESTAMP, duid VARCHAR, scadavalue DOUBLE", 
            'prices_30min': "settlementdate TIMESTAMP, regionid VARCHAR, rrp DOUBLE",
            'prices_5min': "settlementdate TIMESTAMP, regionid VARCHAR, rrp DOUBLE",
            'transmission_30min': "settlementdate TIMESTAMP, interconnectorid VARCHAR, meteredmwflow DOUBLE",
            'rooftop_solar': "settlementdate TIMESTAMP, regionid VARCHAR, power DOUBLE, quality_indicator VARCHAR, type VARCHAR, source_archive VARCHAR"
        }
        
        # Generation data views
        gen_30_path = str(self.config.gen_output_file).replace('scada5.parquet', 'scada30.parquet')
        create_view_with_fallback(
            self.conn,
            'generation_30min',
            f"SELECT * FROM read_parquet('{gen_30_path}')",
            schemas['generation_30min']
        )
        
        gen_5_path = str(self.config.gen_output_file)
        create_view_with_fallback(
            self.conn,
            'generation_5min', 
            f"SELECT * FROM read_parquet('{gen_5_path}')",
            schemas['generation_5min']
        )
        
        # Price data views
        price_30_path = str(self.config.spot_hist_file).replace('prices5.parquet', 'prices30.parquet')
        create_view_with_fallback(
            self.conn,
            'prices_30min',
            f"SELECT settlementdate, regionid, rrp FROM read_parquet('{price_30_path}')",
            schemas['prices_30min']
        )
        
        price_5_path = str(self.config.spot_hist_file)
        create_view_with_fallback(
            self.conn,
            'prices_5min',
            f"SELECT settlementdate, regionid, rrp FROM read_parquet('{price_5_path}')",
            schemas['prices_5min']
        )
        
        # Transmission data
        trans_30_path = str(self.config.transmission_output_file).replace('transmission5.parquet', 'transmission30.parquet')
        create_view_with_fallback(
            self.conn,
            'transmission_30min',
            f"SELECT * FROM read_parquet('{trans_30_path}')",
            schemas['transmission_30min']
        )
        
        # Rooftop solar
        create_view_with_fallback(
            self.conn,
            'rooftop_solar',
            f"SELECT * FROM read_parquet('{self.config.rooftop_solar_file}')",
            schemas['rooftop_solar']
        )
        
        # DUID mapping (no retry needed for pickle)
        try:
            self.conn.execute(f"""
                CREATE VIEW duid_mapping AS 
                SELECT * FROM '{self.config.gen_info_file}'
            """)
        except Exception as e:
            logger.warning(f"Could not create duid_mapping view: {e}")
        
        logger.info("Data views registered with retry logic")
    
    # Apply the patch
    duckdb_module.DuckDBDataService._register_data_views = patched_register_data_views
    logger.info("DuckDB initialization patched with retry logic")