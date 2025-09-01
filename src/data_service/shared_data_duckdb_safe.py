"""
Safe version of DuckDB data service with retry logic for file access
"""

import duckdb
import time
import logging
from pathlib import Path
from typing import Optional, Dict, Any
import threading
import os

from ..aemo_dashboard.shared.config import config
from ..aemo_dashboard.shared.logging_config import get_logger

logger = get_logger(__name__)

class SafeDuckDBDataService:
    """DuckDB data service with retry logic for concurrent file access"""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize DuckDB connection with retry logic"""
        if hasattr(self, '_initialized'):
            return
            
        self._initialized = True
        self.conn = None
        self._init_connection()
    
    def _init_connection(self):
        """Initialize DuckDB connection"""
        logger.info("Initializing SafeDuckDB connection...")
        
        # Create in-memory database
        self.conn = duckdb.connect(':memory:')
        
        # Configure DuckDB for better concurrency
        self.conn.execute("SET threads TO 1")  # Single thread to avoid conflicts
        self.conn.execute("SET enable_progress_bar TO false")
        
        # Register data views with retry logic
        self._register_data_views_with_retry()
        
        logger.info("SafeDuckDB connection initialized successfully")
    
    def _register_data_views_with_retry(self):
        """Register parquet files as views with retry logic"""
        logger.info("Registering data views with retry logic...")
        
        # Define all views to create
        views = [
            # Generation data
            {
                'name': 'generation_30min',
                'path': str(config.gen_output_file).replace('scada5.parquet', 'scada30.parquet'),
                'query': "SELECT * FROM read_parquet('{path}')"
            },
            {
                'name': 'generation_5min',
                'path': str(config.gen_output_file),
                'query': "SELECT * FROM read_parquet('{path}')"
            },
            # Price data
            {
                'name': 'prices_30min',
                'path': str(config.spot_hist_file).replace('prices5.parquet', 'prices30.parquet'),
                'query': """SELECT settlementdate, regionid, rrp 
                           FROM read_parquet('{path}')"""
            },
            {
                'name': 'prices_5min',
                'path': str(config.spot_hist_file),
                'query': """SELECT settlementdate, regionid, rrp 
                           FROM read_parquet('{path}')"""
            },
            # Transmission data
            {
                'name': 'transmission_30min',
                'path': str(config.transmission_output_file).replace('transmission5.parquet', 'transmission30.parquet'),
                'query': "SELECT * FROM read_parquet('{path}')"
            },
            # Rooftop solar
            {
                'name': 'rooftop_solar',
                'path': str(config.rooftop_solar_file),
                'query': "SELECT * FROM read_parquet('{path}')"
            }
        ]
        
        # DUID mapping (pickle file - no retry needed)
        try:
            self.conn.execute(f"""
                CREATE VIEW duid_mapping AS 
                SELECT * FROM '{config.gen_info_file}'
            """)
            logger.debug("Created duid_mapping view")
        except Exception as e:
            logger.warning(f"Could not create duid_mapping view: {e}")
        
        # Create each view with retry logic
        for view_def in views:
            self._create_view_with_retry(
                view_name=view_def['name'],
                view_query=view_def['query'].format(path=view_def['path']),
                file_path=view_def['path'],
                max_retries=3,
                retry_delay=2.0
            )
    
    def _create_view_with_retry(self, view_name: str, view_query: str, file_path: str, 
                               max_retries: int = 3, retry_delay: float = 2.0):
        """Create a view with retry logic for concurrent file access"""
        
        for attempt in range(max_retries):
            try:
                # Check if file exists
                if not os.path.exists(file_path):
                    logger.warning(f"File not found: {file_path}")
                    return
                
                # Try to create the view
                create_query = f"CREATE VIEW {view_name} AS {view_query}"
                self.conn.execute(create_query)
                
                # Verify the view works by reading one row
                test_query = f"SELECT * FROM {view_name} LIMIT 1"
                self.conn.execute(test_query).fetchone()
                
                logger.info(f"Successfully created view: {view_name}")
                return
                
            except Exception as e:
                error_msg = str(e)
                logger.warning(f"Attempt {attempt + 1}/{max_retries} failed for {view_name}: {error_msg}")
                
                # Check if it's a file access error
                if "magic bytes" in error_msg.lower() or "cannot open" in error_msg.lower():
                    if attempt < max_retries - 1:
                        logger.info(f"File may be locked, waiting {retry_delay}s before retry...")
                        time.sleep(retry_delay)
                        
                        # On second attempt, try with a copy
                        if attempt == 1:
                            try:
                                temp_path = f"{file_path}.tmp"
                                import shutil
                                shutil.copy2(file_path, temp_path)
                                view_query = view_query.replace(file_path, temp_path)
                                logger.info(f"Attempting with temporary copy: {temp_path}")
                            except:
                                pass
                    else:
                        # Final attempt - create empty view
                        logger.error(f"Failed to create view {view_name} after {max_retries} attempts")
                        self._create_empty_view(view_name, file_path)
                else:
                    # Non-recoverable error
                    logger.error(f"Non-recoverable error creating view {view_name}: {error_msg}")
                    self._create_empty_view(view_name, file_path)
                    break
    
    def _create_empty_view(self, view_name: str, file_path: str):
        """Create an empty view as fallback"""
        logger.warning(f"Creating empty view for {view_name} as fallback")
        
        # Define schema based on view name
        schemas = {
            'generation_30min': "settlementdate TIMESTAMP, duid VARCHAR, scadavalue DOUBLE",
            'generation_5min': "settlementdate TIMESTAMP, duid VARCHAR, scadavalue DOUBLE",
            'prices_30min': "settlementdate TIMESTAMP, regionid VARCHAR, rrp DOUBLE",
            'prices_5min': "settlementdate TIMESTAMP, regionid VARCHAR, rrp DOUBLE",
            'transmission_30min': "settlementdate TIMESTAMP, interconnectorid VARCHAR, meteredmwflow DOUBLE",
            'rooftop_solar': "settlementdate TIMESTAMP, regionid VARCHAR, power DOUBLE"
        }
        
        schema = schemas.get(view_name, "dummy INTEGER")
        
        try:
            # Create empty table with correct schema
            self.conn.execute(f"""
                CREATE VIEW {view_name} AS 
                SELECT * FROM (
                    SELECT {schema} WHERE 1=0
                ) empty_table
            """)
            logger.info(f"Created empty fallback view for {view_name}")
        except Exception as e:
            logger.error(f"Failed to create empty view: {e}")
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a query with retry logic"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                if params:
                    result = self.conn.execute(query, params)
                else:
                    result = self.conn.execute(query)
                return result
                
            except Exception as e:
                logger.warning(f"Query attempt {attempt + 1}/{max_retries} failed: {str(e)[:100]}")
                
                if attempt < max_retries - 1:
                    time.sleep(1)
                else:
                    logger.error(f"Query failed after {max_retries} attempts")
                    raise
    
    def __del__(self):
        """Clean up temporary files on exit"""
        try:
            # Clean up any .tmp files we created
            import glob
            import os
            for tmp_file in glob.glob(str(config.data_dir / "*.tmp")):
                try:
                    os.remove(tmp_file)
                except:
                    pass
        except:
            pass

# Create singleton instance
safe_duckdb_service = SafeDuckDBDataService()