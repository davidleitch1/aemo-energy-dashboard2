#!/usr/bin/env python3
"""
Run the AEMO dashboard with DuckDB adapters

Includes retry logic to handle startup hangs caused by concurrent
parquet file access from the data collector.
"""

import os
import sys
import time
import logging
import signal
from pathlib import Path
from contextlib import contextmanager

# Set environment variable BEFORE any imports
os.environ['USE_DUCKDB'] = 'true'

# Immediate fallback logging (before any imports that might log)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Initialize unified logging (replaces the basicConfig above)
from aemo_dashboard.shared.logging_config import setup_logging
logger = setup_logging()


class StartupTimeout(Exception):
    """Raised when a startup operation times out"""
    pass


@contextmanager
def timeout(seconds: int, error_message: str = "Operation timed out"):
    """Context manager that raises StartupTimeout after specified seconds"""
    def timeout_handler(signum, frame):
        raise StartupTimeout(error_message)
    
    old_handler = signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(seconds)
    
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


def warmup_parquet_access(conn, timeout_seconds: int = 30) -> bool:
    """
    Execute warmup queries that actually read from parquet files.
    
    This catches file contention issues before the dashboard tries to initialize.
    """
    warmup_queries = [
        ("generation_30min", "SELECT settlementdate FROM generation_30min LIMIT 1"),
        ("prices_30min", "SELECT settlementdate FROM prices_30min LIMIT 1"),
    ]
    
    for view_name, query in warmup_queries:
        try:
            logger.debug(f"Warming up {view_name}...")
            with timeout(timeout_seconds, f"Timeout reading {view_name}"):
                result = conn.execute(query).fetchone()
                if result:
                    logger.debug(f"{view_name} OK")
                else:
                    logger.warning(f"{view_name} returned no data")
        except StartupTimeout as e:
            logger.error(f"{view_name} TIMEOUT: {e}")
            return False
        except Exception as e:
            logger.error(f"{view_name} ERROR: {e}")
            return False
    
    return True


def init_duckdb_with_retry(max_attempts: int = 3, delay: float = 5.0) -> bool:
    """
    Initialize DuckDB service with retry logic.

    Handles concurrent parquet file access from the data collector.
    """
    for attempt in range(max_attempts):
        try:
            logger.info(f"Initializing DuckDB (attempt {attempt + 1}/{max_attempts})...")

            # Force re-initialization by clearing cached instance
            import importlib
            if 'data_service.shared_data_duckdb' in sys.modules:
                module = sys.modules['data_service.shared_data_duckdb']
                if hasattr(module, 'DuckDBDataService'):
                    module.DuckDBDataService._instance = None
                importlib.reload(module)
            
            from data_service.shared_data_duckdb import duckdb_data_service

            if not hasattr(duckdb_data_service, 'conn') or duckdb_data_service.conn is None:
                raise Exception("DuckDB connection not established")
            
            # Basic connection test
            duckdb_data_service.conn.execute("SELECT 1").fetchone()
            
            # Warmup queries that actually read parquet files
            if not warmup_parquet_access(duckdb_data_service.conn, timeout_seconds=30):
                raise Exception("Parquet warmup failed")
            
            logger.info("DuckDB initialized successfully")
            return True

        except Exception as e:
            error_msg = str(e).lower()
            is_file_error = any(err in error_msg for err in [
                'magic bytes', 'cannot open', 'invalid input',
                'file is locked', 'permission denied', 'timeout', 'warmup'
            ])

            if is_file_error:
                logger.warning(f"File access error (attempt {attempt + 1}): {str(e)[:100]}")
            else:
                logger.error(f"Initialization error: {e}")

            if attempt < max_attempts - 1:
                logger.info(f"Retrying in {delay}s...")
                time.sleep(delay)
            else:
                logger.error(f"Failed after {max_attempts} attempts")
                return False

    return False


def main():
    """Main entry point"""
    logger.info("Starting AEMO Dashboard with DuckDB adapters")
    
    if not init_duckdb_with_retry(max_attempts=3, delay=5.0):
        logger.critical("Failed to initialize DuckDB - exiting")
        sys.exit(1)

    # Import and run the dashboard
    from aemo_dashboard.generation.gen_dash import main as dashboard_main
    dashboard_main()


if __name__ == "__main__":
    main()
