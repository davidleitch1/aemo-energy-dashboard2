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

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('dashboard_startup')


class TimeoutError(Exception):
    """Raised when an operation times out"""
    pass


@contextmanager
def timeout(seconds: int, error_message: str = "Operation timed out"):
    """Context manager that raises TimeoutError after specified seconds (Unix only)"""
    def timeout_handler(signum, frame):
        raise TimeoutError(error_message)
    
    # Set the signal handler
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
    Uses LIMIT 1 to minimize data transfer but still test file access.
    
    Args:
        conn: DuckDB connection
        timeout_seconds: Maximum time to wait for each query
        
    Returns:
        True if all warmup queries succeeded, False otherwise
    """
    warmup_queries = [
        ("generation_30min", "SELECT settlementdate FROM generation_30min LIMIT 1"),
        ("prices_30min", "SELECT settlementdate FROM prices_30min LIMIT 1"),
    ]
    
    for view_name, query in warmup_queries:
        try:
            logger.info(f"  Warming up {view_name}...")
            with timeout(timeout_seconds, f"Timeout reading {view_name}"):
                result = conn.execute(query).fetchone()
                if result:
                    logger.info(f"  {view_name} OK")
                else:
                    logger.warning(f"  {view_name} returned no data (may be empty)")
        except TimeoutError as e:
            logger.error(f"  {view_name} TIMEOUT: {e}")
            return False
        except Exception as e:
            logger.error(f"  {view_name} ERROR: {e}")
            return False
    
    return True


def init_duckdb_with_retry(max_attempts: int = 3, delay: float = 5.0) -> bool:
    """
    Initialize DuckDB service with retry logic.

    This handles the case where the data collector is writing to parquet
    files at the same moment the dashboard tries to read them, which can
    cause 'magic bytes' errors or hangs.

    Args:
        max_attempts: Maximum number of initialization attempts
        delay: Seconds to wait between attempts

    Returns:
        True if initialization succeeded, False otherwise
    """
    for attempt in range(max_attempts):
        try:
            logger.info(f"Initializing DuckDB service (attempt {attempt + 1}/{max_attempts})...")

            # Import the DuckDB service - this triggers initialization
            # Force re-initialization by clearing any cached instance
            import importlib
            if 'data_service.shared_data_duckdb' in sys.modules:
                # Reset the singleton to force re-initialization
                module = sys.modules['data_service.shared_data_duckdb']
                if hasattr(module, 'DuckDBDataService'):
                    module.DuckDBDataService._instance = None
                    module.DuckDBDataService._instance = None
                importlib.reload(module)
            
            from data_service.shared_data_duckdb import duckdb_data_service

            # Verify connection exists
            if not hasattr(duckdb_data_service, 'conn') or duckdb_data_service.conn is None:
                raise Exception("DuckDB connection not established")
            
            # Basic connection test
            duckdb_data_service.conn.execute("SELECT 1").fetchone()
            logger.info("DuckDB connection OK")
            
            # CRITICAL: Warmup queries that actually read parquet files
            # This catches file contention before dashboard initialization
            logger.info("Running parquet warmup queries...")
            if not warmup_parquet_access(duckdb_data_service.conn, timeout_seconds=30):
                raise Exception("Parquet warmup queries failed")
            
            logger.info("DuckDB service initialized and warmed up successfully")
            return True

        except Exception as e:
            error_msg = str(e).lower()

            # Check for common file access errors
            is_file_error = any(err in error_msg for err in [
                'magic bytes', 'cannot open', 'invalid input',
                'file is locked', 'permission denied', 'no such file',
                'timeout', 'warmup'
            ])

            if is_file_error:
                logger.warning(
                    f"File access error on attempt {attempt + 1}/{max_attempts}: {str(e)[:200]}"
                )
            else:
                logger.error(f"Initialization error: {e}")

            if attempt < max_attempts - 1:
                logger.info(f"Waiting {delay}s before retry...")
                time.sleep(delay)
            else:
                logger.error(f"Failed to initialize after {max_attempts} attempts")
                return False

    return False


def main():
    """Main entry point with retry-enabled startup"""
    print("Starting AEMO Dashboard with DuckDB adapters...")
    print("USE_DUCKDB environment variable:", os.getenv('USE_DUCKDB'))
    print("\nDashboard will be available at http://localhost:5006")
    print("Press Ctrl+C to stop\n")

    # Initialize DuckDB with retry logic
    if not init_duckdb_with_retry(max_attempts=3, delay=5.0):
        logger.error("Failed to initialize DuckDB. Please check if data files are accessible.")
        logger.info("Tip: If the data collector is running, try stopping it temporarily.")
        sys.exit(1)

    # Now import and run the dashboard
    from aemo_dashboard.generation.gen_dash import main as dashboard_main
    dashboard_main()


if __name__ == "__main__":
    main()
