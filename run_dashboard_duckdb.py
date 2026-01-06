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
from pathlib import Path

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


def init_duckdb_with_retry(max_attempts: int = 3, delay: float = 3.0) -> bool:
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
            from data_service.shared_data_duckdb import duckdb_data_service

            # Verify it's working with a simple health check
            # Try to execute a simple query to confirm connection is valid
            if hasattr(duckdb_data_service, 'conn') and duckdb_data_service.conn is not None:
                # Try a simple query
                duckdb_data_service.conn.execute("SELECT 1").fetchone()
                logger.info("DuckDB service initialized successfully")
                return True
            else:
                raise Exception("DuckDB connection not established")

        except Exception as e:
            error_msg = str(e).lower()

            # Check for common file access errors
            is_file_error = any(err in error_msg for err in [
                'magic bytes', 'cannot open', 'invalid input',
                'file is locked', 'permission denied', 'no such file'
            ])

            if is_file_error:
                logger.warning(
                    f"File access error on attempt {attempt + 1}/{max_attempts}: {str(e)[:100]}"
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
    if not init_duckdb_with_retry(max_attempts=3, delay=3.0):
        logger.error("Failed to initialize DuckDB. Please check if data files are accessible.")
        logger.info("Tip: If the data collector is running, try stopping it temporarily.")
        sys.exit(1)

    # Now import and run the dashboard
    from aemo_dashboard.generation.gen_dash import main as dashboard_main
    dashboard_main()


if __name__ == "__main__":
    main()