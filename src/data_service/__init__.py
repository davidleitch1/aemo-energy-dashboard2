"""
AEMO Dashboard Data Service

Provides shared data access for all dashboard components.
Conditionally loads the appropriate service based on USE_DUCKDB environment variable.
"""

import os

# Check if we should use DuckDB
# Default to DuckDB mode for instant startup and low memory usage
USE_DUCKDB = os.getenv('USE_DUCKDB', 'true').lower() == 'true'

# DuckDB mode is the only supported mode (parquet services moved to legacy/)
from .shared_data_duckdb import duckdb_data_service as data_service
SharedDataService = lambda: duckdb_data_service
api_router = None

__all__ = ['SharedDataService', 'api_router', 'data_service']