"""
AEMO Dashboard Data Service

Provides shared data access for all dashboard components.
Conditionally loads the appropriate service based on USE_DUCKDB environment variable.
"""

import os

# Check if we should use DuckDB
# Default to DuckDB mode for instant startup and low memory usage
USE_DUCKDB = os.getenv('USE_DUCKDB', 'true').lower() == 'true'

if USE_DUCKDB:
    # Import DuckDB service without loading SharedDataService
    from .shared_data_duckdb import duckdb_data_service as data_service
    # Create a compatibility wrapper for SharedDataService
    SharedDataService = lambda: duckdb_data_service
    # API router not needed for DuckDB mode
    api_router = None
else:
    # Import original services
    from .shared_data import SharedDataService, data_service
    from .api_endpoints import router as api_router

__all__ = ['SharedDataService', 'api_router', 'data_service']