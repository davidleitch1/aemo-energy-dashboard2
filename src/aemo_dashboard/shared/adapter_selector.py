"""
Adapter Selector - Configures which data adapters to use

This module provides a central place to switch between pandas-based
and DuckDB-based adapters. Change USE_DUCKDB to True to use the
memory-efficient DuckDB adapters.
"""

import os

# Configuration: Set to True to use DuckDB adapters
USE_DUCKDB = os.getenv('USE_DUCKDB', 'true').lower() == 'true'

if USE_DUCKDB:
    # Import DuckDB adapters
    from .generation_adapter_duckdb import (
        load_generation_data,
        get_generation_summary,
        get_available_duids,
        load_gen_data  # Legacy compatibility
    )
    
    from .price_adapter_duckdb import (
        load_price_data,
        get_price_summary,
        get_available_regions,
        get_price_statistics,
        load_spot_data  # Legacy compatibility
    )
    
    # Import DuckDB transmission and rooftop adapters
    from .transmission_adapter_duckdb import (
        load_transmission_data,
        get_transmission_summary,
        get_available_interconnectors,
        get_flow_statistics
    )
    
    from .rooftop_adapter_duckdb import (
        load_rooftop_data,
        get_rooftop_at_time,
        get_rooftop_summary,
        smooth_rooftop_data
    )
    
    # Import interpolation functions from original adapter
    from .rooftop_adapter import (
        interpolate_and_smooth,
        henderson_smooth
    )
    
    adapter_type = "DuckDB"
    
else:
    # Import original pandas-based adapters
    from .generation_adapter import (
        load_generation_data,
        get_generation_summary,
        get_available_duids,
        load_gen_data  # Legacy compatibility
    )
    
    from .price_adapter import (
        load_price_data,
        get_price_summary,
        get_available_regions,
        get_price_statistics,
        load_spot_data  # Legacy compatibility
    )
    
    from .transmission_adapter import load_transmission_data
    from .rooftop_adapter import (
        load_rooftop_data,
        interpolate_and_smooth,
        get_rooftop_at_time
    )
    
    adapter_type = "Pandas"

# Log which adapters are being used
from .logging_config import get_logger
logger = get_logger(__name__)
logger.info(f"Using {adapter_type} data adapters (USE_DUCKDB={USE_DUCKDB})")

# Export all functions
__all__ = [
    'load_generation_data',
    'get_generation_summary',
    'get_available_duids',
    'load_gen_data',
    'load_price_data',
    'get_price_summary',
    'get_available_regions',
    'get_price_statistics',
    'load_spot_data',
    'load_transmission_data',
    'get_transmission_summary',
    'get_available_interconnectors',
    'get_flow_statistics',
    'load_rooftop_data',
    'interpolate_and_smooth',
    'henderson_smooth',
    'get_rooftop_at_time',
    'get_rooftop_summary',
    'smooth_rooftop_data',
    'adapter_type',
    'USE_DUCKDB'
]