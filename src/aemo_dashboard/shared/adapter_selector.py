"""
Adapter Selector - DuckDB data adapters for the AEMO dashboard.

All data access uses DuckDB for efficient memory usage.
Legacy pandas-based adapters have been removed (see .bak files).
"""

# DuckDB generation adapter
from .generation_adapter_duckdb import (
    load_generation_data,
    get_generation_summary,
    get_available_duids,
    load_gen_data  # Legacy compatibility
)

# DuckDB price adapter
from .price_adapter_duckdb import (
    load_price_data,
    get_price_summary,
    get_available_regions,
    get_price_statistics,
    load_spot_data  # Legacy compatibility
)

# DuckDB transmission adapter
from .transmission_adapter_duckdb import (
    load_transmission_data,
    get_transmission_summary,
    get_available_interconnectors,
    get_flow_statistics
)

# DuckDB rooftop adapter
from .rooftop_adapter_duckdb import (
    load_rooftop_data,
    get_rooftop_at_time,
    get_rooftop_summary,
    smooth_rooftop_data
)

# Smoothing functions from rooftop_adapter (still needed)
from .rooftop_adapter import (
    interpolate_and_smooth,
    henderson_smooth
)

adapter_type = "DuckDB"

from .logging_config import get_logger
logger = get_logger(__name__)
logger.info(f"Using {adapter_type} data adapters")

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
]
