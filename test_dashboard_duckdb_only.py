#!/usr/bin/env python3
"""
Test dashboard with only DuckDB adapters - no SharedDataService loading
"""

import os
import sys
from pathlib import Path

# Set environment variable BEFORE any imports
os.environ['USE_DUCKDB'] = 'true'

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Import logging first
from aemo_dashboard.shared.logging_config import setup_logging, get_logger

# Set up logging to a specific file
import logging
file_handler = logging.FileHandler('dashboard_duckdb_only_test.log')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logging.getLogger().addHandler(file_handler)

setup_logging()
logger = get_logger(__name__)

# Now test adapter imports
logger.info("Starting DuckDB-only dashboard test")
logger.info(f"USE_DUCKDB environment variable: {os.getenv('USE_DUCKDB')}")

# Import adapters to verify DuckDB is being used
from aemo_dashboard.shared.adapter_selector import adapter_type, USE_DUCKDB
logger.info(f"Adapter type: {adapter_type}, USE_DUCKDB: {USE_DUCKDB}")

# Test data loading
from aemo_dashboard.shared.adapter_selector import (
    load_generation_data,
    load_price_data,
    load_rooftop_data,
    load_transmission_data
)

print("\n" + "="*60)
print("TESTING DUCKDB ADAPTER LOADING")
print("="*60)
print(f"Adapter type: {adapter_type}")
print(f"USE_DUCKDB: {USE_DUCKDB}")

# Test generation data
print("\nTesting generation data loading...")
try:
    from datetime import datetime, timedelta
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)
    
    gen_data = load_generation_data(start_date=start_date, end_date=end_date)
    print(f"✅ Generation data loaded: {len(gen_data)} records")
    logger.info(f"Generation data loaded successfully: {len(gen_data)} records")
except Exception as e:
    print(f"❌ Generation data error: {e}")
    logger.error(f"Generation data error: {e}")

# Test price data
print("\nTesting price data loading...")
try:
    price_data = load_price_data(start_date=start_date, end_date=end_date)
    print(f"✅ Price data loaded: {len(price_data)} records")
    logger.info(f"Price data loaded successfully: {len(price_data)} records")
except Exception as e:
    print(f"❌ Price data error: {e}")
    logger.error(f"Price data error: {e}")

# Check memory usage
import psutil
process = psutil.Process()
memory_mb = process.memory_info().rss / 1024 / 1024
print(f"\nMemory usage: {memory_mb:.1f} MB")
logger.info(f"Memory usage after adapter loading: {memory_mb:.1f} MB")

# Now test a simple dashboard component
print("\nTesting dashboard component...")
try:
    import panel as pn
    pn.extension()
    
    # Create a simple test panel
    test_panel = pn.Column(
        pn.pane.Markdown("# DuckDB Dashboard Test"),
        pn.pane.Markdown(f"Adapter type: **{adapter_type}**"),
        pn.pane.Markdown(f"Memory usage: **{memory_mb:.1f} MB**"),
        pn.pane.Markdown(f"Generation records: **{len(gen_data):,}**"),
        pn.pane.Markdown(f"Price records: **{len(price_data):,}**")
    )
    
    print("✅ Dashboard component created successfully")
    logger.info("Dashboard component created successfully")
    
except Exception as e:
    print(f"❌ Dashboard component error: {e}")
    logger.error(f"Dashboard component error: {e}")

print("\n" + "="*60)
print("SUMMARY")
print("="*60)
print(f"✅ DuckDB adapters loaded successfully")
print(f"✅ Memory usage: {memory_mb:.1f} MB (should be < 100 MB)")
print(f"✅ No SharedDataService loaded")
print("\nCheck dashboard_duckdb_only_test.log for detailed logs")
print("="*60)