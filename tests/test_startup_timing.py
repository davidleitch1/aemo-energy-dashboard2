#!/usr/bin/env python3
"""
Test startup timing and identify bottlenecks
"""
import time
import os
import sys
from pathlib import Path

# Track timing for each import
timings = {}
start_time = time.time()

def time_import(name, import_func):
    """Time an import operation"""
    t0 = time.time()
    result = import_func()
    duration = time.time() - t0
    timings[name] = duration
    print(f"{name}: {duration:.3f}s")
    return result

print("Starting startup timing analysis...")
print("-" * 50)

# Time environment setup
t0 = time.time()
os.environ['USE_DUCKDB'] = 'true'
sys.path.insert(0, str(Path(__file__).parent / 'src'))
timings['Environment setup'] = time.time() - t0
print(f"Environment setup: {timings['Environment setup']:.3f}s")

# Time major imports
def import_panel():
    import panel as pn
    return pn

def import_pandas():
    import pandas as pd
    return pd

def import_duckdb():
    import duckdb
    return duckdb

def import_numpy():
    import numpy as np
    return np

def import_hvplot():
    import hvplot.pandas
    return hvplot

# Time each import
pn = time_import("Import panel", import_panel)
pd = time_import("Import pandas", import_pandas)
duckdb = time_import("Import duckdb", import_duckdb)
np = time_import("Import numpy", import_numpy)
hvplot = time_import("Import hvplot", import_hvplot)

# Time dashboard module imports
def import_logging_config():
    from aemo_dashboard.shared.logging_config import get_logger
    return get_logger

def import_config():
    from aemo_dashboard.shared.config import Config
    return Config

def import_hybrid_query():
    from aemo_dashboard.shared.hybrid_query_manager import HybridQueryManager
    return HybridQueryManager

def import_gen_dash():
    from aemo_dashboard.generation import gen_dash
    return gen_dash

time_import("Import logging config", import_logging_config)
time_import("Import config", import_config)
time_import("Import HybridQueryManager", import_hybrid_query)

# Time the actual dashboard import
gen_dash = time_import("Import gen_dash module", import_gen_dash)

# Time dashboard initialization
t0 = time.time()
# We'll just check what the main function does without running it
if hasattr(gen_dash, 'main'):
    print(f"\nDashboard has main() function")
timings['Check main function'] = time.time() - t0

# Summary
print("\n" + "=" * 50)
print("STARTUP TIMING SUMMARY")
print("=" * 50)

total_time = time.time() - start_time
print(f"\nTotal startup time: {total_time:.3f}s")

# Sort by duration
sorted_timings = sorted(timings.items(), key=lambda x: x[1], reverse=True)
print("\nTop time consumers:")
for name, duration in sorted_timings[:10]:
    percentage = (duration / total_time) * 100
    print(f"  {name}: {duration:.3f}s ({percentage:.1f}%)")

# Recommendations
print("\n" + "=" * 50)
print("OPTIMIZATION OPPORTUNITIES")
print("=" * 50)

if timings.get('Import hvplot', 0) > 0.5:
    print("- hvplot import is slow - consider lazy loading")
if timings.get('Import pandas', 0) > 0.3:
    print("- pandas import is heavy - defer until needed")
if timings.get('Import panel', 0) > 0.5:
    print("- panel import is slow - check extensions")
if timings.get('Import gen_dash module', 0) > 1.0:
    print("- gen_dash module has heavy initialization")