# AEMO Energy Dashboard - Code Inventory

## Summary
- **Total Python Files in Dashboard:** 88 files
- **Total Lines of Code:** 36,514 lines
- **Main Runner Scripts:** 13 scripts
- **Project Structure:** Modular dashboard with 10 functional components

## Project Overview

The AEMO Energy Dashboard is a comprehensive, real-time energy market analysis platform built with Python, Panel, and HoloViews. It uses DuckDB for efficient data querying and provides multiple analysis tabs for different aspects of the Australian National Electricity Market (NEM).

## Main Runner Scripts

| File | Lines | Description |
|------|-------|-------------|
| run_with_conda.py | 16 | Simple conda environment launcher for the dashboard |
| run_dashboard_port5021.py | 21 | Runs dashboard on custom port 5021 |
| run_dashboard_duckdb.py | 24 | Main production runner using DuckDB adapters for optimal performance |
| run_dashboard_optimized.py | 29 | Optimized dashboard runner with performance enhancements |
| run_dashboard_optimized_safe.py | 30 | Safe mode version of optimized runner |
| run_dashboard_debug.py | 31 | Debug mode runner with additional logging |
| run_dashboard_optimized_v2.py | 31 | Second iteration of optimized runner |
| run_dashboard.py | 34 | Standard dashboard runner (legacy) |
| run_dashboard_fast.py | 36 | Fast startup runner with reduced initialization |
| run_dashboard_fixed.py | 36 | Fixed version addressing specific bugs |
| run_dashboard_with_retry.py | 38 | Runner with retry logic for robustness |
| run_dashboard_duckdb_safe.py | 48 | Safe mode DuckDB runner with error handling |
| run_dashboard_immediate.py | 70 | Immediate start runner bypassing some initialization |

**Primary Entry Point:** `run_dashboard_duckdb.py` - Sets USE_DUCKDB=true and launches the main dashboard

## Files by Component

### Core Package Initialization
| File | Lines | Description |
|------|-------|-------------|
| src/aemo_dashboard/\_\_init\_\_.py | 0 | Package initialization file (empty) |

### Shared Components (Infrastructure & Utilities)
| File | Lines | Description |
|------|-------|-------------|
| src/aemo_dashboard/shared/\_\_init\_\_.py | 0 | Shared module initialization |
| src/aemo_dashboard/shared/performance_logger.py | 27 | Simple performance timing logger for optimization |
| src/aemo_dashboard/shared/constants.py | 43 | Dashboard-wide constants and configuration values |
| src/aemo_dashboard/shared/smoothing.py | 61 | Data smoothing utilities including exponential weighted moving average |
| src/aemo_dashboard/shared/logging_config.py | 94 | Centralized logging configuration for all modules |
| src/aemo_dashboard/shared/adapter_selector.py | 107 | Selects between DuckDB and Pandas adapters based on environment |
| src/aemo_dashboard/shared/performance_logging.py | 143 | Advanced performance logging with timing decorators |
| src/aemo_dashboard/shared/config.py | 170 | Configuration management loading from environment variables |
| src/aemo_dashboard/shared/hybrid_query_manager_fast.py | 176 | Fast query manager using hybrid Pandas/DuckDB approach |
| src/aemo_dashboard/shared/rooftop_adapter.py | 232 | Loads rooftop solar data from parquet files |
| src/aemo_dashboard/shared/email_alerts.py | 233 | Email alert system for price and data quality alerts |
| src/aemo_dashboard/shared/duckdb_views_lazy.py | 270 | Lazy-loading DuckDB view definitions for performance |
| src/aemo_dashboard/shared/resolution_utils.py | 270 | Utilities for handling different time resolutions (5min, 30min) |
| src/aemo_dashboard/shared/fuel_categories.py | 282 | Fuel type categorization (renewables, thermal, etc.) |
| src/aemo_dashboard/shared/transmission_adapter.py | 377 | Loads transmission flow data between NEM regions |
| src/aemo_dashboard/shared/transmission_adapter_duckdb.py | 305 | DuckDB-optimized transmission data loader |
| src/aemo_dashboard/shared/generation_adapter.py | 353 | Loads generation data from parquet files using Pandas |
| src/aemo_dashboard/shared/generation_adapter_duckdb.py | 339 | DuckDB-optimized generation data loader with SQL queries |
| src/aemo_dashboard/shared/price_adapter.py | 371 | Loads spot price data from parquet files |
| src/aemo_dashboard/shared/price_adapter_duckdb.py | 358 | DuckDB-optimized price data loader |
| src/aemo_dashboard/shared/rooftop_adapter_duckdb.py | 345 | DuckDB-optimized rooftop solar data loader |
| src/aemo_dashboard/shared/performance_optimizer.py | 358 | Performance optimization utilities and caching |
| src/aemo_dashboard/shared/resolution_indicator.py | 422 | UI indicator showing current data resolution |
| src/aemo_dashboard/shared/hybrid_query_manager.py | 477 | Advanced hybrid query manager with caching |
| src/aemo_dashboard/shared/resolution_manager.py | 498 | Manages automatic resolution switching based on time range |
| src/aemo_dashboard/shared/duckdb_views.py | 641 | DuckDB view definitions for efficient querying |

**Total Shared: 25 files, 6,779 lines**

### NEM Dashboard Tab (Today View - Primary Tab)
| File | Lines | Description |
|------|-------|-------------|
| src/aemo_dashboard/nem_dash/\_\_init\_\_.py | 7 | NEM dashboard module initialization |
| src/aemo_dashboard/nem_dash/display_spot_lightweight.py | 52 | Lightweight spot price display component |
| src/aemo_dashboard/nem_dash/nem_dash_tab_lightweight.py | 69 | Lightweight version of NEM dashboard tab |
| src/aemo_dashboard/nem_dash/nem_dash_tab_optimized.py | 199 | Optimized NEM dashboard with performance improvements |
| src/aemo_dashboard/nem_dash/price_components_hvplot.py | 210 | Price visualization using HvPlot |
| src/aemo_dashboard/nem_dash/price_components_progressive.py | 246 | Progressive loading price components |
| src/aemo_dashboard/nem_dash/nem_dash_tab.py | 323 | Main NEM dashboard tab combining price, gauge, and generation overview |
| src/aemo_dashboard/nem_dash/nem_dash_query_manager.py | 373 | Query manager for NEM dashboard data |
| src/aemo_dashboard/nem_dash/price_components.py | 477 | Complete price section with charts and tables |
| src/aemo_dashboard/nem_dash/daily_summary.py | 493 | Daily summary statistics panel |
| src/aemo_dashboard/nem_dash/generation_overview.py | 508 | 24-hour generation overview by fuel type |
| src/aemo_dashboard/nem_dash/renewable_gauge.py | 536 | Real-time renewable energy percentage gauge |

**Total NEM Dash: 12 files, 3,493 lines**

**Purpose:** The "Today" tab showing current prices, renewable gauge, and 24-hour generation overview

### Generation Tab (Historical Analysis)
| File | Lines | Description |
|------|-------|-------------|
| src/aemo_dashboard/generation/\_\_init\_\_.py | 0 | Generation module initialization |
| src/aemo_dashboard/generation/init_fix_patch.py | 80 | Patch file for initialization bug fixes |
| src/aemo_dashboard/generation/gen_dash_fixed.py | 100 | Fixed version of generation dashboard |
| src/aemo_dashboard/generation/gen_dash_diagnostic.py | 163 | Diagnostic version with extensive logging |
| src/aemo_dashboard/generation/gen_dash_fast.py | 277 | Fast-loading generation dashboard |
| src/aemo_dashboard/generation/gen_dash_debug.py | 315 | Debug version with detailed output |
| src/aemo_dashboard/generation/generation_query_manager.py | 330 | Query manager for generation data with caching |
| src/aemo_dashboard/generation/update_generation.py | 361 | Updates generation data from AEMO sources |
| src/aemo_dashboard/generation/gen_dash_cached.py | 381 | Cached version for improved performance |
| src/aemo_dashboard/generation/gen_dash_optimized.py | 383 | Optimized generation dashboard |
| src/aemo_dashboard/generation/gen_dash_original.py | 2,279 | Original generation dashboard (reference) |
| src/aemo_dashboard/generation/gen_dash.py | 5,692 | Main generation dashboard with interactive charts and filtering |

**Total Generation: 12 files, 10,361 lines**

**Purpose:** Historical generation analysis with interactive time-series charts, fuel type filtering, and smoothing options

### Penetration Tab (Renewable Trends)
| File | Lines | Description |
|------|-------|-------------|
| src/aemo_dashboard/penetration/\_\_init\_\_.py | 3 | Penetration module initialization |
| src/aemo_dashboard/penetration/penetration_tab_v2.py | 303 | Version 2 of penetration analysis |
| src/aemo_dashboard/penetration/penetration_tab_backup.py | 318 | Backup version of penetration tab |
| src/aemo_dashboard/penetration/penetration_tab_backup_20250723_170008.py | 633 | Timestamped backup from July 2025 |
| src/aemo_dashboard/penetration/penetration_tab_optimized.py | 650 | Optimized penetration analysis |
| src/aemo_dashboard/penetration/penetration_tab_optimized_correct.py | 788 | Corrected optimized version |
| src/aemo_dashboard/penetration/penetration_tab.py | 931 | Main penetration tab analyzing VRE trends and thermal vs renewables |

**Total Penetration: 7 files, 3,626 lines**

**Purpose:** Analyzes renewable energy penetration trends, VRE production by fuel type, and thermal vs renewable generation transition

### Curtailment Tab
| File | Lines | Description |
|------|-------|-------------|
| src/aemo_dashboard/curtailment/\_\_init\_\_.py | 7 | Curtailment module initialization with factory function |
| src/aemo_dashboard/curtailment/curtailment_tab.py | 549 | Curtailment analysis showing when and why renewable generation is curtailed |
| src/aemo_dashboard/curtailment/curtailment_query_manager.py | 596 | DuckDB-based query manager for curtailment data |

**Total Curtailment: 3 files, 1,152 lines**

**Purpose:** Analyzes renewable energy curtailment by region, fuel type, and station with network/economic categorization

### Price Analysis Tab
| File | Lines | Description |
|------|-------|-------------|
| src/aemo_dashboard/analysis/\_\_init\_\_.py | 2 | Analysis module initialization |
| src/aemo_dashboard/analysis/price_analysis_original.py | 715 | Original price analysis implementation |
| src/aemo_dashboard/analysis/price_analysis.py | 941 | Price analysis calculation engine (motor) |
| src/aemo_dashboard/analysis/price_analysis_ui.py | 1,246 | Price analysis UI with flexible grouping and aggregation |

**Total Price Analysis: 4 files, 2,904 lines**

**Purpose:** Calculates weighted average prices by region, fuel, technology with hierarchical grouping and filtering

### Station Analysis Tab
| File | Lines | Description |
|------|-------|-------------|
| src/aemo_dashboard/station/\_\_init\_\_.py | 12 | Station module initialization |
| src/aemo_dashboard/station/station_search.py | 361 | Fuzzy search engine for finding stations and DUIDs |
| src/aemo_dashboard/station/station_analysis.py | 452 | Station analysis calculation engine |
| src/aemo_dashboard/station/station_analysis_original.py | 507 | Original station analysis implementation |
| src/aemo_dashboard/station/station_analysis_ui.py | 1,165 | Individual station/DUID analysis UI with time-series and time-of-day charts |
| src/aemo_dashboard/station/coal_analysis.py | 800 | Coal station analysis with revenue, utilization, and evolution charts |

**Total Station Analysis: 6 files, 3,297 lines**

**Purpose:** Detailed analysis of individual generators with time-series, time-of-day patterns, and performance metrics

**Subtabs:**
- **Individual Stations:** Deep-dive analysis of individual generators/DUIDs with time-series, time-of-day patterns, and summary statistics
- **Coal:** Revenue and capacity utilization comparison for all coal stations (latest 12m vs previous 12m) using grouped horizontal bar charts
- **Coal Evolution:** Long-term trends for Bayswater, Tarong, and Loy Yang B showing capacity utilization over time (90-day smoothed) and time-of-day dispatch pattern changes (latest 12m vs 5 years ago)

### Insights Tab
| File | Lines | Description |
|------|-------|-------------|
| src/aemo_dashboard/insights/\_\_init\_\_.py | 4 | Insights module initialization |
| src/aemo_dashboard/insights/insights_tab.py | 2,090 | Insights and market commentary tab with custom analysis |

**Total Insights: 2 files, 2,094 lines**

**Purpose:** Market insights, trends, and custom analysis views

### Spot Prices (Data Updates)
| File | Lines | Description |
|------|-------|-------------|
| src/aemo_dashboard/spot_prices/\_\_init\_\_.py | 0 | Spot prices module initialization |
| src/aemo_dashboard/spot_prices/display_spot.py | 226 | Spot price display component |
| src/aemo_dashboard/spot_prices/twilio_price_alerts.py | 244 | SMS alerts for extreme prices using Twilio |
| src/aemo_dashboard/spot_prices/update_spot.py | 330 | Updates spot price data from AEMO |

**Total Spot Prices: 4 files, 800 lines**

### Rooftop Solar (Data Updates)
| File | Lines | Description |
|------|-------|-------------|
| src/aemo_dashboard/rooftop/\_\_init\_\_.py | 0 | Rooftop module initialization |
| src/aemo_dashboard/rooftop/update_rooftop.py | 528 | Updates rooftop solar data from AEMO |

**Total Rooftop: 2 files, 528 lines**

### Transmission (Data Updates)
| File | Lines | Description |
|------|-------|-------------|
| src/aemo_dashboard/transmission/\_\_init\_\_.py | 2 | Transmission module initialization |
| src/aemo_dashboard/transmission/backfill_transmission.py | 425 | Backfills historical transmission flow data |
| src/aemo_dashboard/transmission/update_transmission.py | 395 | Updates transmission flow data from AEMO |

**Total Transmission: 3 files, 822 lines**

### Combined Updates
| File | Lines | Description |
|------|-------|-------------|
| src/aemo_dashboard/combined/\_\_init\_\_.py | 2 | Combined module initialization |
| src/aemo_dashboard/combined/update_all.py | 125 | Orchestrates all data updates in correct order |

**Total Combined: 2 files, 127 lines**

### Diagnostics
| File | Lines | Description |
|------|-------|-------------|
| src/aemo_dashboard/diagnostics/\_\_init\_\_.py | 7 | Diagnostics module initialization |
| src/aemo_dashboard/diagnostics/data_validity_check.py | 351 | Data quality and validity checks |

**Total Diagnostics: 2 files, 358 lines**

## Component Summary

| Component | Files | Lines | Purpose |
|-----------|-------|-------|---------|
| Shared/Infrastructure | 25 | 6,779 | Core utilities, data adapters, configuration |
| NEM Dashboard (Today Tab) | 12 | 3,493 | Real-time market overview |
| Generation Tab | 12 | 10,361 | Historical generation analysis |
| Penetration Tab | 7 | 3,626 | Renewable energy trends |
| Curtailment Tab | 3 | 1,152 | Renewable curtailment analysis |
| Price Analysis Tab | 4 | 2,904 | Weighted average price analysis |
| Station Analysis Tab | 6 | 3,297 | Individual generator & coal station analysis |
| Insights Tab | 2 | 2,094 | Market insights and commentary |
| Spot Prices Updates | 4 | 800 | Price data updates and alerts |
| Rooftop Updates | 2 | 528 | Rooftop solar data updates |
| Transmission Updates | 3 | 822 | Transmission flow updates |
| Combined Updates | 2 | 127 | Orchestrates all updates |
| Diagnostics | 2 | 358 | Data quality checks |
| Core Package | 1 | 0 | Package initialization |
| **TOTAL** | **88** | **36,514** | **Complete Dashboard** |

## Architecture Notes

### Data Flow
1. **Data Sources:** AEMO NEMWeb reports (dispatch, interconnector, rooftop)
2. **Storage:** Parquet files for historical data
3. **Query Layer:** DuckDB views for fast SQL queries or Pandas adapters
4. **Dashboard:** Panel/HoloViews for interactive web interface

### Key Technologies
- **Panel:** Dashboard framework
- **HoloViews/HvPlot:** Interactive visualizations
- **DuckDB:** Fast analytical queries on parquet files
- **Pandas:** Data manipulation fallback
- **Bokeh:** Underlying plotting library

### Performance Optimizations
- Dual data adapter system (DuckDB for speed, Pandas for compatibility)
- Lazy-loading DuckDB views
- Multi-level caching (query results, chart objects)
- Resolution management (5min vs 30min data)
- Progressive loading for large datasets

### Dashboard Tabs
1. **NEM-dash (Today):** Current market snapshot
2. **Generation:** Historical generation trends
3. **Prices:** Average price analysis by hierarchy
4. **Penetration:** Renewable energy transition
5. **Curtailment:** Renewable curtailment events
6. **Station:** Individual generator deep-dive
   - *Individual Stations:* Time-series and time-of-day analysis for any generator
   - *Coal:* Revenue and capacity utilization comparison for all coal stations
   - *Coal Evolution:* Long-term trends for Bayswater, Tarong, Loy Yang B
7. **Insights:** Market commentary and analysis

## Development Notes

### Entry Points
- **Production:** `run_dashboard_duckdb.py` (recommended)
- **Development:** `run_dashboard_debug.py`
- **Testing:** `run_dashboard_fast.py` (quick startup)

### Configuration
- Environment variables in `.env` file
- Managed by `src/aemo_dashboard/shared/config.py`
- Override paths for data files, ports, email alerts

### Data Updates
- Manual: Run individual update scripts in each module
- Automated: Use `src/aemo_dashboard/combined/update_all.py`
- Typical update cycle: Every 5 minutes for spot prices, every 30 minutes for generation

### Code Quality
- Extensive logging throughout
- Multiple versions of critical components (original, optimized, cached)
- Backup versions preserved for rollback safety
- Performance logging decorators for optimization

---

**Generated:** 2026-01-06
**Project:** AEMO Energy Dashboard
**Location:** /Volumes/davidleitch/aemo_production/aemo-energy-dashboard2
