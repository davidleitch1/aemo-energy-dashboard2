# Startup Optimization Notes - July 19, 2025

## Current Issues

### 1. Average Price Analysis Tab - FIXED ✅
**Problem**: "No data available for selected grouping"
**Root Cause**: Column name mismatch
- UI uses 'Fuel' and 'Region' 
- Database has 'fuel_type' and 'region'
**Fix**: Added column name mapping in price_analysis_ui.py

### 2. Slow Initial Render (14 seconds) - INVESTIGATING 🔍
**Problem**: Dashboard takes 14 seconds before first tab renders
**Analysis from logs**:
- `create_views` takes 1.02s at startup (before anything else)
- Dashboard creates multiple HybridQueryManager instances
- Heavy operations during initialization:
  - Loading price data multiple times
  - Creating NEM dash tab components
  - Multiple adapter initializations

**Key timestamps**:
- 19:51:30.183 - First HybridQueryManager initialized
- 19:51:30.603 - Creating Nem-dash tab (0.4s after start)
- 19:51:35.299 - Loading transmission data (5s gap!)
- 19:51:42.514 - Creating station analysis tab (12s from start)

**Bottlenecks identified**:
1. DuckDB view creation (1s)
2. Price data loading between 30.604 and 35.299 (4.7s gap)
3. Multiple duplicate initializations

## Recommendations

### Quick Fixes
1. ✅ Column name mapping (already implemented)
2. Lazy load DuckDB views (don't create all at startup)
3. Cache price data adapter results
4. Avoid duplicate HybridQueryManager instances

### Deeper Optimizations
1. Progressive tab loading (already attempted in fast version)
2. Defer heavy data loads until tab is visible
3. Use singleton pattern for query managers
4. Pre-compile DuckDB views or cache them

## Testing Commands

```bash
# Run dashboard
cd /Users/davidleitch/Library/Mobile\ Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard && .venv/bin/python run_dashboard_duckdb.py

# Test price analysis fix
.venv/bin/python test_price_analysis_fix.py

# Check startup timing
.venv/bin/python test_startup_timing.py
```

## Next Steps
1. Implement lazy DuckDB view creation
2. Add timing logs to identify exact bottlenecks
3. Consider caching DuckDB views between sessions