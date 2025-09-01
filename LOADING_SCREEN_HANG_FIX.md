# Dashboard Loading Screen Hang Issue - Fixed

## Problem Description

The dashboard appears to start successfully (logs show initialization complete, server listening on port), but the browser shows the "Initializing dashboard components..." loading screen indefinitely. The actual dashboard content never appears.

## Root Cause

The issue is with Panel's `add_periodic_callback` mechanism used to defer dashboard initialization:

```python
pn.state.add_periodic_callback(initialize_dashboard, period=100, count=1)
```

This callback is supposed to fire after 100ms to replace the loading screen with the actual dashboard, but in certain contexts (especially with the DuckDB backend), the callback never executes.

## Symptoms

1. Dashboard process starts successfully
2. Server listens on port 5008
3. Browser shows loading screen with spinner
4. Loading screen never disappears
5. Logs show "Auto-update started" but UI doesn't update

## Solution

Created `run_dashboard_fixed.py` that bypasses the loading screen entirely and initializes the dashboard immediately. This avoids the callback timing issue.

## Files Created/Modified

1. **`run_dashboard_fixed.py`** - Main fix that initializes dashboard immediately
2. **`src/aemo_dashboard/generation/init_fix_patch.py`** - Patch module that fixes initialization
3. **`start_dashboard_production.sh`** - Updated to use fixed version

## Usage

```bash
# For development
python run_dashboard_fixed.py

# For production
./start_dashboard_production.sh
```

## Technical Details

The fix works by:
1. Removing the loading screen entirely
2. Creating the dashboard instance immediately when requested
3. Using `pn.state.onload` for auto-update instead of `add_periodic_callback`
4. Providing proper error handling if initialization fails

## Note on Column Error

There's also a secondary issue with rooftop solar data where the column `rooftop_solar_mw` doesn't exist (it's actually called `power`). This has been fixed in `rooftop_adapter_duckdb.py`.

---

*Issue identified and fixed: August 2, 2025*