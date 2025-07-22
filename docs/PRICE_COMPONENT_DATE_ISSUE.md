# Price Component Date Type Mismatch Issue

## Issue Summary

After implementing date filtering to fix the Safari refresh hang (where 346,658 records were being loaded), the price component now shows "No price data available" due to a date type mismatch error.

## Root Cause

The error from the logs:
```
ERROR - Error loading price data via DuckDB: unsupported operand type(s) for -: 'datetime.datetime' and 'datetime.date'
```

This occurs because:
1. The dashboard uses `param.Date` which provides `datetime.date` objects
2. The resolution manager in `resolution_manager.py` expects datetime objects and performs date arithmetic:
   ```python
   duration = end_date - start_date
   hours_from_now = abs((now - end_date).total_seconds() / 3600)
   ```
3. You cannot subtract a `datetime.date` from a `datetime.datetime` object

## What Was Changed

### Original (Working) Code
- `create_price_section()` called with no parameters
- `load_price_data()` called with no parameters  
- This loaded ALL data (346,658 records) but worked

### Changed Code (To Fix Refresh Hang)
- Added date parameters to `load_price_data(start_date=None, end_date=None)`
- Modified `create_price_section(start_date=None, end_date=None)` 
- Updated `nem_dash_tab.py` to pass dashboard dates:
  ```python
  start_date = getattr(dashboard_instance, 'start_date', None)  # Returns datetime.date
  end_date = getattr(dashboard_instance, 'end_date', None)      # Returns datetime.date
  ```

## Proposed Fix

### Option 1: Convert dates in nem_dash_tab.py (Recommended)
```python
# In nem_dash_tab.py
# Get date range from dashboard instance if available
start_date = getattr(dashboard_instance, 'start_date', None) if dashboard_instance else None
end_date = getattr(dashboard_instance, 'end_date', None) if dashboard_instance else None

# Convert date objects to datetime objects for compatibility
if start_date is not None:
    from datetime import datetime
    start_date = datetime.combine(start_date, datetime.min.time())
if end_date is not None:
    from datetime import datetime  
    end_date = datetime.combine(end_date, datetime.max.time())
```

### Option 2: Handle conversion in resolution_manager.py
Make the resolution manager more robust to handle both date and datetime objects:
```python
# In resolution_manager.py
def _ensure_datetime(date_obj):
    """Convert date to datetime if needed"""
    if hasattr(date_obj, 'hour'):  # It's already datetime
        return date_obj
    else:  # It's a date object
        from datetime import datetime
        return datetime.combine(date_obj, datetime.min.time())

# Then use:
start_date = _ensure_datetime(start_date)
end_date = _ensure_datetime(end_date)
```

### Option 3: Keep original behavior as fallback
If date conversion fails, fall back to loading recent data:
```python
# In price_adapter_duckdb.py
try:
    resolution_strategy = resolution_manager.get_optimal_resolution_with_fallback(
        start_date, end_date, 'price'
    )
except TypeError as e:
    # Date type mismatch, use defaults
    logger.warning(f"Date type issue: {e}, using last 48 hours")
    end_date = datetime.now()
    start_date = end_date - timedelta(hours=48)
    resolution_strategy = resolution_manager.get_optimal_resolution_with_fallback(
        start_date, end_date, 'price'
    )
```

## Recommendation

Use **Option 1** - Convert the dates at the source (nem_dash_tab.py) where we know the dashboard provides date objects. This is clearest and prevents the issue from propagating to other components.

## Files to Restore If Needed

Backup files were created:
- `src/aemo_dashboard/nem_dash/price_components.py.backup_before_date_fix`
- `src/aemo_dashboard/nem_dash/nem_dash_tab.py.backup_before_date_fix`

## Next Steps

1. Implement Option 1 fix in `nem_dash_tab.py`
2. Test that price data loads correctly
3. Verify Safari refresh still works (doesn't hang with 346K records)
4. Clean up backup files once confirmed working