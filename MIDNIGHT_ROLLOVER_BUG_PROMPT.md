# Critical Bug: Dashboard Stops Updating at Midnight (11:55 PM)

## URGENT: Production Bug Requiring Fix

**Date Created**: August 10, 2025  
**Priority**: CRITICAL  
**Working Directory**: `/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard`

## Problem Statement

The AEMO Energy Dashboard has a critical bug where the display stops updating at 11:55 PM every night. While the backend continues running and collecting data, the browser display freezes showing 23:55 as the last data point. This affects:
- Price charts (showing 23:55 as last point)
- Price tables (stuck at 23:55 values)  
- Generation charts (no updates after 23:55)
- The "Today" tab (first tab that loads)

Even refreshing the browser doesn't fix it - only restarting the entire dashboard resolves the issue temporarily until the next midnight.

## Previous Fix Attempts

Two fixes have already been applied but **the bug persists**:

1. **Fix 1**: Added date refresh to main dashboard's `auto_update_loop()` in `src/aemo_dashboard/generation/gen_dash.py` (lines 2135-2141)
2. **Fix 2**: Added date refresh to NEM dash tab's `update_all_components()` in `src/aemo_dashboard/nem_dash/nem_dash_tab.py` (lines 146-156)

These fixes ensure the date parameters refresh after midnight, but **the display still doesn't update**.

## Your Mission

1. **IDENTIFY** the root cause of why the display freezes at 23:55 despite the date refresh fixes
2. **DESIGN** a comprehensive solution that ensures the display continues updating after midnight
3. **IMPLEMENT** the fix with careful attention to all update mechanisms
4. **TEST** thoroughly with time-based simulations to prove the fix works

## Investigation Starting Points

### Key Files to Examine

```bash
# Main dashboard module
src/aemo_dashboard/generation/gen_dash.py

# NEM dash tab (Today tab) 
src/aemo_dashboard/nem_dash/nem_dash_tab.py
src/aemo_dashboard/nem_dash/price_components.py
src/aemo_dashboard/nem_dash/generation_overview.py

# Query managers that fetch data
src/aemo_dashboard/generation/generation_query_manager.py
src/aemo_dashboard/nem_dash/nem_dash_query_manager.py
src/aemo_dashboard/shared/hybrid_query_manager.py

# Cache and data services
src/aemo_dashboard/shared/shared_data_duckdb.py
```

### Existing Test Scripts

```bash
# Tests that show the date refresh IS working
test_midnight_fix_simple.py
test_midnight_bug_verification.py
test_specific_fixes.py
test_final_verification.py

# Earlier test attempts
test_midnight_rollover_bug.py
test_display_refresh_bug.py
test_auto_update_fix.py
```

## Critical Clues

1. **The backend IS working** - data collection continues after midnight
2. **Date parameters ARE refreshing** - confirmed by tests
3. **But the DISPLAY doesn't update** - suggests the issue is in:
   - How Panel/hvPlot components are refreshed
   - Cache invalidation mechanisms
   - Component recreation vs. update logic
   - Potential timezone issues
   - Query filtering that may exclude "future" data

## Testing Requirements

Your fix MUST pass these tests:

### Test 1: Midnight Rollover
```python
# Simulate dashboard running from 11:50 PM to 12:10 AM
# Verify that:
# - Price chart shows data points after 00:00
# - Price table shows current 5-minute intervals after midnight
# - Generation chart continues updating
# - All timestamps are synchronized
```

### Test 2: Browser Refresh
```python
# After midnight, simulate browser refresh
# Verify that:
# - Dashboard shows current data, not yesterday's 23:55
# - No components are stuck on old data
```

### Test 3: Component Updates
```python
# Track actual Panel component updates
# Verify that:
# - Components are being recreated or properly refreshed
# - Cache keys are changing appropriately
# - New data is being fetched and displayed
```

## Suggested Investigation Approach

1. **Add extensive logging** around midnight to track:
   - What queries are being made
   - What data is returned
   - How components are updated
   - Cache hit/miss patterns

2. **Check timezone handling**:
   ```python
   # Are we comparing dates correctly?
   # Is there a timezone mismatch causing filtering issues?
   ```

3. **Examine Panel component lifecycle**:
   ```python
   # Are we updating existing components or creating new ones?
   # Do Panel components need explicit refresh calls?
   ```

4. **Test cache invalidation**:
   ```python
   # Is cached data from before midnight being served after midnight?
   # Are cache keys properly incorporating the date change?
   ```

5. **Review data filtering logic**:
   ```python
   # Is there a filter that excludes "future" data?
   # Are we accidentally filtering out today's data after midnight?
   ```

## Implementation Guidelines

When you find and fix the bug:

1. **Document the root cause** clearly
2. **Add inline comments** explaining the fix
3. **Create a specific test** that would have caught this bug
4. **Update CLAUDE.md** with the solution
5. **Test on development first**, then deploy to production

## Success Criteria

The fix is successful when:
- ✅ Dashboard display continues updating seamlessly past midnight
- ✅ Price charts show data points after 00:00
- ✅ Price tables display current 5-minute intervals
- ✅ Generation charts keep updating with new data  
- ✅ Browser refresh after midnight shows current data
- ✅ All tabs affected by the bug are fixed
- ✅ No manual intervention required at midnight

## Environment Setup

```bash
# Working directory
cd /Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard

# Activate virtual environment
source .venv/bin/activate

# Run dashboard for testing
python run_dashboard_duckdb.py

# The dashboard will be at http://localhost:5006
```

## Production Information

- Production path: `/Volumes/davidleitch/aemo_production/aemo-energy-dashboard2/`
- The bug occurs EVERY night at 11:55 PM
- Only a full restart fixes it temporarily
- This is affecting live users

## IMPORTANT NOTES

1. **The date refresh fixes ARE working** - tests confirm dates update correctly
2. **The problem is the DISPLAY not updating** - this is likely a Panel/hvPlot issue
3. **Focus on component refresh mechanisms** - not date calculations
4. **This has been partially debugged** - build on existing work, don't start over

Good luck! This is a critical production bug that needs urgent resolution.