# AEMO Energy System - Known Issues

This document tracks known issues, bugs, and areas for improvement in the AEMO Energy System.

---

## Issue #1: Data Collector Initialization 403 Errors

**Date Reported**: July 20, 2025, 9:35 PM AEST  
**Severity**: Medium  
**Status**: Active  
**Component**: aemo-data-updater/unified_collector.py

### Description
When the unified collector starts or restarts, it encounters 403 Forbidden errors when trying to download 30-minute trading files from AEMO. The collector sees all files on AEMO as "new" because it doesn't persist its file tracking state between restarts.

### Symptoms
- On startup: "Found 4034 new trading files"
- Multiple 403 errors for URLs like:
  - `http://nemweb.com.au/Reports/CURRENT/TradingIS_Reports/PUBLIC_TRADINGIS_202507201955_0000000472810592.zip`
- Errors resolve after a few cycles as the collector builds up its `last_files` history
- Issue recurs every time the collector is restarted

### Root Cause
The `UnifiedCollector` class initializes `self.last_files` as empty sets:
```python
self.last_files = {
    'prices5': set(),
    'scada5': set(),
    'trading': set(),
    ...
}
```

This causes the collector to:
1. Think all files on AEMO are "new"
2. Try to download thousands of files
3. Get rate-limited or blocked by AEMO
4. Receive 403 errors for recent files

### Impact
- Temporary loss of 30-minute data collection on startup
- Unnecessary load on AEMO servers
- Delays in reaching stable operation

### Workaround
Let the collector run for several cycles. It will stabilize once it builds up its internal file history.

### Proposed Solutions

#### Solution 1: Persist last_files state
```python
# On shutdown
with open('last_files_state.json', 'w') as f:
    json.dump({k: list(v) for k, v in self.last_files.items()}, f)

# On startup
try:
    with open('last_files_state.json', 'r') as f:
        state = json.load(f)
        self.last_files = {k: set(v) for k, v in state.items()}
except FileNotFoundError:
    self.last_files = {k: set() for k in ['prices5', 'scada5', ...]}
```

#### Solution 2: Initialize from existing data
```python
def initialize_last_files(self):
    """Check existing parquet files to determine what's already been collected"""
    for data_type, file_path in self.output_files.items():
        if file_path.exists():
            df = pd.read_parquet(file_path)
            if not df.empty:
                latest = df['settlementdate'].max()
                # Use latest timestamp to filter AEMO file list
                # Only process files newer than latest
```

#### Solution 3: Add file limit on initialization
```python
# Process maximum 50 files on first run
if not self.last_files['trading']:  # First run
    new_files = new_files[-50:]  # Only recent files
```

### References
- Code location: `src/aemo_updater/collectors/unified_collector.py` lines 66-70
- Error first observed during production migration
- Similar issue might affect other collectors (price, generation, transmission)

---

## Issue #2: Hardcoded iCloud Paths

**Date Reported**: July 20, 2025, 9:40 PM AEST  
**Severity**: High  
**Status**: Partially Fixed  
**Component**: aemo-data-updater/unified_collector.py

### Description
The unified collector has hardcoded paths pointing to iCloud storage instead of using environment variables, causing data to be written to the wrong location on the production machine.

### Code Location
Lines 39-40 in `unified_collector.py`:
```python
self.base_path = Path("/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot")
self.data_path = self.base_path / "aemo-data-updater" / "data 2"
```

### Impact
- Data written to wrong location
- .env configuration ignored
- Causes "Parquet magic bytes not found" errors

### Fix Applied
Changed to use production path directly. Should be refactored to use environment variables.

### Proposed Permanent Solution
```python
import os
from pathlib import Path

# Use environment variable with fallback
data_path = os.getenv('AEMO_DATA_PATH', '/Users/davidleitch/aemo_production/data')
self.data_path = Path(data_path)
```

---

## Issue #3: Trading Files URL Case Sensitivity

**Date Reported**: July 20, 2025, 8:30 PM AEST  
**Severity**: Low  
**Status**: Fixed  
**Component**: aemo-data-updater/unified_collector.py

### Description
AEMO URLs are case-sensitive but inconsistent. Some use "CURRENT" while others use "Current".

### Resolution
Updated all URLs to use correct case. Working URLs:
- `http://nemweb.com.au/Reports/CURRENT/Dispatch_SCADA/`
- `http://nemweb.com.au/Reports/CURRENT/TradingIS_Reports/`

---

## Contributing

When adding new issues:
1. Include timestamp and timezone
2. Specify severity (Critical/High/Medium/Low)
3. Include code locations and line numbers
4. Document any workarounds
5. Propose solutions where possible

---

## Issue #4: Browser Refresh Causes Dashboard Startup Hang

**Date Reported**: July 20, 2025, 10:35 PM AEST  
**Severity**: Medium  
**Status**: Active  
**Component**: aemo-energy-dashboard2/generation/gen_dash.py

### Description
Refreshing the localhost browser window sometimes causes the dashboard to hang during startup. This appears to be related to Panel's session management and resource initialization.

### Symptoms
- Browser refresh occasionally results in hanging/unresponsive dashboard
- May require killing and restarting the Panel server
- Inconsistent - doesn't happen every time

### Potential Causes
- Panel session cleanup issues
- DuckDB connection not properly closed on session end
- Resource contention between old and new sessions
- Memory/cache not properly released

### Proposed Solutions
1. Implement proper session cleanup hooks
2. Ensure DuckDB connections are closed on session end
3. Add connection pooling for DuckDB
4. Implement session-specific resource management

---

## Issue #5: Slow Dashboard Startup Times

**Date Reported**: July 20, 2025, 10:35 PM AEST  
**Severity**: High  
**Status**: Active  
**Component**: aemo-energy-dashboard2 (all modules)

### Description
Dashboard startup times remain slow despite DuckDB optimization. Initial load can take 6-10 seconds before the interface becomes responsive.

### Current Performance
- Initial startup: 6-10 seconds
- Tab switching: 1-2 seconds
- Data queries: 100-700ms

### Identified Bottlenecks
1. **Synchronous initialization**: All components load at startup
2. **Pre-loading data**: Some modules load data before it's needed
3. **UI component creation**: Heavy Panel components created upfront
4. **Missing lazy loading**: All tabs initialized even if not viewed

### Proposed Solutions

#### Solution 1: Lazy Tab Loading
```python
# Only initialize tab content when selected
def get_tab_content(tab_name):
    if tab_name == "Generation":
        return create_generation_tab()  # Create on demand
    elif tab_name == "Price Analysis":
        return create_price_tab()
```

#### Solution 2: Async Component Loading
```python
# Load data asynchronously after UI renders
pn.state.onload(lambda: load_initial_data())
```

#### Solution 3: Progressive Enhancement
- Show UI skeleton immediately
- Load data in background
- Update visualizations as data arrives

### Target Performance
- Initial UI display: < 1 second
- Full functionality: < 3 seconds
- Perceived performance: Instant

---

## Issue #6: Production Dashboard Shows Only Daily Aggregated Data

**Date Reported**: July 21, 2025, 8:40 AM AEST  
**Severity**: High  
**Status**: Active  
**Component**: aemo-energy-dashboard2 (production)

### Description
The production dashboard is only showing 2 data points in the 24-hour generation chart, while the development version shows proper 5-minute resolution data. The logs indicate the dashboard is using "pre-aggregated data for 1997 day range" even for the 24-hour view.

### Symptoms
- Generation chart shows only 2 daily data points instead of ~288 (5-min) or ~48 (30-min)
- Rooftop solar shows as 0 MW throughout
- Logs show: "Using dashboard processed data: 2 records for last 24h"
- Dashboard appears to be defaulting to "All Available Data" time range selection

### Root Causes Identified
1. **Malformed .env file**: Production .env file contained shell script wrapper content
2. **Hardcoded paths**: resolution_manager.py has hardcoded iCloud path instead of production path
3. **Time range selection**: Dashboard may be defaulting to longest time range with daily aggregation

### Fixes Applied
1. ✅ Fixed .env file formatting
2. ✅ Updated hardcoded path in resolution_manager.py to `/Users/davidleitch/aemo_production/data`

### Next Steps
1. Restart production dashboard to pick up configuration changes
2. Verify dashboard loads with correct time range selection
3. Check if NEM Dashboard tab is forcing daily aggregation

---

*Last Updated: July 21, 2025, 8:40 AM AEST*