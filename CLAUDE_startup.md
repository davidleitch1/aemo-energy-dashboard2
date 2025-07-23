# Dashboard Startup Hanging Analysis

## Date: July 23, 2025

### Problem Description
The dashboard intermittently hangs during startup - "every now and then" it freezes on load, requiring manual intervention. This is a classic symptom of race conditions or resource contention.

## Root Causes Identified

### 1. Synchronous Data Loading During Startup
When the dashboard starts, it immediately creates the "Today" tab which loads 4 components synchronously:

```python
# In create_nem_dash_tab():
price_chart = create_price_chart_component(start_date, end_date)      # Loads price data
price_table = create_price_table_component(start_date, end_date)      # Loads price data AGAIN
renewable_gauge = create_renewable_gauge_component(dashboard_instance) # Loads generation data
generation_overview = create_generation_overview_component(...)        # Loads generation + transmission data
```

Each component makes blocking file I/O calls without timeouts.

### 2. File Access Conflicts
The intermittent nature strongly suggests file locking issues:
- **Data updater runs every 4.5 minutes** writing to the same parquet files
- **Race condition**: If dashboard starts while updater is writing, it hangs waiting for file lock
- **Multiple readers**: Components simultaneously reading the same files can cause locks
- **No file access coordination** between reader (dashboard) and writer (updater)

### 3. iCloud Drive Sync Issues
Data files are stored in iCloud path:
```
/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/
```

Issues with cloud storage:
- **Sync delays**: iCloud might be uploading/downloading files during read
- **Network latency**: Cloud file operations can hang on network issues
- **File coordination**: macOS file coordination for cloud files introduces unpredictable delays
- **Metadata operations**: Even checking file existence can hang with iCloud

### 4. No Timeouts on File Operations
Current implementation has no safeguards:
```python
# This can hang indefinitely:
data = load_price_adapter(start_date=start_date, end_date=end_date)
df = pd.read_parquet(file_path)  # No timeout
```

## Why It's Intermittent

The hanging requires perfect storm timing:
1. Dashboard starts at exact moment updater is writing
2. iCloud sync is active
3. Network/disk I/O is temporarily slow
4. Multiple components hit same file simultaneously

This explains why it's hard to reproduce consistently.

## Evidence Supporting This Analysis

1. **Timing**: Hangs likely correlate with 4.5-minute update intervals
2. **File location**: iCloud paths are inherently less reliable than local storage
3. **Multiple data loads**: Today tab loads same data multiple times
4. **No error handling**: Code lacks timeout/retry mechanisms

## Testing the Hypothesis

### 1. Check Timing Correlation
- Note exact times when hanging occurs
- Check if it aligns with data updater schedule (every 4.5 minutes)
- Look for patterns around :00, :04, :09, :13, etc.

### 2. Test with Local Files
```bash
# Copy files to local directory
cp ~/Library/Mobile\ Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/*.parquet /tmp/test_data/
# Update config to use local path
# Run dashboard multiple times
```

### 3. Add Debug Logging
Add timestamps before each data load:
```python
logger.info(f"[{time.time()}] Starting price data load...")
data = load_price_adapter(...)
logger.info(f"[{time.time()}] Price data loaded")
```

### 4. Monitor File Access
During hang, check file access:
```bash
lsof | grep parquet
# Shows which processes have files open
```

## Immediate Solutions

### 1. Add Timeouts (Quick Fix)
```python
import functools
import threading

def timeout(seconds):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            result = [None]
            exception = [None]
            
            def target():
                try:
                    result[0] = func(*args, **kwargs)
                except Exception as e:
                    exception[0] = e
            
            thread = threading.Thread(target=target)
            thread.start()
            thread.join(seconds)
            
            if thread.is_alive():
                raise TimeoutError(f"{func.__name__} timed out after {seconds}s")
            if exception[0]:
                raise exception[0]
            return result[0]
        return wrapper
    return decorator

# Use it:
@timeout(10)  # 10 second timeout
def load_price_data_safe():
    return pd.read_parquet(file_path)
```

### 2. Retry Logic
```python
def load_with_retry(file_path, max_attempts=3, delay=1):
    for attempt in range(max_attempts):
        try:
            return pd.read_parquet(file_path)
        except Exception as e:
            if attempt < max_attempts - 1:
                time.sleep(delay)
                continue
            raise
```

### 3. File Lock Checking
```python
import fcntl

def is_file_locked(filepath):
    try:
        with open(filepath, 'rb') as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            return False
    except IOError:
        return True
```

## Long-term Solutions

### 1. Use Database Instead of Files
- Implement PostgreSQL/DuckDB for concurrent access
- Already partially done with DuckDB integration

### 2. Implement Proper File Coordination
- Use file locking mechanisms
- Implement read/write separation
- Use temporary files for updates

### 3. Async Loading
- Load components asynchronously
- Show loading indicators
- Prevent blocking UI thread

### 4. Move Away from iCloud
- Use local SSD storage for data files
- Implement separate backup strategy
- Avoid cloud sync for active data files

## Recommended Action Plan

1. **Immediate**: Add timeouts to all file operations (1 day)
2. **Short-term**: Implement retry logic with exponential backoff (2 days)
3. **Medium-term**: Complete DuckDB migration for concurrent access (1 week)
4. **Long-term**: Move data files to local storage with proper backup (2 weeks)

The intermittent hanging is almost certainly due to file access conflicts between the dashboard and data updater, exacerbated by iCloud sync delays.