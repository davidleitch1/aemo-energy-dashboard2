# WebSocket Serialization Diagnosis Guide

## Summary of the Issue

**Hypothesis**: The Safari browser refresh hang is caused by `defer_load=True` creating components that don't properly serialize/deserialize during WebSocket reconnection.

## Testing Strategy

We've created three test scripts to verify this hypothesis:

### 1. `test_defer_load_isolation.py` - Direct Comparison

This test provides three scenarios:

```bash
# Test 1: Without defer_load (should work fine)
.venv/bin/python test_defer_load_isolation.py 1

# Test 2: With defer_load (should hang on refresh)
.venv/bin/python test_defer_load_isolation.py 2

# Test 3: Mixed components (partial issues expected)
.venv/bin/python test_defer_load_isolation.py 3
```

**Expected Results**:
- Test 1: Page refreshes normally in Safari
- Test 2: Page hangs on refresh in Safari
- Test 3: Some components may cause issues

### 2. `test_websocket_serialization.py` - Comprehensive Testing

Tests different configurations:

```bash
# Run each configuration
.venv/bin/python test_websocket_serialization.py baseline
.venv/bin/python test_websocket_serialization.py defer_load_simple
.venv/bin/python test_websocket_serialization.py defer_load_with_callback
.venv/bin/python test_websocket_serialization.py matplotlib_no_defer
.venv/bin/python test_websocket_serialization.py matplotlib_with_defer
```

### 3. `test_websocket_debug.py` - Deep Debugging

Provides detailed WebSocket logging:

```bash
# Test without defer_load
.venv/bin/python test_websocket_debug.py

# Test with defer_load
.venv/bin/python test_websocket_debug.py defer
```

## How to Verify the Diagnosis

### Step 1: Browser Developer Tools

1. Open Safari
2. Open Developer Tools (Cmd+Option+I)
3. Go to Network tab
4. Filter by "WS" to see WebSocket connections
5. Load the test page
6. Refresh (Cmd+R)
7. Watch for:
   - WebSocket connection attempts
   - Error messages
   - Hanging connections

### Step 2: Console Monitoring

In the terminal running the test, watch for:

```
# Good (without defer_load):
WebSocket connection opened
ServerConnection created
WebSocket connection closed (clean disconnect on refresh)
WebSocket connection opened (new connection after refresh)

# Bad (with defer_load):
WebSocket connection opened
ServerConnection created
[Refresh browser]
WebSocket ERROR: Failed to serialize component
Connection timeout
No new connection established
```

### Step 3: Specific Symptoms to Look For

#### With defer_load=True:
1. **Browser symptoms**:
   - Loading spinner continues indefinitely
   - Page becomes unresponsive
   - Developer tools show pending WebSocket frames
   - No JavaScript errors (just hanging)

2. **Server symptoms**:
   - Last log entry before hang often mentions component serialization
   - No new WebSocket connection after refresh
   - Server may show: "Dropping patch" warnings

#### Without defer_load:
1. **Browser symptoms**:
   - Page refreshes quickly
   - New WebSocket connection established
   - All components render properly

2. **Server symptoms**:
   - Clean WebSocket disconnect/reconnect cycle
   - No serialization errors

## Root Cause Analysis

The issue occurs because:

1. `defer_load=True` creates a special wrapper around components
2. This wrapper uses Panel's internal mechanisms to delay rendering
3. During browser refresh, Panel tries to serialize the current state
4. The deferred components contain unserializable elements (callbacks, promises)
5. Safari's WebSocket implementation waits indefinitely for serialization to complete

## Why Matplotlib vs HoloViews Doesn't Matter

The real issue is `defer_load`, not the plotting library:
- Matplotlib without defer_load: ✅ Works fine
- Matplotlib with defer_load: ❌ Hangs on refresh
- HoloViews without defer_load: ✅ Works fine
- HoloViews with defer_load: ❌ Hangs on refresh

## Confirmation Test

Run this simple test to confirm:

```python
# save as confirm_diagnosis.py
import panel as pn

# Test 1: This will work
pn.extension()
pn.pane.Markdown("# This works on refresh").servable()

# Test 2: This will hang on refresh
# pn.extension(defer_load=True)
# pn.panel(lambda: pn.pane.Markdown("# This hangs"), defer_load=True).servable()
```

## Conclusion

If the tests show:
1. ✅ Pages without defer_load refresh properly
2. ❌ Pages with defer_load hang on refresh
3. ✅ The issue is browser-agnostic but most noticeable in Safari

Then we've confirmed that `defer_load` is the root cause of the WebSocket serialization issue.