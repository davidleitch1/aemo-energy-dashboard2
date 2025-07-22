# Startup Loading Indicator Implementation

*Date: July 19, 2025, 9:25 PM AEST*

## Summary

Added a simple startup loading indicator that shows immediately when the dashboard is accessed, providing instant feedback to users (especially web browser users) that the application is loading.

## Implementation

### Simple Solution
Instead of adding complexity to each tab, implemented a single loading screen at the main dashboard level that:

1. **Shows immediately** when the dashboard URL is accessed
2. **Displays a professional spinner** with the dashboard title
3. **Automatically transitions** to the full dashboard after initialization

### Technical Details

**File Modified**: `src/aemo_dashboard/generation/gen_dash.py`

**Changes Made** in `create_app()` function:
1. Created an HTML-based loading screen with CSS animation
2. Used a container pattern to swap content after loading
3. Scheduled dashboard initialization with `pn.state.add_periodic_callback()`

### Loading Screen Features

```html
<div style='text-align: center; padding: 100px;'>
    <h1 style='color: #008B8B;'>NEM Analysis Dashboard</h1>
    <div style='margin: 50px auto;'>
        <div class="spinner"></div>
        <p>Initializing dashboard components...</p>
    </div>
</div>
```

- **Spinner**: CSS-animated rotating circle in teal color (#008B8B)
- **Message**: "Initializing dashboard components..."
- **Styling**: Centered, professional appearance

## User Experience

**Before**:
- Blank white screen for 5-10 seconds during startup
- No indication that anything was happening
- Users might think the app was broken

**After**:
- Immediate loading indicator on page access
- Clear branding and messaging
- Smooth transition to full dashboard

## Benefits

1. **Instant feedback**: Users see activity immediately
2. **Professional appearance**: Branded loading screen
3. **Simple implementation**: No complex state management
4. **Minimal code changes**: Only modified the main app creation function
5. **Works for all access methods**: Web browsers, direct access, etc.

## Testing

To see the loading indicator:
1. Start the dashboard: `.venv/bin/python run_dashboard_duckdb.py`
2. Open browser to http://localhost:5006
3. You'll see the loading spinner for 1-2 seconds before the dashboard appears

The loading indicator is especially noticeable on:
- First access (cold start)
- Slower network connections
- When multiple users access simultaneously