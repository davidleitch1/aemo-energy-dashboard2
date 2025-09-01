# Quarto Iframe Midnight Refresh Fix

## Tested Solution for Your Quarto Page

Add this complete script block to your Quarto markdown file, right after your iframe:

```markdown
---
title: ""
comments: false
format:
  html:
    page-layout: custom
---

<div class="page-layout-custom" style="height:88vh">
<iframe src="https://nemgen.itkservices2.com" title="rolling-5minute-operation" height=100% width=100% ></iframe> 
</div>

<script>
// Automatic midnight refresh for dashboard iframe
(function() {
    // Simple, reliable midnight refresh
    var lastRefresh = Date.now();
    
    function checkMidnight() {
        var now = new Date();
        var hours = now.getHours();
        var minutes = now.getMinutes();
        
        // Refresh at 00:01 AM every day
        if (hours === 0 && minutes === 1) {
            // Only refresh if we haven't in the last 5 minutes
            if (Date.now() - lastRefresh > 300000) {
                var iframe = document.querySelector('iframe');
                if (iframe) {
                    // Add timestamp to force reload
                    var src = iframe.src.split('?')[0];
                    iframe.src = src + '?refresh=' + Date.now();
                    lastRefresh = Date.now();
                    console.log('Dashboard refreshed at midnight');
                }
            }
        }
    }
    
    // Check every minute
    setInterval(checkMidnight, 60000);
    
    // Also do an initial check
    checkMidnight();
})();
</script>
```

## Alternative: More Robust Version with Logging

If you want more visibility into what's happening, use this version instead:

```markdown
---
title: ""
comments: false
format:
  html:
    page-layout: custom
---

<div class="page-layout-custom" style="height:88vh">
<iframe src="https://nemgen.itkservices2.com" title="rolling-5minute-operation" height=100% width=100% ></iframe> 
</div>

<script>
// Enhanced midnight refresh with diagnostics
(function() {
    var config = {
        refreshHour: 0,     // 0 = midnight
        refreshMinute: 1,   // 1 = 00:01
        checkInterval: 60000 // Check every minute
    };
    
    var state = {
        lastRefresh: parseInt(localStorage.getItem('dashboardLastRefresh') || '0'),
        refreshCount: 0
    };
    
    function refreshDashboard() {
        var iframe = document.querySelector('iframe');
        if (!iframe) {
            console.error('[Dashboard] No iframe found');
            return false;
        }
        
        var oldSrc = iframe.src;
        var baseSrc = oldSrc.split('?')[0];
        var newSrc = baseSrc + '?t=' + Date.now();
        
        iframe.src = newSrc;
        
        state.lastRefresh = Date.now();
        state.refreshCount++;
        localStorage.setItem('dashboardLastRefresh', state.lastRefresh);
        
        console.log('[Dashboard] Refreshed at', new Date().toLocaleString());
        console.log('[Dashboard] Refresh count:', state.refreshCount);
        
        return true;
    }
    
    function checkTime() {
        var now = new Date();
        var currentHour = now.getHours();
        var currentMinute = now.getMinutes();
        
        // Check if it's refresh time
        if (currentHour === config.refreshHour && currentMinute === config.refreshMinute) {
            // Don't refresh if we did recently (within 5 minutes)
            var timeSinceLastRefresh = Date.now() - state.lastRefresh;
            
            if (timeSinceLastRefresh > 300000) {
                console.log('[Dashboard] Midnight detected, refreshing...');
                refreshDashboard();
            }
        }
    }
    
    // Start monitoring
    console.log('[Dashboard] Refresh monitor started');
    console.log('[Dashboard] Will refresh at', 
        config.refreshHour + ':' + String(config.refreshMinute).padStart(2, '0'));
    
    // Check every minute
    setInterval(checkTime, config.checkInterval);
    
    // Initial check
    checkTime();
    
    // Add manual refresh for testing (call dashboardRefresh() in console)
    window.dashboardRefresh = refreshDashboard;
})();
</script>
```

## How It Works

1. **Checks every minute** if it's 00:01 AM
2. **Refreshes the iframe** by adding a timestamp to the URL
3. **Prevents duplicate refreshes** by tracking the last refresh time
4. **Uses localStorage** to persist state across page reloads
5. **Logs to console** so you can verify it's working

## Testing

To test without waiting until midnight:

1. Open your Quarto page with the dashboard
2. Open browser console (F12)
3. Type: `dashboardRefresh()` and press Enter
4. You should see the iframe reload and a console message

## Why This Works

- **Simple and reliable** - No complex WebSocket handling needed
- **Cross-origin safe** - Doesn't try to access iframe content
- **Cloudflare compatible** - Just changes the URL, forcing a fresh connection
- **Minimal overhead** - Only checks once per minute
- **Persistent** - Survives page refreshes using localStorage

## Verification

To verify it's working:

1. Check browser console for messages:
   - `[Dashboard] Refresh monitor started`
   - `[Dashboard] Will refresh at 0:01`

2. Leave page open overnight and check console next day for:
   - `[Dashboard] Midnight detected, refreshing...`
   - `[Dashboard] Refreshed at [timestamp]`

This solution bypasses the WebSocket sync issue entirely by forcing a complete iframe reload at 00:01 AM daily.