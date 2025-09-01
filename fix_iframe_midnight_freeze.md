# Fix for Iframe Midnight Freeze Issue

## Quick Fixes to Try First

### 1. Add Cache-Busting Headers to Dashboard

Add these headers to your Panel app to prevent caching:

```python
# In gen_dash.py, add after template creation:
if pn.state.location:
    pn.state.location.sync_headers = {
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        'Pragma': 'no-cache',
        'Expires': '0',
        'X-Content-Type-Options': 'nosniff',
        'X-Frame-Options': 'ALLOWALL'  # Allow iframe embedding
    }
```

### 2. Modify Iframe to Force Refresh

Update your Quarto page to add timestamp and allow features:

```html
---
title: ""
comments: false
format:
  html:
    page-layout: custom
---

<div class="page-layout-custom" style="height:88vh">
<iframe 
    src="https://nemgen.itkservices2.com" 
    title="rolling-5minute-operation" 
    height="100%" 
    width="100%"
    allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
    allowfullscreen
    sandbox="allow-same-origin allow-scripts allow-popups allow-forms"
    style="border: none;">
</iframe>
</div>

<script>
// Force iframe refresh at midnight
(function() {
    function checkMidnight() {
        const now = new Date();
        const hours = now.getHours();
        const minutes = now.getMinutes();
        
        // Refresh at 00:01 (1 minute after midnight)
        if (hours === 0 && minutes === 1) {
            const iframe = document.querySelector('iframe');
            if (iframe) {
                // Add timestamp to force reload
                const currentSrc = iframe.src.split('?')[0];
                iframe.src = currentSrc + '?t=' + Date.now();
                console.log('Iframe refreshed at midnight');
            }
        }
    }
    
    // Check every minute
    setInterval(checkMidnight, 60000);
})();
</script>
```

### 3. Configure Cloudflare Tunnel

In your Cloudflare dashboard, configure these settings for `nemgen.itkservices2.com`:

1. **Page Rules**:
   - URL: `nemgen.itkservices2.com/*`
   - Cache Level: **Bypass**
   - Always Use HTTPS: On

2. **Caching Configuration**:
   - Browser Cache TTL: **Respect Existing Headers**
   - Edge Cache TTL: **2 minutes**

3. **WebSocket Support**:
   - Ensure WebSockets are enabled for the tunnel
   - In `cloudflared` config, add:
   ```yaml
   - hostname: nemgen.itkservices2.com
     service: http://localhost:5006
     originRequest:
       noTLSVerify: true
       connectTimeout: 30s
       httpHostHeader: localhost
       disableChunkedEncoding: false
   ```

## Advanced Solution: Add Server-Side Push

Create a new file to wrap your dashboard with forced updates:

```python
# iframe_dashboard_wrapper.py
import panel as pn
from aemo_dashboard.generation.gen_dash import EnergyDashboard
from datetime import datetime
import asyncio

pn.extension('bokeh')

class IframeDashboard:
    def __init__(self):
        self.dashboard = EnergyDashboard()
        self.last_refresh = datetime.now()
        
    def create_app(self):
        template = self.dashboard.create_template()
        
        # Add anti-cache headers
        if pn.state.location:
            pn.state.location.sync_headers = {
                'Cache-Control': 'no-cache, no-store, must-revalidate, max-age=0',
                'Pragma': 'no-cache',
                'Expires': '0'
            }
        
        # Force refresh every 5 minutes
        async def force_refresh():
            while True:
                await asyncio.sleep(300)  # 5 minutes
                
                # Check if we crossed midnight
                now = datetime.now()
                if self.last_refresh.date() != now.date():
                    # Force complete refresh
                    if hasattr(self.dashboard, '_force_component_refresh'):
                        self.dashboard._force_component_refresh()
                    
                    # Also trigger JavaScript refresh for iframes
                    if pn.state.location:
                        pn.state.location.reload = True
                
                self.last_refresh = now
        
        pn.state.add_periodic_callback(force_refresh, 300000)
        
        return template

if __name__ == "__main__":
    app = IframeDashboard()
    template = app.create_app()
    template.servable()
```

## Memory Issue Fix

The memory reload message indicates a memory leak. Add this to your dashboard:

```python
# In gen_dash.py, add memory management
import gc

class EnergyDashboard:
    def __init__(self):
        # ... existing init code ...
        
        # Schedule periodic garbage collection
        pn.state.add_periodic_callback(self._cleanup_memory, 600000)  # Every 10 min
    
    def _cleanup_memory(self):
        """Periodic memory cleanup"""
        gc.collect()
        
        # Clear any cached data older than 1 hour
        if hasattr(self, '_cache'):
            current_time = datetime.now()
            keys_to_remove = []
            for key, (timestamp, _) in self._cache.items():
                if (current_time - timestamp).seconds > 3600:
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                del self._cache[key]
        
        logger.info(f"Memory cleanup completed, freed {len(keys_to_remove)} cache entries")
```

## Testing the Fix

1. **Test Cloudflare tunnel directly**:
   ```bash
   curl -I https://nemgen.itkservices2.com
   # Check for Cache-Control headers
   ```

2. **Monitor WebSocket connection**:
   - Open browser DevTools on the Quarto page
   - Go to Network tab → WS (WebSocket)
   - Check if WebSocket connections stay active through midnight

3. **Test iframe refresh**:
   - Open browser console
   - Watch for "Iframe refreshed at midnight" message

## Final Option: Scheduled Page Reload

If WebSockets through Cloudflare remain problematic, force a full page reload:

```javascript
// Add to your Quarto page
<script>
(function() {
    // Calculate milliseconds until 00:01
    function msUntilMidnight() {
        const now = new Date();
        const midnight = new Date(now);
        midnight.setHours(24, 1, 0, 0);  // Next day at 00:01
        return midnight - now;
    }
    
    // Schedule reload for just after midnight
    setTimeout(function() {
        location.reload();
        // Then reload every 24 hours
        setInterval(() => location.reload(), 24 * 60 * 60 * 1000);
    }, msUntilMidnight());
})();
</script>
```

## Diagnostic Commands

Run these to identify the issue:

```bash
# Check if WebSocket upgrade is working through tunnel
curl -i -N \
  -H "Connection: Upgrade" \
  -H "Upgrade: websocket" \
  -H "Sec-WebSocket-Version: 13" \
  -H "Sec-WebSocket-Key: SGVsbG8sIHdvcmxkIQ==" \
  https://nemgen.itkservices2.com/ws

# Check response headers
curl -I https://nemgen.itkservices2.com

# Monitor Panel server connections
lsof -i :5006 | grep ESTABLISHED | wc -l
```

## Summary

The issue is likely that:
1. Cloudflare is caching/interrupting WebSocket updates
2. Memory issues are causing connection drops
3. Iframe security is blocking updates

Apply the fixes in order:
1. Add cache-busting headers to Panel app
2. Configure Cloudflare to bypass cache
3. Add JavaScript midnight refresh to iframe
4. Fix memory leaks in dashboard

This should resolve the iframe midnight freeze while the local display continues working.