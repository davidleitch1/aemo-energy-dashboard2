#!/usr/bin/env python3
"""
Fix for iframe midnight freeze - proper WebSocket synchronization
Based on diagnostic findings that param.trigger() doesn't push to clients
"""

print("\n" + "=" * 80)
print("IFRAME MIDNIGHT FREEZE - TARGETED FIX")
print("=" * 80)

print("\n## FIX 1: Enhanced _force_component_refresh() with WebSocket push")
print("-" * 60)

enhanced_refresh_code = '''
def _force_component_refresh(self):
    """
    Force Panel components to refresh AND push to WebSocket clients.
    Enhanced version that ensures client synchronization.
    """
    try:
        logger.info("Starting forced component refresh due to date range change")
        
        # Import Panel's document access
        import panel as pn
        from bokeh.io import push_notebook
        
        # Find all Panel panes and force them to refresh
        components_refreshed = []
        
        # List of ACTUAL pane attributes in the dashboard
        pane_attrs = [
            'plot_pane',              # Main generation plot
            'price_plot_pane',        # Price plot
            'transmission_pane',      # Transmission plot
            'utilization_pane',       # Utilization plot
            'bands_plot_pane',        # Price bands plot
            'tod_plot_pane',          # Time of day plot
            'renewable_gauge',        # Renewable gauge (if exists)
            'loading_indicator'       # Loading indicator
        ]
        
        for attr_name in pane_attrs:
            if hasattr(self, attr_name):
                pane = getattr(self, attr_name)
                
                # Check if it's a Panel pane with an object property
                if hasattr(pane, 'object') and hasattr(pane, 'param'):
                    try:
                        # CRITICAL FIX: Update the object directly to trigger WebSocket sync
                        if hasattr(pane, 'object'):
                            # Store current object
                            current_object = pane.object
                            
                            # Force a complete refresh by setting to None first
                            # This generates a proper ModelChangedEvent
                            pane.object = None
                            
                            # Set back to trigger full update
                            pane.object = current_object
                            
                            components_refreshed.append(f"{attr_name} (object reset)")
                            logger.debug(f"Reset object for {attr_name}")
                            
                    except Exception as e:
                        logger.warning(f"Could not refresh {attr_name}: {e}")
        
        # CRITICAL: Force WebSocket push to all connected clients
        if pn.state.curdoc:
            # Get the current document
            doc = pn.state.curdoc
            
            # Schedule an immediate callback to push changes
            def push_updates():
                """Push updates to all WebSocket clients"""
                try:
                    # This forces Bokeh to send pending events to clients
                    if hasattr(doc, 'add_next_tick_callback'):
                        # Trigger a document change event
                        doc.title = doc.title  # Simple change to force sync
                        logger.info("Forced WebSocket sync via document update")
                except Exception as e:
                    logger.error(f"Error pushing to clients: {e}")
            
            # Use Panel's thread-safe execution
            if hasattr(pn.state, 'execute'):
                pn.state.execute(push_updates)
            else:
                doc.add_next_tick_callback(push_updates)
            
            logger.info("Scheduled WebSocket push to clients")
        
        # Also refresh any tabs that might exist
        if hasattr(self, 'tabs') and hasattr(self.tabs, 'param'):
            try:
                # Instead of param.trigger, modify the active tab to force sync
                if hasattr(self.tabs, 'active'):
                    current_tab = self.tabs.active
                    self.tabs.active = (current_tab + 1) % len(self.tabs)
                    self.tabs.active = current_tab
                    components_refreshed.append("tabs (active toggle)")
            except Exception as e:
                logger.debug(f"Could not refresh tabs: {e}")
        
        logger.info(f"Forced refresh completed for {len(components_refreshed)} components: {', '.join(components_refreshed)}")
        
    except Exception as e:
        logger.error(f"Error in _force_component_refresh: {e}")
'''

print("Enhanced _force_component_refresh() method:")
print(enhanced_refresh_code)

print("\n## FIX 2: Memory Leak Prevention")
print("-" * 60)

memory_fix_code = '''
def _cleanup_old_components(self):
    """Clean up old components before creating new ones to prevent memory leaks"""
    import gc
    
    try:
        # List of components to clean
        components_to_clean = [
            'plot_pane', 'price_plot_pane', 'transmission_pane',
            'utilization_pane', 'bands_plot_pane', 'tod_plot_pane'
        ]
        
        for attr_name in components_to_clean:
            if hasattr(self, attr_name):
                pane = getattr(self, attr_name)
                if pane and hasattr(pane, 'object'):
                    # Clear the object reference
                    pane.object = None
                    
                # Delete the attribute
                delattr(self, attr_name)
                logger.debug(f"Cleaned up {attr_name}")
        
        # Force garbage collection
        gc.collect()
        logger.info("Component cleanup completed")
        
    except Exception as e:
        logger.error(f"Error in cleanup: {e}")

# Call this in update_plot() before creating new components:
# self._cleanup_old_components()
'''

print("Memory leak prevention code:")
print(memory_fix_code)

print("\n## FIX 3: Client-Side Iframe Refresh (Fallback)")
print("-" * 60)

iframe_refresh_html = '''
<!-- Add this to your Quarto HTML page -->
<script>
(function() {
    let lastUpdateTime = Date.now();
    let frozenCount = 0;
    
    // Monitor for frozen updates
    function checkForFreeze() {
        // Check if iframe is still updating
        const iframe = document.querySelector('iframe');
        if (!iframe) return;
        
        try {
            // Try to access iframe content (may fail due to cross-origin)
            const iframeDoc = iframe.contentDocument || iframe.contentWindow.document;
            const updateElement = iframeDoc.querySelector('.header-container div:last-child');
            
            if (updateElement) {
                const timeText = updateElement.textContent;
                const timeMatch = timeText.match(/(\d{2}):(\d{2}):(\d{2})/);
                
                if (timeMatch) {
                    const [_, hours, minutes, seconds] = timeMatch;
                    const now = new Date();
                    
                    // Check if time is frozen at 23:55
                    if (hours === '23' && minutes === '55') {
                        frozenCount++;
                        
                        // If frozen for more than 10 minutes, refresh
                        if (frozenCount > 2) {
                            console.log('Dashboard frozen at 23:55, refreshing iframe...');
                            iframe.src = iframe.src + '?t=' + Date.now();
                            frozenCount = 0;
                        }
                    } else {
                        frozenCount = 0;
                    }
                }
            }
        } catch(e) {
            // Cross-origin, use time-based refresh instead
            const now = new Date();
            
            // Refresh at 00:01 every day
            if (now.getHours() === 0 && now.getMinutes() === 1) {
                const timeSinceLastRefresh = Date.now() - lastUpdateTime;
                
                // Only refresh if we haven't refreshed in the last 5 minutes
                if (timeSinceLastRefresh > 300000) {
                    console.log('Midnight refresh triggered');
                    iframe.src = iframe.src.split('?')[0] + '?t=' + Date.now();
                    lastUpdateTime = Date.now();
                }
            }
        }
    }
    
    // Check every 5 minutes
    setInterval(checkForFreeze, 300000);
    
    // Also check at specific times
    setInterval(() => {
        const now = new Date();
        const mins = now.getMinutes();
        const hours = now.getHours();
        
        // Check at 00:01, 00:02, 00:03 for midnight refresh
        if (hours === 0 && mins >= 1 && mins <= 3) {
            checkForFreeze();
        }
    }, 60000); // Check every minute
})();
</script>
'''

print("Client-side iframe refresh code:")
print(iframe_refresh_html)

print("\n## FIX 4: Server Configuration for WebSocket")
print("-" * 60)

server_config = '''
# In your dashboard startup script or configuration:

import panel as pn

# Configure Panel for better WebSocket handling
pn.config.sizing_mode = 'stretch_width'
pn.config.loading_spinner = 'dots'
pn.config.disconnect_notification = False  # Disable default notification

# Add custom reconnection JavaScript
pn.config.raw_css.append("""
<script>
// Auto-reconnect WebSocket on disconnect
(function() {
    let reconnectAttempts = 0;
    const maxReconnects = 10;
    
    // Monitor Bokeh WebSocket
    if (window.Bokeh && window.Bokeh.documents) {
        const originalOnClose = WebSocket.prototype.onclose;
        
        WebSocket.prototype.onclose = function(event) {
            console.log('WebSocket closed:', event);
            
            // If this is a Bokeh WebSocket, try to reconnect
            if (this.url && this.url.includes('/ws')) {
                reconnectAttempts++;
                
                if (reconnectAttempts < maxReconnects) {
                    console.log(`Attempting reconnect ${reconnectAttempts}/${maxReconnects}...`);
                    
                    setTimeout(() => {
                        // Reload the page to reconnect
                        window.location.reload();
                    }, 5000 * reconnectAttempts); // Exponential backoff
                }
            }
            
            // Call original handler
            if (originalOnClose) {
                originalOnClose.call(this, event);
            }
        };
    }
})();
</script>
""")

# When starting the server, ensure WebSocket origins are configured
# panel serve your_app.py --allow-websocket-origin="*" --keep-alive 10000
'''

print("Server configuration for WebSocket:")
print(server_config)

print("\n## FIX 5: Cloudflare Tunnel Configuration")
print("-" * 60)

cloudflare_config = '''
# In your cloudflared config.yml:

tunnel: your-tunnel-id
credentials-file: /path/to/credentials.json

ingress:
  - hostname: nemgen.itkservices2.com
    service: http://localhost:5006
    originRequest:
      # Critical for WebSocket support
      noTLSVerify: true
      connectTimeout: 60s
      tcpKeepAlive: 30s
      keepAliveConnections: 100
      keepAliveTimeout: 90s
      httpHostHeader: localhost:5006
      originServerName: localhost
      # Disable buffering for real-time updates
      disableChunkedEncoding: false
      # WebSocket specific
      http2Origin: false  # WebSockets don't work with HTTP/2
      
  - service: http_status:404

# Also add these Cloudflare dashboard settings:
# 1. Page Rules for nemgen.itkservices2.com:
#    - Cache Level: Bypass
#    - Disable Performance features
# 2. Network settings:
#    - WebSockets: Enabled
#    - HTTP/2: Disabled for this subdomain
'''

print("Cloudflare tunnel configuration:")
print(cloudflare_config)

print("\n" + "=" * 80)
print("IMPLEMENTATION PRIORITY")
print("=" * 80)

print("""
1. IMMEDIATE: Add iframe refresh JavaScript to Quarto page (FIX 3)
   - This will work around the issue immediately
   
2. SHORT TERM: Update _force_component_refresh() on server (FIX 1)
   - Ensures WebSocket sync works properly
   
3. MEDIUM TERM: Add memory cleanup (FIX 2)
   - Prevents memory reloads that break connections
   
4. LONG TERM: Configure Cloudflare and server properly (FIX 4 & 5)
   - Ensures robust WebSocket handling

The iframe JavaScript refresh (FIX 3) is the quickest solution that will
work immediately without server changes. It detects the 23:55 freeze and
refreshes the iframe automatically.
""")

print("=" * 80)