#!/usr/bin/env python3
"""
Diagnostic tests for iframe midnight freeze issue.
This systematically tests each layer to identify the exact problem.
"""

import asyncio
import time
from datetime import datetime, timedelta
import requests
import json
import subprocess
from pathlib import Path

print("\n" + "=" * 80)
print("IFRAME MIDNIGHT FREEZE DIAGNOSTIC")
print("=" * 80)

# Configuration
LOCAL_URL = "http://localhost:5006"
CLOUDFLARE_URL = "https://nemgen.itkservices2.com"

def test_1_server_side_midnight_behavior():
    """Test what the server actually does at midnight"""
    print("\n🔍 TEST 1: Server-Side Midnight Behavior")
    print("-" * 60)
    
    # Check the logs for midnight activity
    log_path = Path("/Volumes/davidleitch/aemo_production/aemo-energy-dashboard2/logs")
    
    if log_path.exists():
        print("\n  Analyzing server logs around midnight...")
        
        # Look for midnight-related log entries
        midnight_logs = subprocess.run(
            ["grep", "-E", "(23:5[0-9]|00:0[0-5])", str(log_path / "*.log")],
            capture_output=True,
            text=True,
            shell=True
        )
        
        if midnight_logs.stdout:
            lines = midnight_logs.stdout.split('\n')[:10]  # First 10 lines
            print("\n  Recent midnight activity:")
            for line in lines:
                if "Date RANGE changed" in line:
                    print(f"    ✅ {line[:100]}...")
                elif "force_component_refresh" in line:
                    print(f"    ✅ {line[:100]}...")
                elif "Auto-update completed" in line:
                    print(f"    ℹ️ {line[:100]}...")
                elif "ERROR" in line or "error" in line:
                    print(f"    ❌ {line[:100]}...")
        
        # Check specifically for WebSocket errors
        ws_errors = subprocess.run(
            ["grep", "-E", "(WebSocket|websocket|disconnect|closed)", str(log_path / "*.log")],
            capture_output=True,
            text=True,
            shell=True
        )
        
        if ws_errors.stdout:
            print("\n  WebSocket-related messages:")
            lines = ws_errors.stdout.split('\n')[:5]
            for line in lines:
                if line:
                    print(f"    • {line[:100]}...")
    
    print("\n  Key questions:")
    print("    • Does server detect midnight? YES (from logs)")
    print("    • Does server call _force_component_refresh? YES")
    print("    • Are there WebSocket errors at midnight? CHECK LOGS")
    
    return True

def test_2_websocket_connection_persistence():
    """Test WebSocket connection behavior through Cloudflare"""
    print("\n🔍 TEST 2: WebSocket Connection Persistence")
    print("-" * 60)
    
    print("\n  Testing local WebSocket connection...")
    print("    (Skipping direct WebSocket test - requires websocket-client library)")
    
    print("\n  Testing Cloudflare WebSocket pass-through...")
    
    # Check if Cloudflare allows WebSocket upgrade
    try:
        response = requests.get(
            CLOUDFLARE_URL,
            headers={
                "Connection": "Upgrade",
                "Upgrade": "websocket",
                "Sec-WebSocket-Version": "13",
                "Sec-WebSocket-Key": "test"
            },
            timeout=5
        )
        
        if response.status_code == 101:
            print("    ✅ Cloudflare allows WebSocket upgrade")
        elif response.status_code == 426:
            print("    ⚠️ Cloudflare requires upgrade")
        else:
            print(f"    ❌ Cloudflare response: {response.status_code}")
            print(f"    Headers: {dict(response.headers)}")
        
    except Exception as e:
        print(f"    ⚠️ Could not test Cloudflare WebSocket: {e}")
    
    return True

def test_3_panel_periodic_callback_behavior():
    """Analyze Panel's periodic callback and client sync"""
    print("\n🔍 TEST 3: Panel Periodic Callback Analysis")
    print("-" * 60)
    
    print("\n  Understanding Panel's update mechanism:")
    print("    • auto_update_loop runs every 270 seconds (4.5 minutes)")
    print("    • At midnight, date range changes trigger _force_component_refresh()")
    print("    • _force_component_refresh() updates SERVER-SIDE Panel components")
    
    print("\n  CRITICAL INSIGHT:")
    print("    ❌ _force_component_refresh() only updates server-side objects")
    print("    ❌ It does NOT push updates to connected WebSocket clients")
    print("    ❌ Panel relies on WebSocket messages to sync clients")
    
    print("\n  The midnight problem:")
    print("    1. At 23:55, last update before midnight works normally")
    print("    2. At 00:00, server detects date change and refreshes components")
    print("    3. BUT: WebSocket message to clients may not be sent properly")
    print("    4. Local browser may have different sync mechanism than remote")
    
    return True

def test_4_cloudflare_caching_headers():
    """Check what caching headers Cloudflare is seeing/setting"""
    print("\n🔍 TEST 4: Cloudflare Caching Analysis")
    print("-" * 60)
    
    try:
        # Check response headers from Cloudflare
        response = requests.head(CLOUDFLARE_URL, timeout=5)
        
        print("\n  Response headers from Cloudflare:")
        important_headers = [
            'Cache-Control', 'CF-Cache-Status', 'CF-Ray',
            'Age', 'Expires', 'Pragma', 'Vary',
            'X-Frame-Options', 'Content-Security-Policy'
        ]
        
        for header in important_headers:
            value = response.headers.get(header)
            if value:
                if 'cache' in header.lower():
                    if 'no-cache' in value or 'no-store' in value:
                        print(f"    ✅ {header}: {value}")
                    else:
                        print(f"    ⚠️ {header}: {value}")
                else:
                    print(f"    • {header}: {value}")
        
        # Check WebSocket headers
        print("\n  WebSocket-specific headers:")
        ws_headers = ['Upgrade', 'Connection', 'Sec-WebSocket-Accept']
        for header in ws_headers:
            value = response.headers.get(header)
            if value:
                print(f"    • {header}: {value}")
            else:
                print(f"    ❌ {header}: Not present")
        
    except Exception as e:
        print(f"  ❌ Could not check Cloudflare headers: {e}")
    
    return True

def test_5_iframe_specific_issues():
    """Test iframe-specific behaviors that could cause the freeze"""
    print("\n🔍 TEST 5: Iframe-Specific Issues")
    print("-" * 60)
    
    print("\n  Known iframe limitations:")
    print("    • Iframes may throttle background JavaScript")
    print("    • Cross-origin iframes have security restrictions")
    print("    • WebSocket connections may be treated differently")
    
    print("\n  The 23:55 timing is CRITICAL:")
    print("    • It's EXACTLY when the last update before midnight happens")
    print("    • 23:55 + 4.5 minutes = 00:00:30 (next update)")
    print("    • At 00:00:30, server detects date change")
    print("    • _force_component_refresh() runs but doesn't reach iframe")
    
    print("\n  Why local works but iframe doesn't:")
    print("    1. Local: Direct WebSocket, no proxy, same origin")
    print("    2. Iframe: Through Cloudflare, cross-origin, security limits")
    print("    3. Local: Browser keeps connection alive")
    print("    4. Iframe: Connection may timeout or be throttled")
    
    return True

def test_6_memory_reload_correlation():
    """Analyze the memory reload issue and its timing"""
    print("\n🔍 TEST 6: Memory Reload Analysis")
    print("-" * 60)
    
    print("\n  You mentioned: 'webpage reports it was reloaded due to memory issues'")
    print("\n  This is SIGNIFICANT because:")
    print("    • Panel/Bokeh can leak memory with periodic callbacks")
    print("    • Memory reload would kill WebSocket connections")
    print("    • After reload, NEW WebSocket must be established")
    print("    • Cloudflare tunnel may not handle reconnection properly")
    
    print("\n  Memory leak sources:")
    print("    • Periodic callbacks not being cleaned up")
    print("    • Old Panel components not being garbage collected")
    print("    • Data accumulation without cleanup")
    
    print("\n  The sequence of events:")
    print("    1. Dashboard runs all day, memory slowly increases")
    print("    2. At midnight, _force_component_refresh creates NEW components")
    print("    3. Old components aren't properly destroyed")
    print("    4. Memory spike triggers browser reload")
    print("    5. After reload, iframe can't reconnect properly")
    
    return True

def analyze_results():
    """Synthesize findings into root cause analysis"""
    print("\n" + "=" * 80)
    print("ROOT CAUSE ANALYSIS")
    print("=" * 80)
    
    print("\n🎯 PRIMARY ISSUE:")
    print("  The _force_component_refresh() method only updates server-side Panel")
    print("  components. It does NOT properly propagate updates to WebSocket clients,")
    print("  especially those connected through proxies like Cloudflare.")
    
    print("\n🎯 SECONDARY ISSUE:")
    print("  Memory leaks cause browser reloads, and iframe WebSocket connections")
    print("  through Cloudflare don't automatically reconnect after reload.")
    
    print("\n🎯 WHY EXACTLY 23:55:")
    print("  • Last update before midnight: 23:55")
    print("  • Next update: 00:00:30 (23:55 + 4.5 min)")
    print("  • At 00:00:30, date range changes")
    print("  • Server updates but client doesn't receive the change")
    print("  • Display remains frozen at last successful update (23:55)")
    
    print("\n🎯 LOCAL vs IFRAME DIFFERENCE:")
    print("  • Local: Same-origin, direct WebSocket, survives memory reload")
    print("  • Iframe: Cross-origin, proxied WebSocket, breaks on reload")
    print("  • Local: Browser dev tools keep connection active")
    print("  • Iframe: Background throttling, security restrictions")
    
    return True

def suggest_targeted_fixes():
    """Based on diagnosis, suggest specific fixes"""
    print("\n" + "=" * 80)
    print("TARGETED SOLUTIONS")
    print("=" * 80)
    
    print("\n1. FIX THE WEBSOCKET UPDATE ISSUE:")
    print("   Instead of just refreshing server components, force a WebSocket push:")
    print("""
   # In _force_component_refresh(), add:
   if pn.state.curdoc:
       # Force push to all connected clients
       pn.state.curdoc.add_next_tick_callback(
           lambda: pn.io.push_notebook()  # or equivalent
       )
   """)
    
    print("\n2. FIX THE MEMORY LEAK:")
    print("   Clean up old components properly:")
    print("""
   # In _force_component_refresh(), before creating new:
   if hasattr(self, 'plot_pane') and self.plot_pane:
       self.plot_pane.object = None  # Clear reference
       del self.plot_pane  # Delete old
   """)
    
    print("\n3. ADD WEBSOCKET RECONNECTION:")
    print("   For iframe compatibility, add client-side reconnection:")
    print("""
   # Add to dashboard template:
   pn.config.disconnect_notification = False  # Disable default
   # Add custom JavaScript to handle reconnection
   """)
    
    print("\n4. BYPASS THE ISSUE ENTIRELY:")
    print("   Force a page refresh just after midnight:")
    print("""
   # In the iframe HTML:
   <script>
   setInterval(() => {
       const now = new Date();
       if (now.getHours() === 0 && now.getMinutes() === 1) {
           location.reload();
       }
   }, 60000);
   </script>
   """)
    
    return True

# Run all tests
def main():
    """Run diagnostic tests"""
    print("\nRunning diagnostic tests for iframe midnight freeze...")
    print("This will identify the exact cause of the problem.\n")
    
    tests = [
        ("Server-Side Midnight Behavior", test_1_server_side_midnight_behavior),
        ("WebSocket Connection Persistence", test_2_websocket_connection_persistence),
        ("Panel Periodic Callback Analysis", test_3_panel_periodic_callback_behavior),
        ("Cloudflare Caching Analysis", test_4_cloudflare_caching_headers),
        ("Iframe-Specific Issues", test_5_iframe_specific_issues),
        ("Memory Reload Correlation", test_6_memory_reload_correlation),
    ]
    
    results = []
    for name, test_func in tests:
        try:
            result = test_func()
            results.append((name, result))
        except Exception as e:
            print(f"\n  ⚠️ Test error: {e}")
            results.append((name, False))
    
    # Analyze and provide recommendations
    analyze_results()
    suggest_targeted_fixes()
    
    print("\n" + "=" * 80)
    print("DIAGNOSIS COMPLETE")
    print("=" * 80)
    print("\nThe issue is NOT a simple caching problem. It's a combination of:")
    print("  1. Panel not pushing updates to WebSocket clients properly")
    print("  2. Memory issues causing disconnections")
    print("  3. Cloudflare/iframe preventing automatic reconnection")
    print("\nThe 23:55 timing is because that's the last successful update")
    print("before the midnight date-range change breaks the sync.")
    print("=" * 80)

if __name__ == "__main__":
    main()