#!/bin/bash
# Test WebSocket keepalive implementation
# This script monitors both the dashboard and Cloudflare tunnel to verify
# that the WebSocket connection stays alive past Cloudflare's 100-second timeout

echo "==================================================================="
echo "WebSocket Keepalive Test"
echo "==================================================================="
echo "This test will:"
echo "  1. Monitor dashboard logs for keepalive messages (every 60s)"
echo "  2. Monitor Cloudflare tunnel logs for timeout warnings"
echo "  3. Run for 5 minutes to verify connection stays alive"
echo ""
echo "Expected behavior:"
echo "  ✓ Keepalive messages appear every 60 seconds in dashboard log"
echo "  ✓ NO 'timeout: no recent network activity' in tunnel log"
echo "  ✓ Connection stays alive past 100-second Cloudflare timeout"
echo "==================================================================="
echo ""

# Set up log file paths
DASHBOARD_LOG="/Users/davidleitch/aemo_production/aemo-energy-dashboard2/logs/aemo_dashboard.log"
TEST_DURATION=300  # 5 minutes

echo "Starting test at $(date)"
echo "Test duration: ${TEST_DURATION} seconds (5 minutes)"
echo ""

# Function to monitor dashboard logs
monitor_dashboard() {
    echo "--- Dashboard Keepalive Messages ---"
    ssh davidleitch@192.168.68.71 "tail -f ${DASHBOARD_LOG} 2>/dev/null | grep -i 'keepalive' &"
    DASHBOARD_PID=$!
}

# Function to monitor Cloudflare tunnel logs
monitor_tunnel() {
    echo "--- Cloudflare Tunnel Warnings (looking for timeouts) ---"
    ssh davidleitch@192.168.68.71 "/Applications/Docker.app/Contents/Resources/bin/docker logs -f nostalgic_faraday 2>&1 | grep -E 'WRN.*timeout|Connection terminated' &"
    TUNNEL_PID=$!
}

# Start monitoring
monitor_dashboard
monitor_tunnel

# Wait for test duration
echo ""
echo "Monitoring for ${TEST_DURATION} seconds..."
echo "Press Ctrl+C to stop early"
echo ""

sleep $TEST_DURATION

# Cleanup
echo ""
echo "==================================================================="
echo "Test completed at $(date)"
echo "==================================================================="
echo ""
echo "Analysis:"
echo "  1. Check above for keepalive messages every ~60 seconds"
echo "  2. If NO timeout warnings appeared, the fix is working!"
echo "  3. If timeout warnings still appear, we may need to adjust timing"
echo ""

kill $DASHBOARD_PID 2>/dev/null
kill $TUNNEL_PID 2>/dev/null
