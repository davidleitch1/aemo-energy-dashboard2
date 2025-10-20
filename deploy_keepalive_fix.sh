#!/bin/bash
# Deploy WebSocket keepalive fix to production dashboard

echo "==================================================================="
echo "Deploying WebSocket Keepalive Fix"
echo "==================================================================="
echo ""

# Stop current dashboard
echo "Step 1: Stopping current dashboard..."
ssh davidleitch@192.168.68.71 "pkill -f 'run_dashboard_duckdb.py'" && echo "  ✓ Dashboard stopped" || echo "  ℹ No dashboard process found"
sleep 2

# Start new dashboard with keepalive
echo ""
echo "Step 2: Starting dashboard with WebSocket keepalive..."
ssh davidleitch@192.168.68.71 "cd /Users/davidleitch/aemo_production/aemo-energy-dashboard2 && nohup /Users/davidleitch/anaconda3/bin/python run_dashboard_duckdb.py > dashboard_output.log 2>&1 &"
sleep 5

# Check if dashboard started
echo ""
echo "Step 3: Verifying dashboard is running..."
if ssh davidleitch@192.168.68.71 "ps aux | grep -v grep | grep run_dashboard_duckdb.py" > /dev/null; then
    echo "  ✓ Dashboard is running"
    echo ""
    echo "==================================================================="
    echo "Deployment successful!"
    echo "==================================================================="
    echo ""
    echo "Dashboard should now be available at:"
    echo "  - Local: http://localhost:5008"
    echo "  - Public: https://nemgen.itkservices2.com"
    echo ""
    echo "Next steps:"
    echo "  1. Run the test script to verify keepalive is working:"
    echo "     ./test_websocket_keepalive.sh"
    echo ""
    echo "  2. Monitor logs for keepalive messages:"
    echo "     ssh davidleitch@192.168.68.71 'tail -f /Users/davidleitch/aemo_production/aemo-energy-dashboard2/logs/aemo_dashboard.log | grep keepalive'"
    echo ""
    echo "  3. Check for Cloudflare timeout warnings (should see NONE):"
    echo "     ssh davidleitch@192.168.68.71 '/Applications/Docker.app/Contents/Resources/bin/docker logs -f nostalgic_faraday 2>&1 | grep timeout'"
    echo ""
else
    echo "  ✗ ERROR: Dashboard failed to start"
    echo ""
    echo "Check the logs:"
    echo "  ssh davidleitch@192.168.68.71 'cat /Users/davidleitch/aemo_production/aemo-energy-dashboard2/dashboard_output.log'"
    exit 1
fi
