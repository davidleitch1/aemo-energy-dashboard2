#!/bin/bash
# Production start script for AEMO Energy Dashboard with retry logic

echo "Starting AEMO Energy Dashboard with retry logic..."
echo "This version handles concurrent file access gracefully"
echo ""

# Navigate to dashboard directory
cd "$(dirname "$0")"

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo "Error: Virtual environment not found!"
    echo "Please run setup script first or create .venv manually"
    exit 1
fi

# Activate virtual environment
source .venv/bin/activate

# Check if .env exists
if [ ! -f ".env" ]; then
    echo "Error: .env file not found!"
    echo "Please copy .env.example to .env and configure it"
    exit 1
fi

# Find and stop old dashboard if running
OLD_PIDS=$(ps aux | grep -E "run_dashboard|gen_dash\.py" | grep -v grep | awk '{print $2}')
if [ ! -z "$OLD_PIDS" ]; then
    echo "Found existing dashboard processes:"
    echo "$OLD_PIDS"
    echo "Stopping old dashboard instances..."
    for pid in $OLD_PIDS; do
        kill -TERM $pid 2>/dev/null
    done
    sleep 2
    
    # Force kill if still running
    for pid in $OLD_PIDS; do
        if ps -p $pid > /dev/null 2>&1; then
            kill -9 $pid 2>/dev/null
        fi
    done
fi

# Get port from .env or use default
PORT=$(grep "DASHBOARD_PORT=" .env | cut -d'=' -f2)
if [ -z "$PORT" ]; then
    PORT=5008
fi

# Start the dashboard with retry logic
echo ""
echo "Starting dashboard on port $PORT..."
echo "Local URL: http://localhost:$PORT"
echo "Remote URL: https://nemgen.itkservices2.com"
echo ""
echo "Dashboard includes automatic retry logic for file access"
echo "This prevents hangs when data files are being updated"
echo ""
echo "Press Ctrl+C to stop"
echo ""

# Run the dashboard with initialization fix
python run_dashboard_fixed.py