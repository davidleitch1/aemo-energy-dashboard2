#!/bin/bash

# Dashboard restart script - kills and restarts dashboard at scheduled time
# This script should be run with cron to schedule daily restarts

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
LOG_FILE="$SCRIPT_DIR/dashboard_restart.log"
DASHBOARD_SCRIPT="run_dashboard_fixed.py"
PYTHON_PATH="$SCRIPT_DIR/.venv/bin/python"

# Function to log messages
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG_FILE"
}

# Start of restart process
log_message "Starting dashboard restart process"

# Find and kill existing dashboard process
PID=$(pgrep -f "$DASHBOARD_SCRIPT")

if [ ! -z "$PID" ]; then
    log_message "Found dashboard process with PID: $PID"
    kill -TERM $PID
    sleep 5
    
    # Force kill if still running
    if kill -0 $PID 2>/dev/null; then
        log_message "Process still running, force killing..."
        kill -KILL $PID
        sleep 2
    fi
    log_message "Dashboard process killed successfully"
else
    log_message "No existing dashboard process found"
fi

# Clear any orphaned Panel/Bokeh server processes
pkill -f "bokeh.server"
pkill -f "panel serve"

# Wait a moment for ports to be released
sleep 3

# Change to correct directory
cd "$SCRIPT_DIR"

# Start new dashboard process
log_message "Starting new dashboard process"
nohup $PYTHON_PATH $DASHBOARD_SCRIPT > dashboard_output.log 2>&1 &
NEW_PID=$!

# Verify the process started
sleep 5
if kill -0 $NEW_PID 2>/dev/null; then
    log_message "Dashboard restarted successfully with PID: $NEW_PID"
else
    log_message "ERROR: Failed to start dashboard"
fi

log_message "Restart process completed"
log_message "----------------------------------------"