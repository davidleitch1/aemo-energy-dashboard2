#!/bin/bash

# Setup script to add dashboard restart to crontab
# Run this once to schedule the daily restart at 11:55 PM

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
RESTART_SCRIPT="$SCRIPT_DIR/restart_dashboard_daily.sh"

echo "======================================"
echo "Dashboard Daily Restart Setup"
echo "======================================"
echo ""

# Make the restart script executable
chmod +x "$RESTART_SCRIPT"
echo "✓ Made restart script executable"

# Check if cron entry already exists
if crontab -l 2>/dev/null | grep -q "$RESTART_SCRIPT"; then
    echo ""
    echo "⚠️  Cron job already exists for dashboard restart"
    echo "Current cron entry:"
    crontab -l | grep "$RESTART_SCRIPT"
    echo ""
    echo "To remove it, run: crontab -e"
    echo "Then delete the line containing: $RESTART_SCRIPT"
else
    # Add new cron entry
    # 55 23 * * * = Run at 11:55 PM every day
    (crontab -l 2>/dev/null; echo "55 23 * * * $RESTART_SCRIPT") | crontab -
    echo "✓ Cron job added successfully!"
    echo ""
    echo "📅 Dashboard will restart daily at 11:55 PM"
fi

echo ""
echo "======================================"
echo "Useful Commands:"
echo "======================================"
echo "View scheduled jobs:     crontab -l"
echo "Edit cron jobs:          crontab -e"
echo "Remove ALL cron jobs:    crontab -r  (careful!)"
echo "Test restart manually:   $RESTART_SCRIPT"
echo "Watch the log file:      tail -f $SCRIPT_DIR/dashboard_restart.log"
echo ""
echo "======================================"
echo "Cron Schedule Format:"
echo "======================================"
echo "55 23 * * * means:"
echo "  55 = Minute (0-59)"
echo "  23 = Hour (0-23, so 23 = 11 PM)"
echo "  *  = Every day of month"
echo "  *  = Every month"
echo "  *  = Every day of week"
echo ""
echo "To change the time, edit with 'crontab -e'"
echo "======================================"