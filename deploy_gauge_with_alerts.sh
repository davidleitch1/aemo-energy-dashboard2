#!/bin/bash

# Deploy Renewable Energy Gauge Server with SMS Alerts
# This version includes individual fuel tracking and Twilio alerts

echo "=========================================="
echo "Deploying Renewable Gauge with SMS Alerts"
echo "=========================================="

# Default values
PORT=5009
ENV_FILE=""
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --port)
            PORT="$2"
            shift 2
            ;;
        --env-file)
            ENV_FILE="$2"
            shift 2
            ;;
        --test)
            echo "Testing renewable data loading and alert system..."
            cd "$SCRIPT_DIR"
            source .venv/bin/activate 2>/dev/null || source ../.venv/bin/activate 2>/dev/null
            if [ ! -z "$ENV_FILE" ]; then
                python standalone_renewable_gauge_with_alerts.py --env-file "$ENV_FILE" --test
            else
                python standalone_renewable_gauge_with_alerts.py --test
            fi
            exit 0
            ;;
        --test-alerts)
            echo "Testing alert system with mock data..."
            echo "WARNING: This will send real SMS messages!"
            read -p "Continue? (y/N) " -n 1 -r
            echo
            if [[ $REPLY =~ ^[Yy]$ ]]; then
                cd "$SCRIPT_DIR"
                source .venv/bin/activate 2>/dev/null || source ../.venv/bin/activate 2>/dev/null
                if [ ! -z "$ENV_FILE" ]; then
                    python standalone_renewable_gauge_with_alerts.py --env-file "$ENV_FILE" --test-alerts
                else
                    python standalone_renewable_gauge_with_alerts.py --test-alerts
                fi
            fi
            exit 0
            ;;
        --help)
            echo "Usage: $0 [--port PORT] [--env-file PATH] [--test] [--test-alerts]"
            echo "  --port PORT       Port to serve on (default: 5009)"
            echo "  --env-file PATH   Path to .env file to use"
            echo "  --test            Test data loading and exit"
            echo "  --test-alerts     Test alert system (SENDS REAL SMS!)"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Change to script directory
cd "$SCRIPT_DIR"

# Check if we're in the right directory
if [ ! -f "standalone_renewable_gauge_with_alerts.py" ]; then
    echo "Error: standalone_renewable_gauge_with_alerts.py not found!"
    echo "Please run this script from the aemo-energy-dashboard directory"
    exit 1
fi

# Activate virtual environment
if [ -d ".venv" ]; then
    echo "Activating virtual environment..."
    source .venv/bin/activate
elif [ -d "../.venv" ]; then
    echo "Activating virtual environment from parent directory..."
    source ../.venv/bin/activate
else
    echo "Warning: No virtual environment found!"
fi

# Check for Twilio configuration
echo ""
echo "Checking configuration..."
if [ -f ".env" ] || [ ! -z "$ENV_FILE" ]; then
    # Check for required environment variables
    if [ ! -z "$ENV_FILE" ]; then
        source "$ENV_FILE"
    else
        source .env
    fi
    
    if [ -z "$TWILIO_ACCOUNT_SID" ] || [ -z "$TWILIO_AUTH_TOKEN" ]; then
        echo "Warning: Twilio credentials not found - SMS alerts will be disabled"
    else
        echo "✓ Twilio credentials found"
    fi
    
    if [ -z "$TWILIO_FROM_NUMBER" ] || [ -z "$ALERT_PHONE_NUMBER" ]; then
        echo "Warning: Phone numbers not configured - SMS alerts will be disabled"
    else
        echo "✓ Phone numbers configured"
        echo "  From: $TWILIO_FROM_NUMBER"
        echo "  To: $ALERT_PHONE_NUMBER"
    fi
else
    echo "Warning: No .env file found - SMS alerts will be disabled"
fi

echo ""
echo "Configuration:"
echo "  Port: $PORT"
echo "  Host: 0.0.0.0 (all interfaces)"
echo "  Script: standalone_renewable_gauge_with_alerts.py"
if [ ! -z "$ENV_FILE" ]; then
    echo "  Env file: $ENV_FILE"
fi
echo ""
echo "Tracking records for:"
echo "  • Overall renewable percentage"
echo "  • Wind generation (MW)"
echo "  • Solar generation (MW)"
echo "  • Rooftop solar (MW)"
echo "  • Hydro generation (MW)"
echo ""

# Start the server
echo "Starting Renewable Gauge Server with SMS Alerts..."
echo ""
echo "Server will be available at: http://localhost:$PORT"
echo "Public URL: https://gauge.itkservices2.com"
echo ""
echo "To embed in your webpage:"
echo "<iframe src='https://gauge.itkservices2.com' width='450' height='400' frameborder='0'></iframe>"
echo ""
echo "SMS alerts will be sent when new records are set."
echo "Press Ctrl+C to stop the server"
echo "=========================================="
echo ""

# Run the enhanced server
if [ ! -z "$ENV_FILE" ]; then
    # Convert to absolute path if relative
    if [[ ! "$ENV_FILE" = /* ]]; then
        ENV_FILE="$SCRIPT_DIR/$ENV_FILE"
    fi
    exec python standalone_renewable_gauge_with_alerts.py --port $PORT --env-file "$ENV_FILE"
else
    exec python standalone_renewable_gauge_with_alerts.py --port $PORT
fi