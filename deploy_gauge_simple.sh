#!/bin/bash

# Deploy Simple Renewable Energy Gauge Server
# This version reads directly from parquet files

echo "=========================================="
echo "Deploying Simple Renewable Gauge Server"
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
            echo "Testing renewable data loading..."
            cd "$SCRIPT_DIR"
            source .venv/bin/activate 2>/dev/null || source ../.venv/bin/activate 2>/dev/null
            if [ ! -z "$ENV_FILE" ]; then
                python standalone_renewable_gauge_simple.py --env-file "$ENV_FILE" --test
            else
                python standalone_renewable_gauge_simple.py --test
            fi
            exit 0
            ;;
        --help)
            echo "Usage: $0 [--port PORT] [--env-file PATH] [--test]"
            echo "  --port PORT       Port to serve on (default: 5009)"
            echo "  --env-file PATH   Path to .env file to use"
            echo "  --test            Test data loading and exit"
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
if [ ! -f "standalone_renewable_gauge_simple.py" ]; then
    echo "Error: standalone_renewable_gauge_simple.py not found!"
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

echo ""
echo "Configuration:"
echo "  Port: $PORT"
echo "  Host: 0.0.0.0 (all interfaces)"
echo "  Script: standalone_renewable_gauge_simple.py"
if [ ! -z "$ENV_FILE" ]; then
    echo "  Env file: $ENV_FILE"
fi
echo ""

# Start the server
echo "Starting Simple Renewable Gauge Server..."
echo ""
echo "Server will be available at: http://localhost:$PORT"
echo "This serves ONLY the gauge - no Panel UI elements"
echo ""
echo "To embed in your webpage:"
echo "<iframe src='http://localhost:$PORT' width='450' height='400' frameborder='0'></iframe>"
echo ""
echo "Press Ctrl+C to stop the server"
echo "=========================================="
echo ""

# Run the simple server
if [ ! -z "$ENV_FILE" ]; then
    # Convert to absolute path if relative
    if [[ ! "$ENV_FILE" = /* ]]; then
        ENV_FILE="$SCRIPT_DIR/$ENV_FILE"
    fi
    exec python standalone_renewable_gauge_simple.py --port $PORT --env-file "$ENV_FILE"
else
    exec python standalone_renewable_gauge_simple.py --port $PORT
fi