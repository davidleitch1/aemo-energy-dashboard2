#!/bin/bash

# Deploy Renewable Energy Gauge Server for Production
# This script is designed to run on the production server

echo "=========================================="
echo "Deploying Renewable Energy Gauge Server"
echo "=========================================="

# Default values
PORT=5007
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --port)
            PORT="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [--port PORT]"
            echo "  --port PORT    Port to serve on (default: 5007)"
            echo ""
            echo "This script will:"
            echo "  1. Activate the virtual environment"
            echo "  2. Load settings from .env file"
            echo "  3. Start the gauge server on all interfaces (0.0.0.0)"
            echo "  4. Allow all websocket origins (for Cloudflare tunnel)"
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
if [ ! -f "standalone_renewable_gauge_fixed.py" ]; then
    echo "Error: standalone_renewable_gauge_fixed.py not found!"
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
    echo "Python packages may not be available."
fi

# Check if .env file exists
if [ -f ".env" ]; then
    echo "Found .env file in current directory"
elif [ -f "../.env" ]; then
    echo "Found .env file in parent directory"
else
    echo "Warning: No .env file found!"
    echo "The server will use default paths which may not be correct."
fi

# Show configuration
echo ""
echo "Configuration:"
echo "  Port: $PORT"
echo "  Host: 0.0.0.0 (all interfaces)"
echo "  Script: standalone_renewable_gauge_fixed.py"
echo ""

# Check configuration
echo "Checking configuration..."
python standalone_renewable_gauge_fixed.py --show-config
echo ""

# Create systemd service file (optional)
if [ "$1" == "--create-service" ]; then
    SERVICE_FILE="/etc/systemd/system/renewable-gauge.service"
    echo "Creating systemd service file..."
    
    cat > renewable-gauge.service.tmp << EOF
[Unit]
Description=AEMO Renewable Energy Gauge Server
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$SCRIPT_DIR
Environment="PATH=$SCRIPT_DIR/.venv/bin:/usr/local/bin:/usr/bin:/bin"
ExecStart=$SCRIPT_DIR/.venv/bin/python $SCRIPT_DIR/standalone_renewable_gauge_fixed.py --port $PORT
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
    
    echo "Service file created: renewable-gauge.service.tmp"
    echo "To install as a system service, run:"
    echo "  sudo mv renewable-gauge.service.tmp $SERVICE_FILE"
    echo "  sudo systemctl daemon-reload"
    echo "  sudo systemctl enable renewable-gauge"
    echo "  sudo systemctl start renewable-gauge"
    exit 0
fi

# Start the server
echo "Starting Renewable Energy Gauge Server..."
echo ""
echo "Server will be available at: http://localhost:$PORT"
echo "For Cloudflare tunnel, use your tunnel URL"
echo ""
echo "Press Ctrl+C to stop the server"
echo "=========================================="
echo ""

# Run the production server
# Note: Using exec to replace the shell process with Python
exec python standalone_renewable_gauge_fixed.py --port $PORT