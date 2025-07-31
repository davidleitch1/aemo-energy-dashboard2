#!/bin/bash

# Start the Renewable Energy Gauge Server

echo "Starting Renewable Energy Gauge Server..."
echo "========================================"

# Default values
PORT=5007
HOST=localhost

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --port)
            PORT="$2"
            shift 2
            ;;
        --host)
            HOST="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [--port PORT] [--host HOST]"
            echo "  --port PORT    Port to serve on (default: 5007)"
            echo "  --host HOST    Host to serve on (default: localhost)"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Activate virtual environment if it exists
if [ -d ".venv" ]; then
    echo "Activating virtual environment..."
    source .venv/bin/activate
fi

# Start the server with common allowed origins
echo "Starting server on http://$HOST:$PORT"
echo ""
echo "To embed in your website, use:"
echo "<iframe src='http://$HOST:$PORT' width='450' height='400'></iframe>"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

# Run the server with common allowed origins
python standalone_renewable_gauge.py \
    --port $PORT \
    --host $HOST \
    --allow-websocket-origin \
        localhost:8000 \
        localhost:8080 \
        localhost:3000 \
        127.0.0.1:8000 \
        127.0.0.1:8080 \
        127.0.0.1:3000 \
        $HOST:$PORT