#!/bin/bash
# Simple production start script for AEMO Energy Dashboard

echo "Starting AEMO Energy Dashboard..."

# Navigate to dashboard directory
cd "$(dirname "$0")"

# Check if we're already in a conda environment and deactivate it
if [ ! -z "$CONDA_DEFAULT_ENV" ]; then
    echo "Deactivating conda environment..."
    conda deactivate
fi

# Remove old venv if it exists and create fresh one
if [ -d ".venv" ]; then
    echo "Removing old virtual environment..."
    rm -rf .venv
fi

echo "Creating fresh virtual environment..."
/usr/bin/python3 -m venv .venv

# Activate virtual environment
source .venv/bin/activate

# Upgrade pip
echo "Upgrading pip..."
python -m pip install --upgrade pip

# Install dependencies
echo "Installing dependencies..."
pip install -e .

# Check if .env exists
if [ ! -f ".env" ]; then
    echo "Creating .env file from template..."
    cp .env.example .env
    echo ""
    echo "⚠️  IMPORTANT: Edit .env file to add your credentials!"
    echo "Press Enter when you've updated .env..."
    read
fi

# Ensure we're using port 5008
if ! grep -q "DASHBOARD_PORT=5008" .env; then
    echo "Setting DASHBOARD_PORT=5008 in .env..."
    echo "DASHBOARD_PORT=5008" >> .env
fi

# Find and stop old dashboard if running
OLD_PID=$(ps aux | grep "[g]enhist/gen_dash.py" | awk '{print $2}')
if [ ! -z "$OLD_PID" ]; then
    echo "Found old dashboard running with PID: $OLD_PID"
    echo "Stopping old dashboard..."
    kill -9 $OLD_PID
    sleep 2
fi

# Start the dashboard
echo ""
echo "Starting dashboard on port 5008..."
echo "Local URL: http://localhost:5008"
echo "Remote URL: https://nemgen.itkservices2.com"
echo ""
echo "Press Ctrl+C to stop"
echo ""

python -m src.aemo_dashboard.generation.gen_dash