#!/bin/bash
# Production deployment script for AEMO Energy Dashboard
# This script handles the transition from the old dashboard to the new one

set -e  # Exit on error

echo "AEMO Energy Dashboard - Production Deployment Script"
echo "===================================================="

# Check if we're in the right directory
if [ ! -f "pyproject.toml" ]; then
    echo "Error: This script must be run from the aemo-energy-dashboard directory"
    exit 1
fi

# Step 1: Find and stop the old dashboard
echo -e "\n1. Checking for running old dashboard..."
OLD_PID=$(ps aux | grep "[g]enhist/gen_dash.py" | awk '{print $2}')
if [ ! -z "$OLD_PID" ]; then
    echo "Found old dashboard running with PID: $OLD_PID"
    read -p "Stop the old dashboard? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        kill -9 $OLD_PID
        echo "Old dashboard stopped."
        sleep 2
    else
        echo "Keeping old dashboard running. Note: Both can't use port 5008!"
    fi
else
    echo "No old dashboard process found."
fi

# Step 2: Check Python environment
echo -e "\n2. Setting up Python environment..."
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
else
    echo "Virtual environment already exists."
fi

# Activate virtual environment
source .venv/bin/activate

# Step 3: Install/update dependencies
echo -e "\n3. Installing dependencies..."
pip install --upgrade pip
pip install -e .

# Step 4: Configure environment
echo -e "\n4. Configuring environment..."
if [ ! -f ".env" ]; then
    echo "Creating .env file from template..."
    cp .env.example .env
    echo "IMPORTANT: Edit .env file to add your credentials!"
    echo "Press Enter after you've updated .env..."
    read
else
    echo ".env file already exists."
fi

# Ensure we're using production port
if ! grep -q "DASHBOARD_PORT=5008" .env; then
    echo "Setting DASHBOARD_PORT=5008 in .env..."
    echo "DASHBOARD_PORT=5008" >> .env
fi

# Step 5: Verify installation
echo -e "\n5. Verifying installation..."
python -c "import src.aemo_dashboard.generation.gen_dash; print('✅ Dashboard module loaded successfully')" || {
    echo "Failed to import dashboard module. Please check installation."
    exit 1
}

# Step 6: Launch options
echo -e "\n6. Launch options:"
echo "a) Run in foreground (for testing)"
echo "b) Run in screen session (recommended for production)"
echo "c) Exit without starting"
read -p "Choose option (a/b/c): " -n 1 -r
echo

case $REPLY in
    a)
        echo -e "\nStarting dashboard in foreground..."
        echo "Press Ctrl+C to stop"
        .venv/bin/python -m src.aemo_dashboard.generation.gen_dash
        ;;
    b)
        echo -e "\nStarting dashboard in screen session..."
        screen -dmS aemo-dashboard bash -c "
            cd '$PWD'
            source .venv/bin/activate
            .venv/bin/python -m src.aemo_dashboard.generation.gen_dash
        "
        echo "Dashboard started in screen session 'aemo-dashboard'"
        echo "Use 'screen -r aemo-dashboard' to attach"
        echo "Use Ctrl+A, D to detach"
        ;;
    c)
        echo "Exiting without starting dashboard."
        ;;
    *)
        echo "Invalid option."
        ;;
esac

echo -e "\nDeployment complete!"
echo "Dashboard URL: http://localhost:5008"
echo "Remote URL: https://nemgen.itkservices2.com (via Cloudflare tunnel)"