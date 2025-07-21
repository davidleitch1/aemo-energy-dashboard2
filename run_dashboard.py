#!/usr/bin/env python3
"""
Simple wrapper to run the new dashboard with system Python
This allows running with: python3 run_dashboard.py
Similar to the old: python3 gen_dash.py
"""

import subprocess
import sys
import os
from pathlib import Path

# Get the directory where this script is located
SCRIPT_DIR = Path(__file__).parent
VENV_PYTHON = SCRIPT_DIR / ".venv" / "bin" / "python"

# Check if virtual environment exists
if not VENV_PYTHON.exists():
    print("Error: Virtual environment not found!")
    print("Please run: ./deploy_production.sh first")
    sys.exit(1)

# Change to the dashboard directory
os.chdir(SCRIPT_DIR)

# Run the dashboard using the virtual environment Python
cmd = [str(VENV_PYTHON), "-m", "src.aemo_dashboard.generation.gen_dash"] + sys.argv[1:]

print(f"Starting AEMO Energy Dashboard...")
print(f"Using Python: {VENV_PYTHON}")

try:
    subprocess.run(cmd)
except KeyboardInterrupt:
    print("\nDashboard stopped.")