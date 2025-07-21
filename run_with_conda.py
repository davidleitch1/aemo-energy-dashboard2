#!/usr/bin/env python3
"""
Run the dashboard using the current Python environment (Anaconda)
This avoids virtual environment issues on macOS with iCloud
"""

import sys
import os

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Import and run the dashboard
from aemo_dashboard.generation import gen_dash

if __name__ == "__main__":
    gen_dash.main()