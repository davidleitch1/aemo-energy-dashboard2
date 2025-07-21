#!/bin/bash
# Script to create a clean copy of the dashboard repository

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}Creating clean AEMO Energy Dashboard repository...${NC}"

# Create temporary directory for clean repo
TEMP_DIR="/tmp/aemo-energy-dashboard-clean-$(date +%s)"
mkdir -p "$TEMP_DIR"

echo -e "${YELLOW}Copying source files to: $TEMP_DIR${NC}"

# Create directory structure
mkdir -p "$TEMP_DIR/src"
mkdir -p "$TEMP_DIR/scripts"
mkdir -p "$TEMP_DIR/docs"
mkdir -p "$TEMP_DIR/tests"

# Copy source code (excluding __pycache__ and .pyc files)
echo "Copying source code..."
rsync -av --exclude='__pycache__' --exclude='*.pyc' src/ "$TEMP_DIR/src/"

# Copy essential configuration files
echo "Copying configuration files..."
cp pyproject.toml "$TEMP_DIR/"
cp uv.lock "$TEMP_DIR/" 2>/dev/null || echo "No uv.lock file found"
cp README.md "$TEMP_DIR/" 2>/dev/null || echo "No README.md file found"

# Copy documentation (excluding any env files)
echo "Copying documentation..."
for file in CLAUDE.md SYSTEM_ARCHITECTURE.md CLAUDE_MOVE.md; do
    if [ -f "$file" ]; then
        cp "$file" "$TEMP_DIR/"
    fi
done

# Copy scripts (but not deployment scripts with potential secrets)
echo "Copying safe scripts..."
for script in scripts/*.py scripts/*.sh; do
    if [ -f "$script" ]; then
        # Skip any script that might contain deployment info
        if [[ ! "$script" =~ deploy|production|start ]]; then
            cp "$script" "$TEMP_DIR/scripts/" 2>/dev/null || true
        fi
    fi
done

# Copy test files if they exist
echo "Copying test files..."
if [ -d "tests" ]; then
    rsync -av --exclude='__pycache__' --exclude='*.pyc' tests/ "$TEMP_DIR/tests/"
fi

# Create a comprehensive .gitignore
echo "Creating .gitignore..."
cat > "$TEMP_DIR/.gitignore" << 'EOF'
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg
MANIFEST

# Virtual environments
venv/
ENV/
env/
.venv/

# IDE
.vscode/
.idea/
*.swp
*.swo
*~
.DS_Store

# Environment files - NEVER commit these
.env
.env.*
*.env
env.*
.env.local
.env.production
.env.development
.env.test

# Logs
logs/
*.log

# Data files
data/
*.parquet
*.csv
*.pkl
*.pickle
*.json

# Temporary files
.tmp/
tmp/
temp/
*.tmp

# Test outputs
htmlcov/
.tox/
.coverage
.coverage.*
.cache
nosetests.xml
coverage.xml
*.cover
.hypothesis/
.pytest_cache/

# Jupyter
.ipynb_checkpoints
*.ipynb

# Screenshots and media
screenshots/
*.png
*.jpg
*.jpeg
*.gif

# Claude settings
.claude/

# Other
*.bak
*.backup
*.old
.history/
node_modules/
EOF

# Create a clean README
echo "Creating clean README..."
cat > "$TEMP_DIR/README.md" << 'EOF'
# AEMO Energy Dashboard

A comprehensive web-based visualization platform for analyzing Australian electricity market data.

## Features

- Real-time data visualization updated every 4.5 minutes
- Interactive charts and tables for generation, pricing, and transmission analysis
- Station-level performance analysis with revenue calculations
- Automatic resolution selection for optimal performance
- DuckDB-powered queries for memory-efficient data processing

## Installation

```bash
# Clone the repository
git clone https://github.com/davidleitch1/aemo-energy-dashboard.git
cd aemo-energy-dashboard

# Create virtual environment
uv venv
source .venv/bin/activate

# Install dependencies
uv pip install -e .
```

## Configuration

Create a `.env` file with your data file paths:

```bash
# Data file locations
GEN_OUTPUT_FILE=/path/to/scada30.parquet
GEN_OUTPUT_FILE_5MIN=/path/to/scada5.parquet
TRANSMISSION_OUTPUT_FILE=/path/to/transmission30.parquet
TRANSMISSION_OUTPUT_FILE_5MIN=/path/to/transmission5.parquet
SPOT_HIST_FILE=/path/to/prices30.parquet
SPOT_HIST_FILE_5MIN=/path/to/prices5.parquet
ROOFTOP_SOLAR_FILE=/path/to/rooftop30.parquet
GEN_INFO_FILE=/path/to/gen_info.pkl

# Dashboard settings
DASHBOARD_PORT=5006
USE_DUCKDB=true
```

## Usage

```bash
# Run the dashboard
python run_dashboard_duckdb.py

# Access at http://localhost:5006
```

## Data Source

This dashboard reads data files created by the [aemo-data-updater](https://github.com/davidleitch1/aemo-data-updater) service.

## License

MIT License - See LICENSE file for details.
EOF

# Create example env file
echo "Creating example env file..."
cat > "$TEMP_DIR/.env.example" << 'EOF'
# Data file locations (update these paths to match your system)
GEN_OUTPUT_FILE=/path/to/your/data/scada30.parquet
GEN_OUTPUT_FILE_5MIN=/path/to/your/data/scada5.parquet
TRANSMISSION_OUTPUT_FILE=/path/to/your/data/transmission30.parquet
TRANSMISSION_OUTPUT_FILE_5MIN=/path/to/your/data/transmission5.parquet
SPOT_HIST_FILE=/path/to/your/data/prices30.parquet
SPOT_HIST_FILE_5MIN=/path/to/your/data/prices5.parquet
ROOFTOP_SOLAR_FILE=/path/to/your/data/rooftop30.parquet
GEN_INFO_FILE=/path/to/your/data/gen_info.pkl

# Dashboard settings
DASHBOARD_PORT=5006
DASHBOARD_HOST=0.0.0.0
LOG_LEVEL=INFO
USE_DUCKDB=true
EOF

# Copy the main run script
echo "Copying run script..."
if [ -f "run_dashboard_duckdb.py" ]; then
    cp run_dashboard_duckdb.py "$TEMP_DIR/"
fi

# Initialize git repository
cd "$TEMP_DIR"
git init
git add -A
git commit -m "Initial commit: AEMO Energy Dashboard

- Clean repository without any secrets
- DuckDB-powered dashboard for Australian electricity market data
- Reads data from aemo-data-updater service
- Interactive visualizations for generation, pricing, and transmission"

echo -e "${GREEN}✓ Clean repository created at: $TEMP_DIR${NC}"
echo -e "${YELLOW}Next steps:${NC}"
echo "1. cd \"$TEMP_DIR\""
echo "2. git remote add origin https://github.com/davidleitch1/aemo-energy-dashboard-new.git"
echo "3. git branch -M main"
echo "4. git push -u origin main"
echo ""
echo -e "${YELLOW}Or to move it to a permanent location:${NC}"
echo "mv \"$TEMP_DIR\" ~/aemo-energy-dashboard-clean"