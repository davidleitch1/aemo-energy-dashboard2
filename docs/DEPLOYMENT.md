# Deployment Guide for AEMO Energy Dashboard

## Replacing the Production Dashboard

The new dashboard is designed to replace the existing dashboard running at `/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/genhist/gen_dash.py`.

### Key Changes

1. **Port Configuration**: The new dashboard now uses port 5008 by default (same as production)
2. **Cloudflare Tunnel**: Already configured to accept connections from `nemgen.itkservices2.com`
3. **Environment Variable**: Use `DASHBOARD_PORT` to control the port

### Environment Differences

**Important**: The old dashboard uses system Python, while the new dashboard uses a virtual environment. This isolation prevents conflicts but requires proper setup.

### Automated Deployment

Use the deployment script for easiest transition:

```bash
cd /Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard
./deploy_production.sh
```

This script will:
- Stop the old dashboard
- Set up the Python virtual environment
- Install all dependencies
- Configure the environment
- Start the new dashboard

### Manual Deployment Steps

1. **Stop the old dashboard**:
   ```bash
   # Find the process
   ps aux | grep "genhist/gen_dash.py"
   # Kill the process
   kill -9 <PID>
   ```

2. **Set up the new dashboard environment**:
   ```bash
   cd /Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard
   
   # Create virtual environment if needed
   python3 -m venv .venv
   
   # Activate virtual environment
   source .venv/bin/activate
   
   # Install dependencies
   pip install -e .
   ```

3. **Configure the new dashboard**:
   ```bash
   # Copy environment template
   cp .env.example .env
   
   # Edit .env to add credentials and ensure:
   # DASHBOARD_PORT=5008  (for production)
   ```

4. **Start the new dashboard**:
   ```bash
   # Using the wrapper script (works like old dashboard)
   python3 run_dashboard.py
   
   # Or using virtual environment directly
   .venv/bin/python -m src.aemo_dashboard.generation.gen_dash
   ```

4. **For development** (running alongside production):
   ```bash
   # Set different port in .env
   DASHBOARD_PORT=5010
   
   # Or use environment variable
   DASHBOARD_PORT=5010 .venv/bin/python -m src.aemo_dashboard.generation.gen_dash
   ```

### Running as a Service

For persistent deployment, create a systemd service or use screen/tmux:

```bash
# Using screen
screen -S aemo-dashboard
cd /Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard
source .venv/bin/activate
.venv/bin/python -m src.aemo_dashboard.generation.gen_dash
# Detach with Ctrl+A, D
```

### Verification

1. **Local access**: http://localhost:5008
2. **Remote access**: https://nemgen.itkservices2.com (via Cloudflare tunnel)

### Features in New Dashboard

- **Nem-dash tab**: New primary overview with gauge, generation chart, and price info
- **Generation by Fuel**: Enhanced with better battery handling
- **Price Analysis**: Advanced filtering and analysis
- **Station Analysis**: Detailed station/DUID analysis
- **Improved rooftop solar**: Better interpolation and handling

### Rollback

If needed, restart the old dashboard:
```bash
cd /Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/genhist
python gen_dash.py
```