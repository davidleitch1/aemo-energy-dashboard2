# AEMO System Production Migration Plan

*Created: July 20, 2025, 12:45 PM AEST*  
*Updated: July 20, 2025, 5:40 PM AEST - Changed dashboard repository to aemo-energy-dashboard2*

## Repository Status Update

✅ **Git Push Complete** (July 20, 2025):
- **aemo-data-updater**: Successfully pushed to https://github.com/davidleitch1/aemo-data-updater
- **aemo-energy-dashboard2**: Successfully pushed to https://github.com/davidleitch1/aemo-energy-dashboard2
  - Clean repository created without any secret history
  - Original dashboard repo (aemo-energy-dashboard) DELETED (July 21, 2025) due to secret scanning issues

## Overview

This document provides step-by-step instructions for migrating the AEMO Energy System from iCloud storage to local SSD storage on an M2 Mac production machine. The migration ensures zero data loss by running collectors in parallel during the transition.

## Pre-Migration Checklist

### Required Files to Migrate
```
Data Files (2.5GB+):
├── scada5.parquet          (~400MB, 6M+ records)
├── scada30.parquet         (~1.5GB, 38M+ records)
├── prices5.parquet         (~200MB, 69K+ records)
├── prices30.parquet        (~200MB, 1.7M+ records)
├── transmission5.parquet   (~150MB, 46K+ records)
├── transmission30.parquet  (~150MB, 1.9M+ records)
├── rooftop30.parquet      (~100MB, 811K+ records)
├── gen_info.pkl           (~40KB, DUID metadata)
├── price_alert_state.pkl  (~1KB, alert tracking)
└── renewable_records.json  (~2KB, if exists)
```

### Repository Status Check
```bash
# Check uncommitted changes in both repos
cd ~/Library/Mobile\ Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater
git status

cd ~/Library/Mobile\ Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard  
git status
```

## Phase 1: Repository Preparation (30 minutes)

### Step 1.1: Commit All Changes
```bash
# Dashboard repository
cd ~/Library/Mobile\ Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard
git add -A
git commit -m "Pre-migration commit: Fixed gauge legend positioning, updated documentation"
git push origin main

# Data updater repository  
cd ~/Library/Mobile\ Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater
git add -A
git commit -m "Pre-migration commit: Latest data collection updates"
git push origin main
```

### Step 1.2: Create Migration Branches
```bash
# Create migration branches for testing
cd ~/Library/Mobile\ Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard
git checkout -b production-migration

cd ~/Library/Mobile\ Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater
git checkout -b production-migration
```

## Phase 2: Production Directory Setup (15 minutes)

### Step 2.1: Create Production Structure
```bash
# Create production directories
mkdir -p ~/aemo_production/{data,logs,backup}

# Set permissions
chmod 755 ~/aemo_production
chmod 755 ~/aemo_production/*
```

### Step 2.2: Clone Repositories
```bash
cd ~/aemo_production

# Clone from GitHub (not local copies)
git clone https://github.com/davidleitch1/aemo-data-updater.git
git clone https://github.com/davidleitch1/aemo-energy-dashboard2.git

# Checkout migration branches (if they exist)
cd aemo-data-updater && git checkout production-migration || git checkout main
cd ../aemo-energy-dashboard2 && git checkout production-migration || git checkout main
```

## Phase 3: Data Migration (45 minutes)

### Step 3.1: Stop Current Collectors (CRITICAL)
```bash
# Find and note PIDs of running collectors
ps aux | grep -E "update_spot|gen_dash|unified_collector" | grep -v grep

# Stop them gracefully (replace PID with actual numbers)
# kill -TERM <PID>
```

### Step 3.2: Copy Data Files with Verification
```bash
# Create migration script
cat > ~/aemo_production/migrate_data.sh << 'EOF'
#!/bin/bash
set -e

SOURCE="/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot"
DEST="/Users/davidleitch/aemo_production/data"

echo "Starting data migration..."

# Function to copy and verify
copy_verify() {
    local file=$1
    echo "Copying $file..."
    cp "$SOURCE/$file" "$DEST/"
    
    # Verify size matches
    src_size=$(stat -f%z "$SOURCE/$file")
    dst_size=$(stat -f%z "$DEST/$file")
    
    if [ "$src_size" -eq "$dst_size" ]; then
        echo "✓ $file copied successfully ($src_size bytes)"
    else
        echo "✗ ERROR: Size mismatch for $file"
        exit 1
    fi
}

# Copy parquet files
for file in scada5.parquet scada30.parquet prices5.parquet prices30.parquet \
            transmission5.parquet transmission30.parquet rooftop30.parquet; do
    copy_verify "$file"
done

# Copy reference files
echo "Copying reference files..."
cp "$SOURCE/aemo-energy-dashboard/data/gen_info.pkl" "$DEST/"
cp "$SOURCE/price_alert_state.pkl" "$DEST/" 2>/dev/null || echo "No price_alert_state.pkl found"
cp "$SOURCE/renewable_records.json" "$DEST/" 2>/dev/null || echo "No renewable_records.json found"

echo "Migration complete!"
ls -lah "$DEST/"
EOF

chmod +x ~/aemo_production/migrate_data.sh
~/aemo_production/migrate_data.sh
```

### Step 3.3: Create Symbolic Links (Optional)
```bash
# If you want to keep some files in iCloud for backup
cd ~/aemo_production/data
ln -s "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/gen_info.pkl" gen_info_icloud.pkl
```

## Phase 4: Configuration Updates (20 minutes)

### Step 4.1: Update Data Updater Configuration
```bash
cd ~/aemo_production/aemo-data-updater

# Create production .env file
cat > .env << 'EOF'
# Production paths - Local SSD
GEN_OUTPUT_FILE=/Users/davidleitch/aemo_production/data/scada30.parquet
GEN_OUTPUT_FILE_5MIN=/Users/davidleitch/aemo_production/data/scada5.parquet
SPOT_HIST_FILE=/Users/davidleitch/aemo_production/data/prices30.parquet
SPOT_HIST_FILE_5MIN=/Users/davidleitch/aemo_production/data/prices5.parquet
TRANSMISSION_OUTPUT_FILE=/Users/davidleitch/aemo_production/data/transmission30.parquet
TRANSMISSION_OUTPUT_FILE_5MIN=/Users/davidleitch/aemo_production/data/transmission5.parquet
ROOFTOP_SOLAR_FILE=/Users/davidleitch/aemo_production/data/rooftop30.parquet
GEN_INFO_FILE=/Users/davidleitch/aemo_production/data/gen_info.pkl
PRICE_ALERT_STATE_FILE=/Users/davidleitch/aemo_production/data/price_alert_state.pkl

# Alert credentials (copy from original .env)
TWILIO_ACCOUNT_SID=your_account_sid
TWILIO_AUTH_TOKEN=your_auth_token
TWILIO_FROM_PHONE=+19513382338
TWILIO_TO_PHONE=+61412519001
HIGH_PRICE_THRESHOLD=1000
EXTREME_PRICE_THRESHOLD=10000
NORMAL_PRICE_THRESHOLD=300

EMAIL_ADDRESS=david.leitch@icloud.com
EMAIL_PASSWORD=cnud-iufu-mpyr-wbcn
EMAIL_SMTP_SERVER=smtp.mail.me.com
EMAIL_SMTP_PORT=587
EOF
```

### Step 4.2: Update Dashboard Configuration
```bash
cd ~/aemo_production/aemo-energy-dashboard2

# Create production .env file
cat > .env << 'EOF'
# Production paths - Local SSD
GEN_OUTPUT_FILE=/Users/davidleitch/aemo_production/data/scada30.parquet
GEN_OUTPUT_FILE_5MIN=/Users/davidleitch/aemo_production/data/scada5.parquet
TRANSMISSION_OUTPUT_FILE=/Users/davidleitch/aemo_production/data/transmission30.parquet
TRANSMISSION_OUTPUT_FILE_5MIN=/Users/davidleitch/aemo_production/data/transmission5.parquet
SPOT_HIST_FILE=/Users/davidleitch/aemo_production/data/prices30.parquet
SPOT_HIST_FILE_5MIN=/Users/davidleitch/aemo_production/data/prices5.parquet
ROOFTOP_SOLAR_FILE=/Users/davidleitch/aemo_production/data/rooftop30.parquet
GEN_INFO_FILE=/Users/davidleitch/aemo_production/data/gen_info.pkl

# Dashboard settings
DASHBOARD_PORT=5006
DASHBOARD_HOST=0.0.0.0
LOG_LEVEL=INFO
USE_DUCKDB=true
EOF
```

## Phase 5: Parallel Running Strategy (Critical for Zero Data Loss)

### Step 5.1: Configure Temporary File Locking
```bash
# Create file lock mechanism to prevent corruption
cat > ~/aemo_production/aemo-data-updater/src/aemo_updater/file_lock.py << 'EOF'
import fcntl
import time
import logging

logger = logging.getLogger(__name__)

class ParquetFileLock:
    """Ensures safe concurrent access to parquet files during migration"""
    
    def __init__(self, filepath):
        self.filepath = filepath
        self.lockfile = f"{filepath}.lock"
        self.lock_fd = None
        
    def __enter__(self):
        self.lock_fd = open(self.lockfile, 'w')
        retries = 0
        while retries < 30:  # 30 second timeout
            try:
                fcntl.flock(self.lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return self
            except IOError:
                logger.warning(f"Waiting for lock on {self.filepath}...")
                time.sleep(1)
                retries += 1
        raise TimeoutError(f"Could not acquire lock on {self.filepath}")
        
    def __exit__(self, *args):
        if self.lock_fd:
            fcntl.flock(self.lock_fd, fcntl.LOCK_UN)
            self.lock_fd.close()
EOF
```

### Step 5.2: Test New Collector
```bash
cd ~/aemo_production/aemo-data-updater

# Set up virtual environment
uv venv
source .venv/bin/activate
uv pip install -e .

# Test single collection cycle
python -m aemo_updater.collectors.unified_collector --test-mode
```

## Phase 6: Transition Process (30 minutes)

### Step 6.1: Start New Collector in Production
```bash
cd ~/aemo_production/aemo-data-updater

# Create startup script
cat > start_production_collector.sh << 'EOF'
#!/bin/bash
cd /Users/davidleitch/aemo_production/aemo-data-updater
source .venv/bin/activate
nohup python -m aemo_updater.collectors.unified_collector \
    --log-file /Users/davidleitch/aemo_production/logs/collector.log \
    > /Users/davidleitch/aemo_production/logs/collector.out 2>&1 &
echo $! > /Users/davidleitch/aemo_production/collector.pid
echo "Collector started with PID: $(cat /Users/davidleitch/aemo_production/collector.pid)"
EOF

chmod +x start_production_collector.sh
./start_production_collector.sh
```

### Step 6.2: Verify New Collector Operation
```bash
# Monitor logs for 5 minutes
tail -f ~/aemo_production/logs/collector.log

# Check file modifications
watch -n 60 'ls -lah ~/aemo_production/data/*.parquet | head -5'
```

### Step 6.3: Stop Old Collectors
```bash
# After confirming new collector works (wait at least 10 minutes)
# Stop old collectors using PIDs from Step 3.1
# kill -TERM <OLD_PID>
```

## Phase 7: Dashboard Migration (15 minutes)

### Step 7.1: Stop Old Dashboard
```bash
# Find dashboard process
ps aux | grep gen_dash | grep -v grep
# kill -TERM <PID>
```

### Step 7.2: Start New Dashboard
```bash
cd ~/aemo_production/aemo-energy-dashboard2

# Set up virtual environment
uv venv
source .venv/bin/activate
uv pip install -e .

# Create startup script
cat > start_production_dashboard.sh << 'EOF'
#!/bin/bash
cd /Users/davidleitch/aemo_production/aemo-energy-dashboard2
source .venv/bin/activate
nohup python run_dashboard_duckdb.py \
    > /Users/davidleitch/aemo_production/logs/dashboard.out 2>&1 &
echo $! > /Users/davidleitch/aemo_production/dashboard.pid
echo "Dashboard started with PID: $(cat /Users/davidleitch/aemo_production/dashboard.pid)"
echo "Access at: http://localhost:5006"
EOF

chmod +x start_production_dashboard.sh
./start_production_dashboard.sh
```

## Phase 8: Verification & Monitoring (20 minutes)

### Step 8.1: Verify Data Integrity
```bash
# Create verification script
cat > ~/aemo_production/verify_data.py << 'EOF'
import pandas as pd
import os
from datetime import datetime, timedelta

data_dir = "/Users/davidleitch/aemo_production/data"

def verify_file(filename):
    filepath = os.path.join(data_dir, filename)
    try:
        df = pd.read_parquet(filepath)
        latest = df['settlementdate'].max()
        age = datetime.now() - pd.to_datetime(latest)
        
        print(f"\n{filename}:")
        print(f"  Records: {len(df):,}")
        print(f"  Latest: {latest}")
        print(f"  Age: {age}")
        
        if age > timedelta(hours=1):
            print(f"  ⚠️  WARNING: Data is stale!")
            
    except Exception as e:
        print(f"\n{filename}: ❌ ERROR - {e}")

# Check all parquet files
for f in ['scada5.parquet', 'scada30.parquet', 'prices5.parquet', 
          'prices30.parquet', 'transmission5.parquet', 'rooftop30.parquet']:
    verify_file(f)
EOF

python ~/aemo_production/verify_data.py
```

### Step 8.2: Performance Comparison
```bash
# Test dashboard load time
time curl -s http://localhost:5006 > /dev/null

# Monitor memory usage
ps aux | grep -E "python.*dashboard|python.*collector" | grep -v grep
```

## Phase 9: Finalization (15 minutes)

### Step 9.1: Set Up Automatic Startup
```bash
# Create LaunchAgent for collector
cat > ~/Library/LaunchAgents/com.aemo.datacollector.plist << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.aemo.datacollector</string>
    <key>ProgramArguments</key>
    <array>
        <string>/Users/davidleitch/aemo_production/aemo-data-updater/start_production_collector.sh</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/Users/davidleitch/aemo_production/logs/launchd-collector.out</string>
    <key>StandardErrorPath</key>
    <string>/Users/davidleitch/aemo_production/logs/launchd-collector.err</string>
</dict>
</plist>
EOF

# Load the service
launchctl load ~/Library/LaunchAgents/com.aemo.datacollector.plist
```

### Step 9.2: Set Up Backup Sync
```bash
# Create daily backup to iCloud
cat > ~/aemo_production/backup_to_icloud.sh << 'EOF'
#!/bin/bash
SOURCE="/Users/davidleitch/aemo_production/data"
DEST="/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/backup"

# Create backup directory
mkdir -p "$DEST"

# Sync only parquet files modified in last 2 days
find "$SOURCE" -name "*.parquet" -mtime -2 -exec rsync -av {} "$DEST/" \;

echo "Backup completed at $(date)"
EOF

chmod +x ~/aemo_production/backup_to_icloud.sh

# Add to crontab (runs daily at 2 AM)
(crontab -l 2>/dev/null; echo "0 2 * * * /Users/davidleitch/aemo_production/backup_to_icloud.sh") | crontab -
```

### Step 9.3: Update Git Repositories
```bash
# Commit configuration changes
cd ~/aemo_production/aemo-data-updater
git add .env start_production_collector.sh
git commit -m "Add production configuration for local SSD storage"
git push origin production-migration

cd ~/aemo_production/aemo-energy-dashboard2
git add .env start_production_dashboard.sh
git commit -m "Add production configuration for local SSD storage"
git push origin production-migration

# Create pull requests to merge into main branch
```

## Post-Migration Checklist

### Verify Everything is Working
- [ ] New collector is updating all parquet files every 4.5 minutes
- [ ] Dashboard loads in < 2 seconds
- [ ] Price alerts are functioning (test with threshold adjustment)
- [ ] Email alerts for new DUIDs are working
- [ ] No data gaps during migration period
- [ ] Memory usage is under 500MB for dashboard
- [ ] Backup sync to iCloud is configured

### Performance Metrics to Track
```bash
# Create monitoring script
cat > ~/aemo_production/monitor_performance.sh << 'EOF'
#!/bin/bash
echo "=== AEMO System Performance Report ==="
echo "Time: $(date)"
echo ""
echo "=== Data Freshness ==="
python ~/aemo_production/verify_data.py | grep -E "Latest:|Age:"
echo ""
echo "=== Process Status ==="
ps aux | grep -E "unified_collector|dashboard" | grep -v grep
echo ""
echo "=== Disk Usage ==="
du -sh ~/aemo_production/data/*.parquet | sort -h
echo ""
echo "=== Recent Errors ==="
tail -5 ~/aemo_production/logs/collector.log | grep ERROR || echo "No recent errors"
EOF

chmod +x ~/aemo_production/monitor_performance.sh
```

## Rollback Plan

If issues occur, you can quickly rollback:

```bash
# Stop new services
kill $(cat ~/aemo_production/collector.pid)
kill $(cat ~/aemo_production/dashboard.pid)

# Update .env files to point back to iCloud paths
# Restart old collectors
cd "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot"
# Start previous collectors
```

## Expected Performance Improvements

### Before (iCloud)
- Dashboard startup: 8-10 seconds
- Query time (1 year): 2-5 seconds  
- File access latency: 10-100ms

### After (Local SSD)
- Dashboard startup: 1-2 seconds
- Query time (1 year): 50-200ms
- File access latency: <1ms

## Support Notes

1. **File Locking**: The parallel running phase uses file locking to prevent corruption
2. **Data Gaps**: The 4.5-minute collection cycle means maximum 5 minutes of potential gap
3. **iCloud Sync**: Disable iCloud sync for the production directory to avoid conflicts
4. **Monitoring**: Check logs daily for the first week after migration

---

*Migration plan prepared for AEMO Energy System v2.0*
*Estimated total migration time: 3 hours with verification*