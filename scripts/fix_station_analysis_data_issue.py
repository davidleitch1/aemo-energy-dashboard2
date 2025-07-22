#!/usr/bin/env python3
"""
Fix for Station Analysis Data Display Issue

This script fixes the root cause: DUID mapping contains DUIDs that don't have recent data.
The solution is to filter the station search to only show DUIDs with recent generation data.
"""

import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

def get_active_duids(days_back=7):
    """
    Get list of DUIDs that have generation data in recent days
    
    Args:
        days_back: Number of days to look back for active data
    
    Returns:
        Set of DUIDs with recent data
    """
    print(f"🔍 Finding DUIDs with data in last {days_back} days...")
    
    # Load recent generation data
    gen_file = "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2/scada5.parquet"
    gen_data = pd.read_parquet(gen_file)
    
    # Filter to recent period
    cutoff_date = datetime.now() - timedelta(days=days_back)
    recent_data = gen_data[gen_data['settlementdate'] >= cutoff_date]
    
    active_duids = set(recent_data['duid'].unique())
    print(f"✅ Found {len(active_duids)} DUIDs with recent data")
    
    return active_duids

def create_active_duids_filter():
    """Create a filter file containing DUIDs with recent data"""
    
    print("🔧 Creating active DUIDs filter...")
    
    # Get DUIDs with data in last 7 days
    active_duids = get_active_duids(days_back=7)
    
    # Save to file for use by station search
    filter_file = "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard/data/active_duids.txt"
    
    with open(filter_file, 'w') as f:
        for duid in sorted(active_duids):
            f.write(f"{duid}\n")
    
    print(f"✅ Saved {len(active_duids)} active DUIDs to {filter_file}")
    return filter_file

def create_station_search_patch():
    """Create a patched version of the station search that only shows active DUIDs"""
    
    patch_content = '''
# Add this method to StationSearchEngine class

def filter_to_active_duids(self, active_duids_file=None):
    """
    Filter search index to only include DUIDs with recent generation data
    
    Args:
        active_duids_file: Path to file containing active DUIDs (one per line)
    """
    if active_duids_file is None:
        active_duids_file = "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard/data/active_duids.txt"
    
    try:
        # Load active DUIDs
        with open(active_duids_file, 'r') as f:
            active_duids = set(line.strip() for line in f if line.strip())
        
        # Filter search index
        original_count = len(self.search_index)
        self.search_index = [
            entry for entry in self.search_index 
            if entry['duid'] in active_duids
        ]
        
        # Filter station index
        original_station_count = len(self.station_index)
        self.station_index = [
            station for station in self.station_index
            if any(duid in active_duids for duid in station.get('duids', [station.get('duid', '')]))
        ]
        
        logger.info(f"Filtered search index: {original_count} -> {len(self.search_index)} DUIDs")
        logger.info(f"Filtered station index: {original_station_count} -> {len(self.station_index)} stations")
        
        return True
        
    except Exception as e:
        logger.error(f"Error filtering to active DUIDs: {e}")
        return False
'''
    
    patch_file = "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard/station_search_patch.py"
    with open(patch_file, 'w') as f:
        f.write(patch_content)
    
    print(f"✅ Created patch file: {patch_file}")
    return patch_file

def apply_quick_fix():
    """Apply a quick fix to the station analysis UI initialization"""
    
    print("🔧 Applying Quick Fix to Station Analysis UI...")
    
    ui_file = "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard/src/aemo_dashboard/station/station_analysis_ui.py"
    
    # Read the current file
    with open(ui_file, 'r') as f:
        content = f.read()
    
    # Find the _initialize_components method and add active DUID filtering
    old_code = """            if self.motor.load_data():
                # Initialize search engine with loaded DUID mapping
                self.search_engine = StationSearchEngine(self.motor.duid_mapping)
                self.data_loaded = True
                logger.info("Station analysis components initialized successfully")"""
    
    new_code = """            if self.motor.load_data():
                # Initialize search engine with loaded DUID mapping
                self.search_engine = StationSearchEngine(self.motor.duid_mapping)
                
                # CRITICAL FIX: Filter to only show DUIDs with recent data
                self._filter_to_active_duids()
                
                self.data_loaded = True
                logger.info("Station analysis components initialized successfully")"""
    
    if old_code in content:
        content = content.replace(old_code, new_code)
        
        # Add the filtering method
        filter_method = '''
    def _filter_to_active_duids(self):
        """Filter search engine to only show DUIDs with recent generation data"""
        try:
            from datetime import datetime, timedelta
            from ..shared.generation_adapter import load_generation_data
            
            # Get DUIDs with data in last 7 days
            cutoff_date = datetime.now() - timedelta(days=7)
            recent_data = load_generation_data(
                start_date=cutoff_date,
                end_date=datetime.now(),
                resolution='5min'
            )
            
            if not recent_data.empty:
                active_duids = set(recent_data['duid'].unique())
                
                # Filter search index to only include active DUIDs
                original_count = len(self.search_engine.search_index)
                self.search_engine.search_index = [
                    entry for entry in self.search_engine.search_index 
                    if entry['duid'] in active_duids
                ]
                
                # Filter station index too
                original_station_count = len(self.search_engine.station_index)
                self.search_engine.station_index = [
                    station for station in self.search_engine.station_index
                    if any(duid in active_duids for duid in station.get('duids', [station.get('duid', '')]))
                ]
                
                logger.info(f"Filtered to active DUIDs: {original_count} -> {len(self.search_engine.search_index)} DUIDs")
                logger.info(f"Filtered to active stations: {original_station_count} -> {len(self.search_engine.station_index)} stations")
            else:
                logger.warning("No recent generation data found for filtering")
                
        except Exception as e:
            logger.error(f"Error filtering to active DUIDs: {e}")
            # Continue anyway - this is a non-critical optimization
'''
        
        # Add the method before the create_ui_components method
        insert_point = content.find("    def create_ui_components(self):")
        if insert_point != -1:
            content = content[:insert_point] + filter_method + "\n" + content[insert_point:]
        
        # Write the modified content back
        with open(ui_file, 'w') as f:
            f.write(content)
        
        print("✅ Applied quick fix to station analysis UI")
        return True
    else:
        print("❌ Could not find target code to replace")
        return False

def main():
    """Main fix application"""
    print("🔧 Fixing Station Analysis Data Display Issue")
    print("=" * 50)
    
    print("\nRoot Cause:")
    print("- DUID mapping contains DUIDs without recent generation data")
    print("- Station analysis tries to use these inactive DUIDs")
    print("- filter_station_data() correctly returns False for no data")
    print("- UI shows 'no data' instead of trying active DUIDs")
    
    print("\nSolution:")
    print("- Filter station search to only show DUIDs with recent data")
    print("- This ensures users only see stations that will actually work")
    
    print("\n" + "=" * 50)
    
    # Create active DUIDs filter
    filter_file = create_active_duids_filter()
    
    # Create patch
    patch_file = create_station_search_patch()
    
    # Apply quick fix
    if apply_quick_fix():
        print("\n✅ Fix Applied Successfully!")
        print("\nNext Steps:")
        print("1. Restart the dashboard")
        print("2. Test 1-day view station analysis")
        print("3. Verify that stations now show data")
        print("\nThe fix ensures only DUIDs with recent data are shown to users,")
        print("preventing the 'no data available' issue.")
    else:
        print("\n❌ Failed to apply automatic fix")
        print("Manual fix required - see the patch file for guidance")

if __name__ == "__main__":
    main()