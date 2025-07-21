
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
