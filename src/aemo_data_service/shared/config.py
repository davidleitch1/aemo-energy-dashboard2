#!/usr/bin/env python3
"""
Configuration adapter for AEMO Data Service
Uses the existing dashboard configuration system.
"""

# Import from the existing dashboard configuration
from aemo_dashboard.shared.config import config as dashboard_config

# Create a simple wrapper that provides the interface expected by collectors
class DataServiceConfig:
    """
    Configuration adapter for the data service.
    Uses existing dashboard config to maintain compatibility.
    """
    
    def __init__(self):
        """Initialize using existing dashboard config."""
        self._dashboard_config = dashboard_config
    
    @property
    def gen_output_file(self):
        return self._dashboard_config.gen_output_file
    
    @property
    def gen_info_file(self):
        return self._dashboard_config.gen_info_file
    
    @property
    def spot_hist_file(self):
        return self._dashboard_config.spot_hist_file
    
    @property
    def rooftop_file(self):
        return self._dashboard_config.rooftop_solar_file
    
    @property
    def transmission_file(self):
        return self._dashboard_config.transmission_output_file
    
    @property
    def update_interval_minutes(self):
        return self._dashboard_config.update_interval_minutes
    
    @property
    def aemo_dispatch_url(self):
        return self._dashboard_config.aemo_dispatch_url
    
    @property
    def aemo_scada_url(self):
        return "http://nemweb.com.au/Reports/CURRENT/Dispatch_SCADA/"
    
    @property
    def log_level(self):
        return 'INFO'
    
    @property
    def log_file(self):
        return self._dashboard_config.data_dir.parent / 'logs' / 'data_service.log'
    
    def get_summary(self) -> str:
        """Get a summary of the current configuration."""
        summary = "AEMO Data Service Configuration:\n"
        summary += f"  Update interval: {self.update_interval_minutes} minutes\n"
        summary += f"  Log file: {self.log_file}\n"
        summary += "  Data files:\n"
        summary += f"    Generation: {self.gen_output_file}\n"
        summary += f"    Prices: {self.spot_hist_file}\n"
        summary += f"    Rooftop: {self.rooftop_file}\n"
        summary += f"    Transmission: {self.transmission_file}\n"
        
        return summary
    
    def create_directories(self):
        """Ensure all required directories exist."""
        self.log_file.parent.mkdir(parents=True, exist_ok=True)


# Create global config instance
config = DataServiceConfig()

# Ensure directories exist when module is imported
config.create_directories()