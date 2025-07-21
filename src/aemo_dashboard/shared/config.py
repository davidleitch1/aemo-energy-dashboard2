"""
Shared configuration management for AEMO Energy Dashboard
"""

import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

from .logging_config import get_logger

logger = get_logger(__name__)

class Config:
    """Centralized configuration management"""
    
    def __init__(self, env_file: Optional[str] = None):
        """
        Initialize configuration.
        
        Args:
            env_file: Path to .env file (defaults to .env in project root)
        """
        # Load environment variables
        if env_file:
            env_path = Path(env_file)
        else:
            # Look for .env in project root
            project_root = Path(__file__).parent.parent.parent.parent
            env_path = project_root / '.env'
        
        if env_path.exists():
            load_dotenv(env_path)
            logger.info(f"Loaded configuration from {env_path}")
        else:
            logger.warning(f"No .env file found at {env_path}")
        
        # Set up paths
        self.project_root = Path(__file__).parent.parent.parent.parent
        self.setup_paths()
    
    def setup_paths(self):
        """Set up data and log directory paths"""
        # Data directory
        data_dir = os.getenv('DATA_DIR')
        if data_dir:
            self.data_dir = Path(data_dir)
        else:
            self.data_dir = self.project_root / 'data'
        
        # Logs directory  
        logs_dir = os.getenv('LOGS_DIR')
        if logs_dir:
            self.logs_dir = Path(logs_dir)
        else:
            self.logs_dir = self.project_root / 'logs'
        
        # Create directories
        self.data_dir.mkdir(exist_ok=True)
        self.logs_dir.mkdir(exist_ok=True)
        
        # Specific data files
        self.spot_hist_file = self._get_file_path('SPOT_HIST_FILE', 'spot_hist.parquet')
        self.gen_output_file = self._get_file_path('GEN_OUTPUT_FILE', 'gen_output.parquet')
        self.gen_info_file = self._get_file_path('GEN_INFO_FILE', 'gen_info.pkl')
        self.transmission_output_file = self._get_file_path('TRANSMISSION_OUTPUT_FILE', 'transmission_flows.parquet')
        self.rooftop_solar_file = self._get_file_path('ROOFTOP_SOLAR_FILE', 'rooftop_solar.parquet')
    
    def _get_file_path(self, env_var: str, default_name: str) -> Path:
        """Get file path from environment or use default in data directory"""
        file_path = os.getenv(env_var)
        if file_path:
            return Path(file_path)
        else:
            return self.data_dir / default_name
    
    # Email configuration
    @property
    def email_enabled(self) -> bool:
        return os.getenv('ENABLE_EMAIL_ALERTS', 'true').lower() == 'true'
    
    @property
    def alert_email(self) -> Optional[str]:
        return os.getenv('ALERT_EMAIL')
    
    @property
    def alert_password(self) -> Optional[str]:
        return os.getenv('ALERT_PASSWORD')
    
    @property
    def recipient_email(self) -> Optional[str]:
        return os.getenv('RECIPIENT_EMAIL', self.alert_email)
    
    @property
    def smtp_server(self) -> str:
        return os.getenv('SMTP_SERVER', 'smtp.mail.me.com')
    
    @property
    def smtp_port(self) -> int:
        return int(os.getenv('SMTP_PORT', '587'))
    
    # Dashboard configuration
    @property
    def default_region(self) -> str:
        return os.getenv('DEFAULT_REGION', 'NEM')
    
    @property
    def update_interval_minutes(self) -> float:
        return float(os.getenv('UPDATE_INTERVAL_MINUTES', '4.5'))
    
    @property
    def dashboard_port(self) -> int:
        return int(os.getenv('DASHBOARD_PORT', '5008'))
    
    @property
    def dashboard_host(self) -> str:
        return os.getenv('DASHBOARD_HOST', 'localhost')
    
    # Price alert thresholds
    @property
    def high_price_threshold(self) -> float:
        return float(os.getenv('HIGH_PRICE_THRESHOLD', '1000.0'))
    
    @property
    def low_price_threshold(self) -> float:
        return float(os.getenv('LOW_PRICE_THRESHOLD', '300.0'))
    
    @property
    def extreme_price_threshold(self) -> float:
        return float(os.getenv('EXTREME_PRICE_THRESHOLD', '10000.0'))
    
    # Twilio SMS configuration
    @property
    def twilio_account_sid(self) -> Optional[str]:
        return os.getenv('TWILIO_ACCOUNT_SID')
    
    @property
    def twilio_auth_token(self) -> Optional[str]:
        return os.getenv('TWILIO_AUTH_TOKEN')
    
    @property
    def twilio_phone_number(self) -> Optional[str]:
        return os.getenv('TWILIO_PHONE_NUMBER')
    
    @property
    def my_phone_number(self) -> Optional[str]:
        return os.getenv('MY_PHONE_NUMBER')
    
    # AEMO data sources
    @property
    def aemo_dispatch_url(self) -> str:
        return os.getenv('AEMO_DISPATCH_URL', 'http://nemweb.com.au/Reports/Current/Dispatch_Reports/')
    
    @property
    def aemo_interconnector_url(self) -> str:
        return os.getenv('AEMO_INTERCONNECTOR_URL', 'http://nemweb.com.au/Reports/Current/DispatchIS_Reports/')
    
    # Alert behavior
    @property
    def alert_cooldown_hours(self) -> int:
        return int(os.getenv('ALERT_COOLDOWN_HOURS', '24'))
    
    @property
    def auto_add_to_exceptions(self) -> bool:
        return os.getenv('AUTO_ADD_TO_EXCEPTIONS', 'true').lower() == 'true'

# Global configuration instance
config = Config()