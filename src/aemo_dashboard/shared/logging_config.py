"""
Unified logging configuration for AEMO Energy Dashboard
"""

import logging
import os
from pathlib import Path
from typing import Optional

def setup_logging(
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    log_format: Optional[str] = None,
    logs_dir: Optional[str] = None
) -> logging.Logger:
    """
    Set up unified logging for all dashboard components.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Log file name (default: aemo_dashboard.log)
        log_format: Log format string
        logs_dir: Directory for log files (default: logs/)
        
    Returns:
        Configured logger instance
    """
    
    # Get configuration from environment or use defaults
    # Performance optimization: Default to WARNING in production for less overhead
    default_level = "WARNING" if os.getenv('PRODUCTION', '').lower() == 'true' else log_level
    log_level = os.getenv('LOG_LEVEL', default_level).upper()
    log_file = os.getenv('LOG_FILE', log_file or 'aemo_dashboard.log')
    log_format = os.getenv('LOG_FORMAT', log_format or 
                          '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Determine logs directory
    if logs_dir:
        logs_path = Path(logs_dir)
    else:
        logs_dir_env = os.getenv('LOGS_DIR')
        if logs_dir_env:
            logs_path = Path(logs_dir_env)
        else:
            # Default to logs/ in project root
            project_root = Path(__file__).parent.parent.parent.parent
            logs_path = project_root / 'logs'
    
    # Create logs directory if it doesn't exist
    logs_path.mkdir(exist_ok=True)
    
    # Full path to log file
    log_file_path = logs_path / log_file
    
    # Clear any existing handlers
    logging.getLogger().handlers.clear()
    
    # Performance optimization: Only log to file in production, skip console
    handlers = []
    
    # Always add file handler
    file_handler = logging.FileHandler(log_file_path)
    handlers.append(file_handler)
    
    # Only add console handler if not in production or explicitly requested
    if os.getenv('PRODUCTION', '').lower() != 'true' or os.getenv('CONSOLE_LOGGING', '').lower() == 'true':
        handlers.append(logging.StreamHandler())
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_level),
        format=log_format,
        handlers=handlers
    )
    
    # Get logger for this module
    logger = logging.getLogger('aemo_dashboard')
    
    # Only log initialization message at INFO level or below
    if logger.isEnabledFor(logging.INFO):
        logger.info(f"Logging configured: level={log_level}, file={log_file_path}")
    
    return logger

def get_logger(name: str) -> logging.Logger:
    """
    Get a logger for a specific module.
    
    Args:
        name: Logger name (usually __name__)
        
    Returns:
        Logger instance
    """
    return logging.getLogger(f'aemo_dashboard.{name}')