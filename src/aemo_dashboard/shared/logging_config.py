"""
Unified logging configuration for AEMO Energy Dashboard

Log Level Guidelines:
- CRITICAL: System cannot function (disk full, database unavailable)
- ERROR: Operation failed but system continues (data load failed, API error)
- WARNING: Unexpected but handled (unknown DUID, file retry needed, slow query)
- INFO: Significant milestones only (startup, shutdown, major operations complete)
- DEBUG: Development/troubleshooting (record counts, routine operations)
"""

import logging
import logging.handlers
import os
import sys
import time
import threading
from pathlib import Path
from typing import Optional

# Configuration defaults
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_MAX_BYTES = 10 * 1024 * 1024  # 10 MB per file
DEFAULT_BACKUP_COUNT = 9  # Keep 9 rotated files (100 MB total max)

_logging_initialized = False


class LogOnce:
    """
    Thread-safe utility to log messages only once per TTL period.
    
    Prevents log spam for repetitive conditions while allowing
    re-logging after TTL expires (so recurring issues are visible).
    
    Usage:
        log_once = LogOnce()
        log_once(logger, logging.WARNING, 'unknown_duids', 
                 f"Found {count} unknown DUIDs")
    """
    
    def __init__(self, maxsize: int = 1000, ttl_seconds: int = 3600):
        self._cache = {}
        self._lock = threading.Lock()
        self._maxsize = maxsize
        self._ttl = ttl_seconds
    
    def __call__(self, logger: logging.Logger, level: int, key: str, 
                 message: str) -> bool:
        """
        Log message if not recently logged for this key.
        
        Returns True if message was logged, False if suppressed.
        """
        now = time.time()
        
        with self._lock:
            # Clean expired entries if cache is getting full
            if len(self._cache) > self._maxsize:
                self._cache = {
                    k: v for k, v in self._cache.items() 
                    if now - v < self._ttl
                }
            
            # Log if key not seen or TTL expired
            if key not in self._cache or (now - self._cache[key]) > self._ttl:
                logger.log(level, message)
                self._cache[key] = now
                return True
        
        return False
    
    def reset(self, key: Optional[str] = None):
        """Reset cache for a specific key or all keys."""
        with self._lock:
            if key:
                self._cache.pop(key, None)
            else:
                self._cache.clear()


# Global instance for convenience
log_once = LogOnce()


def setup_logging(
    log_level: Optional[str] = None,
    log_file: Optional[str] = None,
    logs_dir: Optional[str] = None,
    max_bytes: int = DEFAULT_MAX_BYTES,
    backup_count: int = DEFAULT_BACKUP_COUNT
) -> logging.Logger:
    """
    Set up unified logging with rotation for all dashboard components.
    
    This should be called ONCE at application startup.
    Subsequent calls return the existing logger without reconfiguration.
    
    Args:
        log_level: Logging level (default: INFO, or from LOG_LEVEL env var)
        log_file: Log file name (default: aemo_dashboard.log)
        logs_dir: Directory for log files
        max_bytes: Max size per log file before rotation (default: 10MB)
        backup_count: Number of rotated files to keep (default: 9)
        
    Returns:
        Configured logger instance
    """
    global _logging_initialized
    
    # Prevent duplicate initialization
    if _logging_initialized:
        return logging.getLogger('aemo_dashboard')
    
    # Get configuration from environment or use defaults
    log_level = os.getenv('LOG_LEVEL', log_level or DEFAULT_LOG_LEVEL).upper()
    log_file = os.getenv('LOG_FILE', log_file or 'aemo_dashboard.log')
    
    # Determine logs directory
    if logs_dir:
        logs_path = Path(logs_dir)
    else:
        logs_dir_env = os.getenv('LOGS_DIR')
        if logs_dir_env:
            logs_path = Path(logs_dir_env)
        else:
            # Use production path on Mac Mini
            prod_path = Path('/Users/davidleitch/aemo_production/aemo-energy-dashboard2/logs')
            if prod_path.exists():
                logs_path = prod_path
            else:
                # Fallback to relative path for development
                project_root = Path(__file__).parent.parent.parent.parent
                logs_path = project_root / 'logs'
    
    # Create formatter
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(log_format)
    
    # Clear any existing handlers on root logger
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(getattr(logging, log_level))
    
    # Set up file handler with rotation (with graceful degradation)
    try:
        logs_path.mkdir(parents=True, exist_ok=True)
        log_file_path = logs_path / log_file
        
        file_handler = logging.handlers.RotatingFileHandler(
            log_file_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
        
    except (OSError, PermissionError) as e:
        # Fall back to stderr only - don't crash the dashboard
        sys.stderr.write(f"WARNING: Could not configure file logging: {e}\n")
        log_file_path = None
    
    # Add console handler for development or if explicitly requested
    if os.getenv('CONSOLE_LOGGING', '').lower() == 'true':
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    
    # Suppress verbose third-party loggers
    for noisy_logger in ['tornado', 'bokeh', 'panel', 'urllib3', 'asyncio']:
        logging.getLogger(noisy_logger).setLevel(logging.WARNING)
    
    _logging_initialized = True
    
    # Log startup message
    logger = logging.getLogger('aemo_dashboard')
    if log_file_path:
        logger.info(f"Dashboard logging initialized: {log_file_path} (level={log_level}, rotation={max_bytes//1024//1024}MB x {backup_count})")
    else:
        logger.warning("Dashboard logging initialized: console only (file logging failed)")
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger for a specific module.
    
    Args:
        name: Logger name (usually __name__)
        
    Returns:
        Logger instance under aemo_dashboard namespace
    """
    # Handle modules that already have aemo_dashboard prefix
    if name.startswith('aemo_dashboard.'):
        return logging.getLogger(name)
    return logging.getLogger(f'aemo_dashboard.{name}')
