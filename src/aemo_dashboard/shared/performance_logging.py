"""
Performance-aware logging utilities for AEMO Energy Dashboard.

This module provides optimized logging functions that reduce overhead
by only logging when necessary and batching operations.
"""

import time
import logging
from contextlib import contextmanager
from functools import wraps
from typing import Optional, Dict, Any

from .logging_config import get_logger

logger = get_logger(__name__)

# Performance thresholds for logging
SLOW_OPERATION_THRESHOLD = 1.0  # seconds
DATA_LOAD_THRESHOLD = 0.5  # seconds
LARGE_DATASET_THRESHOLD = 100_000  # records

class PerformanceLogger:
    """
    A performance-aware logger that only logs slow operations
    and provides context managers for timing.
    """
    
    def __init__(self, logger_name: str):
        self.logger = get_logger(logger_name)
        self._timing_data: Dict[str, float] = {}
    
    @contextmanager
    def timer(self, operation_name: str, threshold: float = SLOW_OPERATION_THRESHOLD):
        """
        Context manager that times an operation and only logs if it's slow.
        
        Usage:
            with perf_logger.timer("data_loading", threshold=0.5):
                # Load data here
                pass
        """
        start_time = time.time()
        
        yield
        
        duration = time.time() - start_time
        self._timing_data[operation_name] = duration
        
        # Only log if operation was slow
        if duration > threshold:
            self.logger.warning(
                f"Slow operation: {operation_name} took {duration:.2f}s "
                f"(threshold: {threshold}s)"
            )
        elif self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f"{operation_name} completed in {duration:.2f}s")
    
    def log_data_operation(self, operation: str, record_count: int, 
                          duration: Optional[float] = None,
                          metadata: Optional[Dict[str, Any]] = None):
        """
        Log data operations with smart thresholds.
        Only logs if:
        - Operation is slow
        - Dataset is large
        - Logger is in DEBUG mode
        """
        # Skip logging for small, fast operations unless in DEBUG
        if (record_count < LARGE_DATASET_THRESHOLD and 
            (duration is None or duration < DATA_LOAD_THRESHOLD) and
            not self.logger.isEnabledFor(logging.DEBUG)):
            return
        
        # Build log message
        msg_parts = [f"{operation}: {record_count:,} records"]
        
        if duration is not None:
            msg_parts.append(f"in {duration:.2f}s")
            if record_count > 0 and duration > 0:
                rate = record_count / duration
                msg_parts.append(f"({rate:,.0f} records/s)")
        
        if metadata:
            meta_str = ", ".join(f"{k}={v}" for k, v in metadata.items())
            msg_parts.append(f"[{meta_str}]")
        
        message = " ".join(msg_parts)
        
        # Choose log level based on performance
        if duration and duration > SLOW_OPERATION_THRESHOLD:
            self.logger.warning(f"Slow: {message}")
        elif record_count > LARGE_DATASET_THRESHOLD:
            self.logger.info(message)
        else:
            self.logger.debug(message)
    
    def get_timing_summary(self) -> Dict[str, float]:
        """Get all timing data collected."""
        return self._timing_data.copy()

def performance_monitor(threshold: float = SLOW_OPERATION_THRESHOLD):
    """
    Decorator that monitors function performance and logs slow operations.
    
    Usage:
        @performance_monitor(threshold=0.5)
        def load_data():
            # Function implementation
            pass
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start_time
                
                if duration > threshold:
                    logger = get_logger(func.__module__)
                    logger.warning(
                        f"Slow function: {func.__name__} took {duration:.2f}s "
                        f"(threshold: {threshold}s)"
                    )
        
        return wrapper
    return decorator

def conditional_log(logger_instance: logging.Logger, level: int, 
                   message: str, condition: bool = True):
    """
    Only log if condition is met and logger level allows it.
    
    This avoids string formatting overhead when logging is disabled.
    """
    if condition and logger_instance.isEnabledFor(level):
        logger_instance.log(level, message)

# Create a global performance logger instance
perf_logger = PerformanceLogger('aemo_dashboard.performance')