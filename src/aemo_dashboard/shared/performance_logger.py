"""
Simple performance logger for timing operations
"""
import time
from contextlib import contextmanager
from ..shared.logging_config import get_logger

logger = get_logger(__name__)

class PerformanceLogger:
    """Simple performance logger for timing operations"""
    
    def __init__(self, name):
        self.name = name
        self.logger = get_logger(name)
    
    @contextmanager
    def timer(self, operation_name, threshold=0.5):
        """Context manager for timing operations"""
        start_time = time.time()
        try:
            yield
        finally:
            duration = time.time() - start_time
            if duration > threshold:
                self.logger.info(f"Slow operation: {operation_name} took {duration:.2f}s (threshold: {threshold}s)")
            else:
                self.logger.debug(f"{operation_name} took {duration:.3f}s")