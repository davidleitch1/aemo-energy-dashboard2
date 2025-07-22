#!/usr/bin/env python3
"""
Logging configuration for AEMO Data Service
Uses the existing dashboard logging system.
"""

# Import from the existing dashboard logging system
from aemo_dashboard.shared.logging_config import setup_logging as dashboard_setup_logging, get_logger
from .config import config

def setup_logging(console_output: bool = True) -> None:
    """Set up logging using the existing dashboard system."""
    # Use the existing dashboard logging setup
    dashboard_setup_logging()

def configure_service_logging():
    """Configure logging for the entire data service."""
    setup_logging(console_output=True)
    
    logger = get_logger(__name__)
    logger.info("AEMO Data Service logging initialized")
    logger.info(config.get_summary())