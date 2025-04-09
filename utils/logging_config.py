"""
Centralized logging configuration for the Dremio benchmarking project.
"""

import logging
import sys
from pathlib import Path
from typing import Optional

def setup_logging(
    log_file: Optional[str] = None,
    module_name: Optional[str] = None,
    level: int = logging.INFO
) -> logging.Logger:
    """
    Set up logging configuration with consistent formatting.
    
    Args:
        log_file: Path to log file. If None, defaults to benchmark.log
        module_name: Name of the module for the logger. If None, uses root logger
        level: Logging level (default: INFO)
        
    Returns:
        Configured logger instance
    """
    # Set up default log file if none provided
    if log_file is None:
        log_file = "benchmark.log"
    
    # Create log directory if it doesn't exist
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Get logger instance
    logger = logging.getLogger(module_name) if module_name else logging.getLogger()
    logger.setLevel(level)
    
    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Set up file handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(level)
    
    # Set up console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(level)
    
    # Remove any existing handlers
    logger.handlers = []
    
    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def get_module_logger(module_name: str) -> logging.Logger:
    """
    Get a logger for a specific module.
    
    Args:
        module_name: Name of the module
        
    Returns:
        Logger instance for the module
    """
    return logging.getLogger(module_name)

# Default logging configuration
def configure_default_logging():
    """Configure default logging for the project"""
    setup_logging(
        log_file="benchmark.log",
        level=logging.INFO
    ) 