"""
Logging configuration utilities
"""

import logging
import sys
import os
from typing import Optional

def setup_logging(
    log_file: Optional[str] = None,
    log_level: int = logging.INFO,
    module_name: str = "dremio_benchmark"
) -> logging.Logger:
    """
    Configure logging for a module
    
    Args:
        log_file (Optional[str]): Path to log file
        log_level (int): Logging level
        module_name (str): Module name for the logger
    
    Returns:
        logging.Logger: Configured logger
    """
    # Create logger
    logger = logging.getLogger(module_name)
    logger.setLevel(log_level)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Create file handler if log_file is provided
    if log_file:
        # Create directory if it doesn't exist
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
            
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger 