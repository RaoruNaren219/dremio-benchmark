"""
Filesystem utilities for the Dremio benchmark pipeline
"""

import os
import shutil
import logging
from typing import Dict, Optional, List

logger = logging.getLogger(__name__)

def create_directories(
    base_dir: str, 
    subdirs: Dict[str, str]
) -> Dict[str, str]:
    """
    Create directories for the benchmark pipeline
    
    Args:
        base_dir (str): Base directory
        subdirs (Dict[str, str]): Subdirectories relative to base_dir
    
    Returns:
        Dict[str, str]: Dictionary of absolute directory paths
    """
    directories = {}
    
    # Create base directory if it doesn't exist
    os.makedirs(base_dir, exist_ok=True)
    
    # Create subdirectories
    for name, rel_path in subdirs.items():
        abs_path = os.path.join(base_dir, rel_path)
        os.makedirs(abs_path, exist_ok=True)
        logger.info(f"Created directory: {abs_path}")
        directories[name] = abs_path
    
    return directories

def clean_directory(directory: str, confirm: bool = True) -> bool:
    """
    Clean a directory by removing its contents
    
    Args:
        directory (str): Directory to clean
        confirm (bool): Whether to confirm before cleaning
    
    Returns:
        bool: True if successful, False otherwise
    """
    if not os.path.exists(directory):
        logger.warning(f"Directory does not exist: {directory}")
        return False
    
    if not os.path.isdir(directory):
        logger.error(f"Not a directory: {directory}")
        return False
    
    try:
        if confirm:
            logger.warning(f"About to clean directory: {directory}")
            user_input = input("Are you sure? (y/n): ")
            if user_input.lower() != 'y':
                logger.info("Directory cleaning aborted")
                return False
        
        for item in os.listdir(directory):
            item_path = os.path.join(directory, item)
            
            if os.path.isdir(item_path):
                shutil.rmtree(item_path)
            else:
                os.remove(item_path)
        
        logger.info(f"Directory cleaned: {directory}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to clean directory {directory}: {e}")
        return False

def get_files_with_extension(
    directory: str, 
    extension: str,
    recursive: bool = False
) -> List[str]:
    """
    Get list of files with a specific extension
    
    Args:
        directory (str): Directory to search
        extension (str): File extension (with or without '.')
        recursive (bool): Whether to search recursively
    
    Returns:
        List[str]: List of file paths
    """
    # Normalize extension to start with a dot if not already
    if not extension.startswith('.'):
        extension = f".{extension}"
    
    result = []
    
    if not os.path.exists(directory):
        logger.warning(f"Directory does not exist: {directory}")
        return result
    
    if recursive:
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith(extension):
                    result.append(os.path.join(root, file))
    else:
        for file in os.listdir(directory):
            file_path = os.path.join(directory, file)
            if os.path.isfile(file_path) and file.endswith(extension):
                result.append(file_path)
    
    return result

def move_file(
    source: str, 
    destination: str, 
    overwrite: bool = False
) -> bool:
    """
    Move a file from source to destination
    
    Args:
        source (str): Source file path
        destination (str): Destination file path
        overwrite (bool): Whether to overwrite if destination exists
    
    Returns:
        bool: True if successful, False otherwise
    """
    if not os.path.exists(source):
        logger.error(f"Source file does not exist: {source}")
        return False
    
    if os.path.exists(destination) and not overwrite:
        logger.error(f"Destination file already exists: {destination}")
        return False
    
    try:
        # Create destination directory if it doesn't exist
        dest_dir = os.path.dirname(destination)
        if dest_dir:
            os.makedirs(dest_dir, exist_ok=True)
        
        shutil.move(source, destination)
        logger.info(f"Moved file from {source} to {destination}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to move file from {source} to {destination}: {e}")
        return False

def copy_file(
    source: str, 
    destination: str, 
    overwrite: bool = False
) -> bool:
    """
    Copy a file from source to destination
    
    Args:
        source (str): Source file path
        destination (str): Destination file path
        overwrite (bool): Whether to overwrite if destination exists
    
    Returns:
        bool: True if successful, False otherwise
    """
    if not os.path.exists(source):
        logger.error(f"Source file does not exist: {source}")
        return False
    
    if os.path.exists(destination) and not overwrite:
        logger.error(f"Destination file already exists: {destination}")
        return False
    
    try:
        # Create destination directory if it doesn't exist
        dest_dir = os.path.dirname(destination)
        if dest_dir:
            os.makedirs(dest_dir, exist_ok=True)
        
        shutil.copy2(source, destination)
        logger.info(f"Copied file from {source} to {destination}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to copy file from {source} to {destination}: {e}")
        return False 