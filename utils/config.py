"""
Configuration utilities for the Dremio benchmark pipeline
"""

import os
import yaml
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

def load_config(config_file: str) -> Dict[str, Any]:
    """
    Load configuration from YAML file
    
    Args:
        config_file (str): Path to YAML configuration file
    
    Returns:
        Dict[str, Any]: Configuration dictionary
    """
    if not os.path.exists(config_file):
        logger.warning(f"Configuration file not found: {config_file}")
        return {}
    
    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        if not config:
            logger.warning(f"Empty configuration file: {config_file}")
            return {}
        
        logger.info(f"Loaded configuration from {config_file}")
        return config
    
    except Exception as e:
        logger.error(f"Failed to load configuration from {config_file}: {e}")
        return {}

def get_cluster_config(
    config: Dict[str, Any], 
    cluster_name: str
) -> Dict[str, Any]:
    """
    Get configuration for a specific cluster
    
    Args:
        config (Dict[str, Any]): Configuration dictionary
        cluster_name (str): Cluster name (e.g., 'dremio_a', 'dremio_b')
    
    Returns:
        Dict[str, Any]: Cluster configuration
    """
    clusters = config.get("clusters", {})
    return clusters.get(cluster_name, {})

def get_hdfs_config(
    config: Dict[str, Any], 
    cluster_name: str
) -> Dict[str, Any]:
    """
    Get HDFS configuration for a specific cluster
    
    Args:
        config (Dict[str, Any]): Configuration dictionary
        cluster_name (str): Cluster name (e.g., 'simple_auth', 'kerberized')
    
    Returns:
        Dict[str, Any]: HDFS configuration
    """
    hdfs_config = config.get("hdfs", {})
    return hdfs_config.get(cluster_name, {})

def merge_configs(
    base_config: Dict[str, Any], 
    override_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Merge two configuration dictionaries
    
    Args:
        base_config (Dict[str, Any]): Base configuration
        override_config (Dict[str, Any]): Override configuration
    
    Returns:
        Dict[str, Any]: Merged configuration
    """
    result = base_config.copy()
    
    for key, value in override_config.items():
        if isinstance(value, dict) and key in result and isinstance(result[key], dict):
            result[key] = merge_configs(result[key], value)
        else:
            result[key] = value
    
    return result

def save_config(config: Dict[str, Any], output_file: str) -> bool:
    """
    Save configuration to YAML file
    
    Args:
        config (Dict[str, Any]): Configuration dictionary
        output_file (str): Path to output YAML file
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Create directory if it doesn't exist
        output_dir = os.path.dirname(output_file)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        
        with open(output_file, 'w') as f:
            yaml.dump(config, f, default_flow_style=False)
        
        logger.info(f"Configuration saved to {output_file}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to save configuration to {output_file}: {e}")
        return False 