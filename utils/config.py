"""
Configuration utilities for the Dremio benchmark pipeline
"""

import os
import yaml
import logging
from typing import Dict, Any, Optional, List, Tuple
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

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
        
        # Apply environment variable overrides
        config = apply_environment_overrides(config)
        
        logger.info(f"Loaded configuration from {config_file}")
        return config
    
    except Exception as e:
        logger.error(f"Failed to load configuration from {config_file}: {e}")
        return {}

def apply_environment_overrides(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Override configuration values with environment variables
    
    Environment variables should be prefixed with DREMIO_
    Example: DREMIO_CLUSTERS_DREMIO_A_PASSWORD=mysecret
    
    Args:
        config (Dict[str, Any]): Original configuration
        
    Returns:
        Dict[str, Any]: Configuration with environment overrides
    """
    # Handle Dremio cluster credentials
    if "clusters" in config:
        for cluster in ["dremio_a", "dremio_b"]:
            if cluster in config["clusters"]:
                # Override host if specified
                env_host = os.environ.get(f"DREMIO_{cluster.upper()}_HOST")
                if env_host:
                    config["clusters"][cluster]["host"] = env_host
                
                # Override username if specified
                env_username = os.environ.get(f"DREMIO_{cluster.upper()}_USERNAME")
                if env_username:
                    config["clusters"][cluster]["username"] = env_username
                
                # Override password if specified
                env_password = os.environ.get(f"DREMIO_{cluster.upper()}_PASSWORD")
                if env_password:
                    config["clusters"][cluster]["password"] = env_password
    
    # Handle HDFS configuration
    if "hdfs" in config:
        # Handle kerberized cluster credentials
        if "kerberized" in config["hdfs"]:
            env_principal = os.environ.get("DREMIO_KERBERIZED_PRINCIPAL")
            if env_principal:
                config["hdfs"]["kerberized"]["principal"] = env_principal
            
            env_keytab = os.environ.get("DREMIO_KERBERIZED_KEYTAB")
            if env_keytab:
                config["hdfs"]["kerberized"]["keytab"] = env_keytab
        
        # Handle simple auth cluster credentials
        if "simple_auth" in config["hdfs"]:
            env_user = os.environ.get("DREMIO_SIMPLE_AUTH_USER")
            if env_user:
                config["hdfs"]["simple_auth"]["user"] = env_user
    
    return config

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

def validate_config(config: Dict[str, Any], step: str) -> Tuple[bool, List[str]]:
    """
    Validate configuration for a specific pipeline step
    
    Args:
        config (Dict[str, Any]): Configuration dictionary
        step (str): Pipeline step to validate
    
    Returns:
        Tuple[bool, List[str]]: (is_valid, error_messages)
    """
    errors = []
    
    # Common validations
    if not config:
        errors.append("Configuration is empty")
        return False, errors
    
    # Step-specific validations
    if step == "data":
        data_gen_config = config.get("data_generation", {})
        if not data_gen_config.get("dsdgen_path"):
            errors.append("data_generation.dsdgen_path is required for data generation step")
    
    elif step == "upload":
        # Validate HDFS configs
        simple_auth = get_hdfs_config(config, "simple_auth")
        kerberized = get_hdfs_config(config, "kerberized")
        
        if not simple_auth.get("hadoop_conf"):
            errors.append("hdfs.simple_auth.hadoop_conf is required for upload step")
        
        if not kerberized.get("hadoop_conf"):
            errors.append("hdfs.kerberized.hadoop_conf is required for upload step")
            
        if not kerberized.get("keytab"):
            errors.append("hdfs.kerberized.keytab is required for upload step")
            
        if not kerberized.get("principal"):
            errors.append("hdfs.kerberized.principal is required for upload step")
    
    elif step in ["ddl", "cross", "benchmark"]:
        # Validate Dremio configs
        dremio_a = get_cluster_config(config, "dremio_a")
        dremio_b = get_cluster_config(config, "dremio_b")
        
        if not dremio_a.get("host"):
            errors.append("clusters.dremio_a.host is required")
            
        if not dremio_b.get("host"):
            errors.append("clusters.dremio_b.host is required")
            
        if step == "benchmark":
            pipeline_config = config.get("pipeline", {})
            if not pipeline_config.get("query_dir"):
                errors.append("pipeline.query_dir is required for benchmark step")
    
    return len(errors) == 0, errors 