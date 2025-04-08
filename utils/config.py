"""
Configuration utilities for the Dremio benchmark pipeline
"""

import os
import yaml
import logging
from typing import Dict, Any, Optional, List, Tuple, Union
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)

class Config:
    """Configuration manager for the Dremio benchmark pipeline"""
    
    def __init__(self, config_file: str = None):
        """
        Initialize configuration from file and environment variables
        
        Args:
            config_file: Path to YAML configuration file
        """
        self.config_data = {}
        if config_file:
            self.load_from_file(config_file)
    
    def load_from_file(self, config_file: str) -> bool:
        """
        Load configuration from YAML file
        
        Args:
            config_file: Path to YAML configuration file
            
        Returns:
            True if successful, False otherwise
        """
        if not os.path.exists(config_file):
            logger.warning(f"Configuration file not found: {config_file}")
            return False
        
        try:
            with open(config_file, 'r') as f:
                self.config_data = yaml.safe_load(f) or {}
            
            # Apply environment variable overrides
            self._apply_environment_overrides()
            
            logger.info(f"Loaded configuration from {config_file}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to load configuration from {config_file}: {e}")
            return False
    
    def _apply_environment_overrides(self) -> None:
        """Apply environment variable overrides to the configuration"""
        self._override_cluster_config()
        self._override_hdfs_config()
        self._override_pipeline_config()
        self._override_data_generation_config()
    
    def _override_cluster_config(self) -> None:
        """Override Dremio cluster configuration from environment variables"""
        if "clusters" not in self.config_data:
            self.config_data["clusters"] = {}
            
        for cluster in ["dremio_a", "dremio_b"]:
            if cluster not in self.config_data["clusters"]:
                self.config_data["clusters"][cluster] = {}
                
            cluster_config = self.config_data["clusters"][cluster]
            
            # Override host
            env_var = f"DREMIO_{cluster.upper()}_HOST"
            if env_var in os.environ:
                cluster_config["host"] = os.environ[env_var]
            
            # Override port
            env_var = f"DREMIO_{cluster.upper()}_PORT"
            if env_var in os.environ:
                try:
                    cluster_config["port"] = int(os.environ[env_var])
                except ValueError:
                    logger.warning(f"Invalid port in {env_var}: {os.environ[env_var]}")
            
            # Override username
            env_var = f"DREMIO_{cluster.upper()}_USERNAME"
            if env_var in os.environ:
                cluster_config["username"] = os.environ[env_var]
            
            # Override password
            env_var = f"DREMIO_{cluster.upper()}_PASSWORD"
            if env_var in os.environ:
                cluster_config["password"] = os.environ[env_var]
            
            # Override SSL
            env_var = f"DREMIO_{cluster.upper()}_SSL"
            if env_var in os.environ:
                ssl_value = os.environ[env_var].lower()
                cluster_config["ssl"] = ssl_value in ["true", "yes", "1"]
    
    def _override_hdfs_config(self) -> None:
        """Override HDFS configuration from environment variables"""
        if "hdfs" not in self.config_data:
            self.config_data["hdfs"] = {}
        
        # Kerberized HDFS configuration
        if "kerberized" not in self.config_data["hdfs"]:
            self.config_data["hdfs"]["kerberized"] = {}
            
        kerberized_config = self.config_data["hdfs"]["kerberized"]
        
        if "DREMIO_KERBERIZED_PRINCIPAL" in os.environ:
            kerberized_config["principal"] = os.environ["DREMIO_KERBERIZED_PRINCIPAL"]
            
        if "DREMIO_KERBERIZED_KEYTAB" in os.environ:
            kerberized_config["keytab"] = os.environ["DREMIO_KERBERIZED_KEYTAB"]
            
        if "DREMIO_KERBERIZED_HADOOP_BIN" in os.environ:
            kerberized_config["hadoop_bin"] = os.environ["DREMIO_KERBERIZED_HADOOP_BIN"]
            
        if "DREMIO_KERBERIZED_HADOOP_CONF" in os.environ:
            kerberized_config["hadoop_conf"] = os.environ["DREMIO_KERBERIZED_HADOOP_CONF"]
        
        # Simple auth HDFS configuration
        if "simple_auth" not in self.config_data["hdfs"]:
            self.config_data["hdfs"]["simple_auth"] = {}
            
        simple_auth_config = self.config_data["hdfs"]["simple_auth"]
        
        if "DREMIO_SIMPLE_AUTH_USER" in os.environ:
            simple_auth_config["user"] = os.environ["DREMIO_SIMPLE_AUTH_USER"]
            
        if "DREMIO_SIMPLE_AUTH_HADOOP_BIN" in os.environ:
            simple_auth_config["hadoop_bin"] = os.environ["DREMIO_SIMPLE_AUTH_HADOOP_BIN"]
            
        if "DREMIO_SIMPLE_AUTH_HADOOP_CONF" in os.environ:
            simple_auth_config["hadoop_conf"] = os.environ["DREMIO_SIMPLE_AUTH_HADOOP_CONF"]
    
    def _override_pipeline_config(self) -> None:
        """Override pipeline configuration from environment variables"""
        if "pipeline" not in self.config_data:
            self.config_data["pipeline"] = {}
            
        pipeline_config = self.config_data["pipeline"]
        
        if "DREMIO_HDFS_TARGET_DIR" in os.environ:
            pipeline_config["hdfs_target_dir"] = os.environ["DREMIO_HDFS_TARGET_DIR"]
            
        if "DREMIO_QUERY_DIR" in os.environ:
            pipeline_config["query_dir"] = os.environ["DREMIO_QUERY_DIR"]
            
        if "DREMIO_QUERY_TIMEOUT_SECONDS" in os.environ:
            try:
                pipeline_config["timeout_seconds"] = int(os.environ["DREMIO_QUERY_TIMEOUT_SECONDS"])
            except ValueError:
                logger.warning(f"Invalid timeout: {os.environ['DREMIO_QUERY_TIMEOUT_SECONDS']}")
                
        if "DREMIO_BENCHMARK_ITERATIONS" in os.environ:
            try:
                pipeline_config["num_iterations"] = int(os.environ["DREMIO_BENCHMARK_ITERATIONS"])
            except ValueError:
                logger.warning(f"Invalid iterations: {os.environ['DREMIO_BENCHMARK_ITERATIONS']}")
                
        if "DREMIO_SCALE_FACTORS" in os.environ:
            try:
                scale_factors = [int(sf) for sf in os.environ["DREMIO_SCALE_FACTORS"].split(",")]
                pipeline_config["scale_factors"] = scale_factors
            except ValueError:
                logger.warning(f"Invalid scale factors: {os.environ['DREMIO_SCALE_FACTORS']}")
                
        if "DREMIO_FORMATS" in os.environ:
            formats = [fmt.strip() for fmt in os.environ["DREMIO_FORMATS"].split(",")]
            pipeline_config["formats"] = formats
    
    def _override_data_generation_config(self) -> None:
        """Override data generation configuration from environment variables"""
        if "data_generation" not in self.config_data:
            self.config_data["data_generation"] = {}
            
        data_gen_config = self.config_data["data_generation"]
        
        if "DREMIO_DSDGEN_PATH" in os.environ:
            data_gen_config["dsdgen_path"] = os.environ["DREMIO_DSDGEN_PATH"]
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value
        
        Args:
            key: Configuration key (can use dot notation for nested keys)
            default: Default value if key is not found
            
        Returns:
            Configuration value or default
        """
        if "." in key:
            # Handle nested keys with dot notation
            keys = key.split(".")
            value = self.config_data
            
            for k in keys:
                if isinstance(value, dict) and k in value:
                    value = value[k]
                else:
                    return default
                    
            return value
        
        return self.config_data.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """
        Set a configuration value
        
        Args:
            key: Configuration key (can use dot notation for nested keys)
            value: Value to set
        """
        if "." in key:
            # Handle nested keys with dot notation
            keys = key.split(".")
            data = self.config_data
            
            # Navigate to the parent object
            for k in keys[:-1]:
                if k not in data:
                    data[k] = {}
                data = data[k]
                
            # Set the value
            data[keys[-1]] = value
        else:
            self.config_data[key] = value
    
    def get_cluster_config(self, cluster_name: str) -> Dict[str, Any]:
        """
        Get configuration for a specific cluster
        
        Args:
            cluster_name: Cluster name (e.g., 'dremio_a', 'dremio_b')
        
        Returns:
            Cluster configuration
        """
        clusters = self.config_data.get("clusters", {})
        return clusters.get(cluster_name, {})
    
    def get_hdfs_config(self, cluster_name: str) -> Dict[str, Any]:
        """
        Get HDFS configuration for a specific cluster
        
        Args:
            cluster_name: Cluster name (e.g., 'simple_auth', 'kerberized')
        
        Returns:
            HDFS configuration
        """
        hdfs_config = self.config_data.get("hdfs", {})
        return hdfs_config.get(cluster_name, {})
    
    def save_to_file(self, output_file: str) -> bool:
        """
        Save configuration to YAML file
        
        Args:
            output_file: Path to output YAML file
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create directory if it doesn't exist
            output_dir = os.path.dirname(output_file)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
            
            with open(output_file, 'w') as f:
                yaml.dump(self.config_data, f, default_flow_style=False)
            
            logger.info(f"Configuration saved to {output_file}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to save configuration to {output_file}: {e}")
            return False
    
    def validate(self, step: str) -> Tuple[bool, List[str]]:
        """
        Validate configuration for a specific pipeline step
        
        Args:
            step: Pipeline step to validate
        
        Returns:
            (is_valid, error_messages)
        """
        errors = []
        
        # Common validations
        if not self.config_data:
            errors.append("Configuration is empty")
            return False, errors
        
        # Step-specific validations
        if step == "data":
            data_gen_config = self.config_data.get("data_generation", {})
            if not data_gen_config.get("dsdgen_path"):
                errors.append("data_generation.dsdgen_path is required for data generation step")
        
        elif step == "upload":
            # Validate HDFS configs
            simple_auth = self.get_hdfs_config("simple_auth")
            kerberized = self.get_hdfs_config("kerberized")
            
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
            dremio_a = self.get_cluster_config("dremio_a")
            dremio_b = self.get_cluster_config("dremio_b")
            
            if not dremio_a.get("host"):
                errors.append("clusters.dremio_a.host is required")
                
            if not dremio_b.get("host"):
                errors.append("clusters.dremio_b.host is required")
                
            if step == "benchmark":
                pipeline_config = self.config_data.get("pipeline", {})
                if not pipeline_config.get("query_dir"):
                    errors.append("pipeline.query_dir is required for benchmark step")
        
        return len(errors) == 0, errors
    
    def merge(self, override_config: Dict[str, Any]) -> None:
        """
        Merge override configuration into this configuration
        
        Args:
            override_config: Override configuration
        """
        self.config_data = merge_configs(self.config_data, override_config)
    
    def get_all(self) -> Dict[str, Any]:
        """
        Get complete configuration data
        
        Returns:
            Complete configuration dictionary
        """
        return self.config_data


def load_config(config_file: str) -> Dict[str, Any]:
    """
    Legacy function to load configuration from YAML file
    
    Args:
        config_file: Path to YAML configuration file
    
    Returns:
        Configuration dictionary
    """
    config = Config()
    config.load_from_file(config_file)
    return config.get_all()


def get_cluster_config(
    config: Dict[str, Any], 
    cluster_name: str
) -> Dict[str, Any]:
    """
    Legacy function to get configuration for a specific cluster
    
    Args:
        config: Configuration dictionary
        cluster_name: Cluster name (e.g., 'dremio_a', 'dremio_b')
    
    Returns:
        Cluster configuration
    """
    clusters = config.get("clusters", {})
    return clusters.get(cluster_name, {})


def get_hdfs_config(
    config: Dict[str, Any], 
    cluster_name: str
) -> Dict[str, Any]:
    """
    Legacy function to get HDFS configuration for a specific cluster
    
    Args:
        config: Configuration dictionary
        cluster_name: Cluster name (e.g., 'simple_auth', 'kerberized')
    
    Returns:
        HDFS configuration
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
        base_config: Base configuration
        override_config: Override configuration
    
    Returns:
        Merged configuration
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
    Legacy function to save configuration to YAML file
    
    Args:
        config: Configuration dictionary
        output_file: Path to output YAML file
    
    Returns:
        True if successful, False otherwise
    """
    config_obj = Config()
    config_obj.merge(config)
    return config_obj.save_to_file(output_file)


def validate_config(config: Dict[str, Any], step: str) -> Tuple[bool, List[str]]:
    """
    Legacy function to validate configuration for a specific pipeline step
    
    Args:
        config: Configuration dictionary
        step: Pipeline step to validate
    
    Returns:
        (is_valid, error_messages)
    """
    config_obj = Config()
    config_obj.merge(config)
    return config_obj.validate(step) 