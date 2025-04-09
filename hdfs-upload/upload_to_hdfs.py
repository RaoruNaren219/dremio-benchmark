#!/usr/bin/env python3

"""
TPC-DS HDFS Upload Script.
This script uploads the formatted TPC-DS data to HDFS clusters.
Uses Python native libraries instead of shell commands.
"""

import os
import argparse
import logging
import sys
import time
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
import platform
import subprocess

sys.path.append(str(Path(__file__).parent.parent))
from utils.command import run_command, PythonCommand

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("hdfs_upload.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# TPC-DS Tables
TPC_DS_TABLES = [
    "call_center", "catalog_page", "catalog_returns", "catalog_sales",
    "customer", "customer_address", "customer_demographics", "date_dim",
    "household_demographics", "income_band", "inventory", "item",
    "promotion", "reason", "ship_mode", "store", "store_returns",
    "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
    "web_sales", "web_site"
]

# Data formats
FORMATS = ["csv", "json", "pipe", "orc", "parquet"]

class HDFSPythonClient:
    """
    Python-native HDFS client that provides alternatives to shell commands.
    """
    
    def __init__(self, hadoop_bin: str, hadoop_conf: str, user: str = "hdfs"):
        """
        Initialize the HDFS client
        
        Args:
            hadoop_bin: Path to Hadoop binary
            hadoop_conf: Path to Hadoop configuration
            user: Hadoop user name
        """
        self.hadoop_bin = hadoop_bin
        self.hadoop_conf = hadoop_conf
        self.user = user
        
        # Check if running in WSL
        self.is_wsl = 'microsoft-standard' in platform.uname().release.lower() if platform.system() == 'Linux' else False
        
        # Set environment variables
        self.env = os.environ.copy()
        self.env["HADOOP_CONF_DIR"] = hadoop_conf
        self.env["HADOOP_USER_NAME"] = user
        
        # For WSL, ensure paths are properly formatted
        if self.is_wsl:
            self.hadoop_bin = self._convert_wsl_path(self.hadoop_bin)
            self.hadoop_conf = self._convert_wsl_path(self.hadoop_conf)
    
    def _convert_wsl_path(self, path: str) -> str:
        """
        Convert Windows path to WSL path if needed
        
        Args:
            path: Path to convert
            
        Returns:
            Converted path
        """
        if self.is_wsl and '\\' in path:
            # Convert Windows path to WSL path
            # Example: C:\path\to\file -> /mnt/c/path/to/file
            path = path.replace('\\', '/')
            if ':' in path:
                drive, rest = path.split(':', 1)
                path = f"/mnt/{drive.lower()}{rest}"
            return path
        return path
    
    def mkdir(self, hdfs_path: str) -> bool:
        """
        Create directory in HDFS
        
        Args:
            hdfs_path: HDFS path
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert path if needed
            hdfs_path = self._convert_wsl_path(hdfs_path)
            
            # Use subprocess to create directory
            cmd = [self.hadoop_bin, "fs", "-mkdir", "-p", hdfs_path]
            
            logger.info(f"Creating HDFS directory: {hdfs_path}")
            result = subprocess.run(cmd, env=self.env, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            logger.info(f"Successfully created HDFS directory: {hdfs_path}")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Error creating HDFS directory {hdfs_path}: {e}")
            logger.error(f"stdout: {e.stdout.decode('utf-8')}")
            logger.error(f"stderr: {e.stderr.decode('utf-8')}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error creating HDFS directory {hdfs_path}: {e}")
            return False
    
    def upload_file(self, local_file: str, hdfs_file: str) -> bool:
        """
        Upload file to HDFS
        
        Args:
            local_file: Local file path
            hdfs_file: HDFS target path
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert paths if needed
            local_file = self._convert_wsl_path(local_file)
            hdfs_file = self._convert_wsl_path(hdfs_file)
            
            # Use subprocess to upload file
            cmd = [self.hadoop_bin, "fs", "-put", "-f", local_file, hdfs_file]
            
            logger.info(f"Uploading file to HDFS: {local_file} -> {hdfs_file}")
            result = subprocess.run(cmd, env=self.env, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            logger.info(f"Successfully uploaded file to HDFS: {hdfs_file}")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Error uploading file to HDFS {local_file} -> {hdfs_file}: {e}")
            logger.error(f"stdout: {e.stdout.decode('utf-8')}")
            logger.error(f"stderr: {e.stderr.decode('utf-8')}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error uploading file to HDFS {local_file} -> {hdfs_file}: {e}")
            return False
    
    def upload_directory(self, local_dir: str, hdfs_dir: str) -> bool:
        """
        Upload directory to HDFS
        
        Args:
            local_dir: Local directory path
            hdfs_dir: HDFS target directory
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert paths if needed
            local_dir = self._convert_wsl_path(local_dir)
            hdfs_dir = self._convert_wsl_path(hdfs_dir)
            
            # Create target directory
            self.mkdir(hdfs_dir)
            
            # Walk through local directory and upload files
            for root, dirs, files in os.walk(local_dir):
                # Get relative path
                rel_path = os.path.relpath(root, local_dir)
                if rel_path == '.':
                    rel_path = ''
                
                # Create directories
                for dir_name in dirs:
                    hdfs_subdir = os.path.join(hdfs_dir, rel_path, dir_name)
                    self.mkdir(hdfs_subdir)
                
                # Upload files
                for file_name in files:
                    local_file = os.path.join(root, file_name)
                    hdfs_file = os.path.join(hdfs_dir, rel_path, file_name)
                    if not self.upload_file(local_file, hdfs_file):
                        return False
            
            return True
        except Exception as e:
            logger.error(f"Error uploading directory {local_dir} to {hdfs_dir}: {e}")
            return False

def upload_to_hdfs(
    data_dir: str,
    hdfs_target_dir: str,
    scale: int,
    format_type: str,
    hadoop_conf_dir: str,
    user: str = "hdfs",
    keytab: Optional[str] = None,
    principal: Optional[str] = None
) -> bool:
    """
    Upload data to HDFS cluster using Python native HDFS client.
    
    Args:
        data_dir: Path to local data directory
        hdfs_target_dir: HDFS target directory
        scale: Scale factor
        format_type: Data format
        hadoop_conf_dir: Path to Hadoop configuration
        user: Hadoop user name
        keytab: Optional path to the keytab file
        principal: Optional Kerberos principal
    
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Uploading {scale}GB {format_type} data to HDFS...")
    
    # Set environment variables
    old_conf_dir = os.environ.get('HADOOP_CONF_DIR')
    os.environ['HADOOP_CONF_DIR'] = hadoop_conf_dir
    
    try:
        # Initialize HDFS client
        hdfs_client = HDFSPythonClient(hadoop_bin=hadoop_conf_dir, hadoop_conf=hadoop_conf_dir, user=user)
        
        # Create target directory
        target_dir = f"{hdfs_target_dir}/{scale}gb/{format_type}"
        if not hdfs_client.mkdir(target_dir):
            return False
        
        # Upload each table
        for table in TPC_DS_TABLES:
            logger.info(f"Uploading {table}...")
            local_path = f"{data_dir}/{scale}gb/{format_type}/{table}"
            
            if not os.path.exists(local_path):
                logger.warning(f"Local path does not exist: {local_path}")
                continue
            
            target_path = f"{target_dir}/{table}"
            success = hdfs_client.upload_directory(local_path, target_path)
            
            if not success:
                logger.error(f"Failed to upload {table}")
                return False
                
            logger.info(f"Successfully uploaded {table} to HDFS")
        
        return True
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False
    
    finally:
        # Restore environment variables
        if old_conf_dir is not None:
            os.environ['HADOOP_CONF_DIR'] = old_conf_dir
        else:
            os.environ.pop('HADOOP_CONF_DIR', None)

def main():
    """Main function to upload TPC-DS data to HDFS clusters."""
    parser = argparse.ArgumentParser(description="Upload TPC-DS data to HDFS")
    parser.add_argument("--data-dir", default="../data/formatted", help="Path to formatted data")
    parser.add_argument("--hdfs-target-dir", default="/benchmark/tpcds", help="HDFS target directory")
    parser.add_argument(
        "--scale-factors", 
        nargs="+", 
        type=int, 
        default=[1, 10], 
        help="Scale factors in GB (default: 1 10)"
    )
    parser.add_argument(
        "--formats", 
        nargs="+", 
        default=FORMATS, 
        help=f"Data formats (default: {' '.join(FORMATS)})"
    )
    
    # HDFS options
    parser.add_argument(
        "--hadoop-conf", 
        required=True, 
        help="Path to Hadoop configuration"
    )
    parser.add_argument("--user", default="hdfs", help="Hadoop user name")
    
    # Optional Kerberos options
    parser.add_argument("--keytab", help="Path to keytab file")
    parser.add_argument("--principal", help="Kerberos principal")
    
    args = parser.parse_args()
    
    logger.info("Starting TPC-DS data upload to HDFS...")
    
    for scale in args.scale_factors:
        for format_type in args.formats:
            logger.info(f"Processing {scale}GB {format_type} data...")
            
            success = upload_to_hdfs(
                args.data_dir, 
                args.hdfs_target_dir, 
                scale, 
                format_type,
                args.hadoop_conf,
                args.user,
                args.keytab,
                args.principal
            )
            
            if not success:
                logger.error(f"Failed to upload {scale}GB {format_type} data to HDFS")
                return False
    
    logger.info("TPC-DS data upload completed successfully")
    return True

if __name__ == "__main__":
    sys.exit(0 if main() else 1) 