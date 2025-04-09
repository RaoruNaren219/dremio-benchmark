#!/usr/bin/env python3

"""
TPC-DS HDFS Upload Script.
This script uploads the formatted TPC-DS data to HDFS clusters.
Uses subprocess to call Hadoop commands directly.
"""

import os
import argparse
import sys
import time
import subprocess
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
import platform

sys.path.append(str(Path(__file__).parent.parent))
from utils.logging_config import setup_logging

# Configure logging
logger = setup_logging(
    log_file="hdfs_upload.log",
    module_name="dremio_benchmark.hdfs_upload"
)

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

class HDFSClient:
    """
    HDFS client that uses subprocess to call Hadoop commands directly.
    """
    
    def __init__(self, hadoop_conf: str, user: str = "hdfs"):
        """
        Initialize the HDFS client
        
        Args:
            hadoop_conf: Path to Hadoop configuration
            user: Hadoop user name
        """
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
            self.hadoop_conf = self._convert_wsl_path(self.hadoop_conf)
            
        # Verify Hadoop commands are available
        self._verify_hadoop_commands()
    
    def _verify_hadoop_commands(self) -> None:
        """
        Verify that Hadoop commands are available in the system path
        """
        try:
            subprocess.run(["hadoop", "version"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except (subprocess.CalledProcessError, FileNotFoundError):
            logger.error("Hadoop commands not found. Please ensure Hadoop is installed and available in the system path.")
            raise RuntimeError("Hadoop commands not found. Please ensure Hadoop is installed and available in the system path.")
    
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
    
    def _run_hadoop_command(self, cmd: List[str], description: str) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Run a Hadoop command using subprocess
        
        Args:
            cmd: Command to run
            description: Description for logging
            
        Returns:
            Tuple of success status, stdout, stderr
        """
        logger.info(f"Running {description}...")
        logger.debug(f"Command: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(
                cmd,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=self.env
            )
            
            stdout = result.stdout.decode('utf-8')
            stderr = result.stderr.decode('utf-8')
            
            logger.info(f"{description} completed successfully")
            return True, stdout, stderr
            
        except subprocess.CalledProcessError as e:
            logger.error(f"{description} failed: {e}")
            logger.error(f"stdout: {e.stdout.decode('utf-8')}")
            logger.error(f"stderr: {e.stderr.decode('utf-8')}")
            return False, e.stdout.decode('utf-8'), e.stderr.decode('utf-8')
            
        except Exception as e:
            logger.error(f"Error running {description}: {str(e)}")
            return False, None, str(e)
    
    def mkdir(self, hdfs_path: str) -> bool:
        """
        Create directory in HDFS
        
        Args:
            hdfs_path: HDFS path to create
            
        Returns:
            True if successful, False otherwise
        """
        # Construct command
        cmd = [
            "hadoop", "fs", "-mkdir", "-p", hdfs_path
        ]
        
        # Execute command
        success, _, _ = self._run_hadoop_command(
            cmd,
            f"Creating HDFS directory: {hdfs_path}"
        )
        
        return success
    
    def upload_file(self, local_file: str, hdfs_file: str) -> bool:
        """
        Upload a file to HDFS
        
        Args:
            local_file: Local file path
            hdfs_file: HDFS file path
            
        Returns:
            True if successful, False otherwise
        """
        # Convert paths for WSL if needed
        if self.is_wsl:
            local_file = self._convert_wsl_path(local_file)
        
        # Construct command
        cmd = [
            "hadoop", "fs", "-put", "-f", local_file, hdfs_file
        ]
        
        # Execute command
        success, _, _ = self._run_hadoop_command(
            cmd,
            f"Uploading file: {local_file} to {hdfs_file}"
        )
        
        return success
    
    def upload_directory(self, local_dir: str, hdfs_dir: str) -> bool:
        """
        Upload a directory to HDFS
        
        Args:
            local_dir: Local directory path
            hdfs_dir: HDFS directory path
            
        Returns:
            True if successful, False otherwise
        """
        # Convert paths for WSL if needed
        if self.is_wsl:
            local_dir = self._convert_wsl_path(local_dir)
        
        # Construct command
        cmd = [
            "hadoop", "fs", "-put", "-f", local_dir, hdfs_dir
        ]
        
        # Execute command
        success, _, _ = self._run_hadoop_command(
            cmd,
            f"Uploading directory: {local_dir} to {hdfs_dir}"
        )
        
        return success

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
    Upload data to HDFS cluster using Hadoop commands.
    
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
        hdfs_client = HDFSClient(hadoop_conf=hadoop_conf_dir, user=user)
        
        # Create target directory
        target_dir = f"{hdfs_target_dir}/{scale}gb/{format_type}"
        if not hdfs_client.mkdir(target_dir):
            return False
        
        # Upload each table
        for table in TPC_DS_TABLES:
            logger.info(f"Uploading {table}...")
            local_path = f"{data_dir}/{scale}gb/{format_type}/{table}"
            
            if not os.path.exists(local_path):
                logger.error(f"Local path does not exist: {local_path}")
                return False
            
            hdfs_path = f"{target_dir}/{table}"
            
            # Upload file or directory
            if os.path.isfile(local_path):
                if not hdfs_client.upload_file(local_path, hdfs_path):
                    return False
            else:
                if not hdfs_client.upload_directory(local_path, hdfs_path):
                    return False
        
        logger.info(f"Successfully uploaded {scale}GB {format_type} data to HDFS")
        return True
        
    except Exception as e:
        logger.error(f"Error uploading data to HDFS: {e}")
        return False
        
    finally:
        # Restore environment variables
        if old_conf_dir:
            os.environ['HADOOP_CONF_DIR'] = old_conf_dir
        else:
            del os.environ['HADOOP_CONF_DIR']

def main():
    """Main function to upload data to HDFS"""
    parser = argparse.ArgumentParser(description="Upload TPC-DS data to HDFS")
    parser.add_argument("--data-dir", required=True, help="Path to local data directory")
    parser.add_argument("--hdfs-target-dir", required=True, help="HDFS target directory")
    parser.add_argument("--scale", type=int, required=True, help="Scale factor in GB")
    parser.add_argument("--format", required=True, help="Data format")
    parser.add_argument("--hadoop-conf", required=True, help="Path to Hadoop configuration")
    parser.add_argument("--user", default="hdfs", help="Hadoop user name")
    parser.add_argument("--keytab", help="Path to keytab file")
    parser.add_argument("--principal", help="Kerberos principal")
    
    args = parser.parse_args()
    
    logger.info("Starting TPC-DS data upload to HDFS...")
    
    success = upload_to_hdfs(
        args.data_dir,
        args.hdfs_target_dir,
        args.scale,
        args.format,
        args.hadoop_conf,
        args.user,
        args.keytab,
        args.principal
    )
    
    if not success:
        logger.error("Failed to upload data to HDFS")
        sys.exit(1)
    
    logger.info("TPC-DS data upload completed successfully!")

if __name__ == "__main__":
    main() 