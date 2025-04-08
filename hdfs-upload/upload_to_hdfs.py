#!/usr/bin/env python3

"""
TPC-DS HDFS Upload Script.
This script uploads the formatted TPC-DS data to both HDFS clusters (Simple-auth and Kerberized).
"""

import os
import argparse
import logging
import sys
import time
from pathlib import Path
from typing import List, Optional

sys.path.append(str(Path(__file__).parent.parent))
from utils.command import run_command

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


def upload_to_simple_auth_hdfs(
    data_dir: str,
    hdfs_target_dir: str,
    scale: int,
    format_type: str,
    hadoop_bin: str,
    hadoop_conf_dir: str,
    user: str
) -> bool:
    """
    Upload data to Simple-auth HDFS cluster.
    
    Args:
        data_dir: Path to local data directory
        hdfs_target_dir: HDFS target directory
        scale: Scale factor
        format_type: Data format
        hadoop_bin: Path to Hadoop binary
        hadoop_conf_dir: Path to Hadoop configuration
        user: Hadoop user name
    
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Uploading {scale}GB {format_type} data to Simple-auth HDFS...")
    
    # Set environment variables
    env = os.environ.copy()
    env["HADOOP_USER_NAME"] = user
    env["HADOOP_CONF_DIR"] = hadoop_conf_dir
    
    try:
        # Create target directory
        target_dir = f"{hdfs_target_dir}/{scale}gb/{format_type}"
        mkdir_cmd = [hadoop_bin, "fs", "-mkdir", "-p", target_dir]
        success, stdout, stderr = run_command(mkdir_cmd, "Create HDFS directory", env=env)
        if not success:
            logger.error(f"Failed to create directory: {stderr}")
            return False
        
        # Upload each table
        for table in TPC_DS_TABLES:
            logger.info(f"Uploading {table}...")
            local_path = f"{data_dir}/{scale}gb/{format_type}/{table}"
            
            if not os.path.exists(local_path):
                logger.warning(f"Local path does not exist: {local_path}")
                continue
            
            put_cmd = [hadoop_bin, "fs", "-put", "-f", local_path, f"{target_dir}/"]
            success, stdout, stderr = run_command(put_cmd, f"Upload {table}", env=env)
            if not success:
                logger.error(f"Failed to upload {table}: {stderr}")
                return False
                
            logger.info(f"Successfully uploaded {table} to Simple-auth HDFS")
        
        return True
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False


def upload_to_kerberized_hdfs(
    data_dir: str,
    hdfs_target_dir: str,
    scale: int,
    format_type: str,
    hadoop_bin: str,
    hadoop_conf_dir: str,
    keytab: str,
    principal: str
) -> bool:
    """
    Upload data to Kerberized HDFS cluster.
    
    Args:
        data_dir: Path to local data directory
        hdfs_target_dir: HDFS target directory
        scale: Scale factor
        format_type: Data format
        hadoop_bin: Path to Hadoop binary
        hadoop_conf_dir: Path to Hadoop configuration
        keytab: Path to the keytab file
        principal: Kerberos principal
    
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Uploading {scale}GB {format_type} data to Kerberized HDFS...")
    
    # Set environment variables
    env = os.environ.copy()
    env["HADOOP_CONF_DIR"] = hadoop_conf_dir
    
    try:
        # Obtain Kerberos ticket if not already present
        klist_cmd = ["klist", "-s"]
        success, _, _ = run_command(klist_cmd, "Check Kerberos ticket")
        
        if not success:
            logger.info("Obtaining Kerberos ticket...")
            kinit_cmd = ["kinit", "-kt", keytab, principal]
            success, stdout, stderr = run_command(kinit_cmd, "Obtain Kerberos ticket")
            if not success:
                logger.error(f"Failed to obtain Kerberos ticket: {stderr}")
                return False
        
        # Create target directory
        target_dir = f"{hdfs_target_dir}/{scale}gb/{format_type}"
        mkdir_cmd = [hadoop_bin, "fs", "-mkdir", "-p", target_dir]
        success, stdout, stderr = run_command(mkdir_cmd, "Create HDFS directory", env=env)
        if not success:
            logger.error(f"Failed to create directory: {stderr}")
            return False
        
        # Upload each table
        for table in TPC_DS_TABLES:
            logger.info(f"Uploading {table}...")
            local_path = f"{data_dir}/{scale}gb/{format_type}/{table}"
            
            if not os.path.exists(local_path):
                logger.warning(f"Local path does not exist: {local_path}")
                continue
            
            put_cmd = [hadoop_bin, "fs", "-put", "-f", local_path, f"{target_dir}/"]
            success, stdout, stderr = run_command(put_cmd, f"Upload {table}", env=env)
            if not success:
                logger.error(f"Failed to upload {table}: {stderr}")
                return False
                
            logger.info(f"Successfully uploaded {table} to Kerberized HDFS")
        
        return True
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False


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
    
    # Simple-auth HDFS options
    parser.add_argument(
        "--simple-auth-hadoop-bin", 
        default="hadoop", 
        help="Path to Hadoop binary for Simple-auth cluster"
    )
    parser.add_argument(
        "--simple-auth-hadoop-conf", 
        required=True, 
        help="Path to Hadoop configuration for Simple-auth cluster"
    )
    parser.add_argument("--simple-auth-user", default="hdfs", help="Hadoop user name")
    
    # Kerberized HDFS options
    parser.add_argument(
        "--kerberized-hadoop-bin", 
        default="hadoop", 
        help="Path to Hadoop binary for Kerberized cluster"
    )
    parser.add_argument(
        "--kerberized-hadoop-conf", 
        required=True, 
        help="Path to Hadoop configuration for Kerberized cluster"
    )
    parser.add_argument("--keytab", required=True, help="Path to keytab file")
    parser.add_argument("--principal", required=True, help="Kerberos principal")
    
    args = parser.parse_args()
    
    logger.info("Starting TPC-DS data upload to HDFS clusters...")
    
    for scale in args.scale_factors:
        for format_type in args.formats:
            logger.info(f"Processing {scale}GB {format_type} data...")
            
            # Upload to Simple-auth HDFS
            simple_auth_success = upload_to_simple_auth_hdfs(
                args.data_dir, 
                args.hdfs_target_dir, 
                scale, 
                format_type,
                args.simple_auth_hadoop_bin,
                args.simple_auth_hadoop_conf,
                args.simple_auth_user
            )
            
            if not simple_auth_success:
                logger.error("Failed to upload to Simple-auth HDFS. Exiting.")
                sys.exit(1)
            
            # Upload to Kerberized HDFS
            kerberized_success = upload_to_kerberized_hdfs(
                args.data_dir, 
                args.hdfs_target_dir, 
                scale, 
                format_type,
                args.kerberized_hadoop_bin,
                args.kerberized_hadoop_conf,
                args.keytab,
                args.principal
            )
            
            if not kerberized_success:
                logger.error("Failed to upload to Kerberized HDFS. Exiting.")
                sys.exit(1)
    
    logger.info("TPC-DS data upload to HDFS clusters completed successfully!")


if __name__ == "__main__":
    main() 