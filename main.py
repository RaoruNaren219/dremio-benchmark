#!/usr/bin/env python3

"""
Dremio Benchmark Tool
This script orchestrates the TPC-DS benchmark process for Dremio.
"""

import os
import sys
import argparse
import logging
import time
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple

# Add project root to Python path
sys.path.append(str(Path(__file__).parent))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("benchmark.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Import project modules
from data_generation.generate_tpcds_data import generate_tpcds_data
from hdfs_upload.upload_to_hdfs import upload_to_hdfs
from utils.command import run_command, PythonCommand

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Dremio TPC-DS Benchmark Tool")
    
    # Data generation options
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
        default=["csv", "json", "pipe", "orc", "parquet"], 
        help="Data formats to generate"
    )
    parser.add_argument(
        "--data-dir", 
        default="../data", 
        help="Base directory for data"
    )
    
    # HDFS options
    parser.add_argument(
        "--hadoop-conf", 
        required=True, 
        help="Path to Hadoop configuration"
    )
    parser.add_argument(
        "--hdfs-target-dir", 
        default="/benchmark/tpcds", 
        help="HDFS target directory"
    )
    parser.add_argument(
        "--user", 
        default="hdfs", 
        help="Hadoop user name"
    )
    
    # Optional Kerberos options
    parser.add_argument(
        "--keytab", 
        help="Path to keytab file"
    )
    parser.add_argument(
        "--principal", 
        help="Kerberos principal"
    )
    
    return parser.parse_args()

def main():
    """Main function to run the benchmark."""
    args = parse_args()
    
    logger.info("Starting Dremio TPC-DS benchmark...")
    
    # Generate TPC-DS data
    logger.info("Generating TPC-DS data...")
    for scale in args.scale_factors:
        for format_type in args.formats:
            logger.info(f"Generating {scale}GB {format_type} data...")
            success = generate_tpcds_data(
                args.data_dir,
                scale,
                format_type
            )
            if not success:
                logger.error(f"Failed to generate {scale}GB {format_type} data")
                return False
    
    # Upload data to HDFS
    logger.info("Uploading data to HDFS...")
    for scale in args.scale_factors:
        for format_type in args.formats:
            logger.info(f"Uploading {scale}GB {format_type} data to HDFS...")
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
    
    logger.info("Benchmark completed successfully")
    return True

if __name__ == "__main__":
    sys.exit(0 if main() else 1) 