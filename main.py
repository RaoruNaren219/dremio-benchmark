#!/usr/bin/env python3

"""
Dremio Benchmark Tool
This script orchestrates the TPC-DS benchmark process for Dremio.
"""

import os
import sys
import argparse
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple

# Add project root to Python path
sys.path.append(str(Path(__file__).parent))

# Import project modules
from utils.logging_config import setup_logging
from data_generation.generate_tpcds_data import generate_tpcds_data
from hdfs_upload.upload_to_hdfs import HDFSClient

# Configure logging
logger = setup_logging(
    log_file="benchmark.log",
    module_name="dremio_benchmark.main"
)

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
    
    return parser.parse_args()

def main():
    """
    Main function to run the Dremio benchmark
    """
    args = parse_args()
    
    try:
        # Generate TPC-DS data
        logger.info("Starting Dremio TPC-DS benchmark...")
        data_dir = os.path.join(args.data_dir, f"tpcds_{args.scale_factors[0]}gb")
        
        # Generate data
        from data_generation.generate_tpcds_data import generate_tpcds_data
        if not generate_tpcds_data(args.scale_factors[0], data_dir, args.formats[0]):
            logger.error("Failed to generate TPC-DS data")
            return False
            
        # Upload to HDFS if target directory is specified
        if args.hdfs_target_dir:
            from hdfs_upload.upload_to_hdfs import HDFSClient
            try:
                hdfs_client = HDFSClient(hadoop_conf=args.hadoop_conf, user=args.user)
                hdfs_client.upload_directory(data_dir, args.hdfs_target_dir)
                logger.info(f"Successfully uploaded data to HDFS: {args.hdfs_target_dir}")
            except RuntimeError as e:
                logger.error(f"Failed to upload to HDFS: {str(e)}")
                return False
            except Exception as e:
                logger.error(f"Unexpected error during HDFS upload: {str(e)}")
                return False
                
        logger.info("Benchmark completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Unexpected error during benchmark: {str(e)}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 