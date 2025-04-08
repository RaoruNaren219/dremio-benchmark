#!/usr/bin/env python3

"""
TPC-DS Data Generation Script
This script generates TPC-DS data at 1GB and 10GB scale factors
"""

import os
import subprocess
import argparse
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_generation.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def generate_tpcds_data(dsdgen_path, output_dir, scale_factor):
    """
    Generate TPC-DS data at the specified scale factor
    
    Args:
        dsdgen_path (str): Path to dsdgen binary
        output_dir (str): Directory to output generated data
        scale_factor (int): Scale factor in GB
    
    Returns:
        bool: True if successful, False otherwise
    """
    logger.info(f"Generating data at scale factor {scale_factor}GB...")
    
    # Create the output directory
    os.makedirs(os.path.join(output_dir, f"{scale_factor}gb"), exist_ok=True)
    
    # Get the dsdgen directory
    dsdgen_dir = os.path.dirname(dsdgen_path)
    
    try:
        # Change to the dsdgen directory
        current_dir = os.getcwd()
        os.chdir(dsdgen_dir)
        
        # Run dsdgen command
        cmd = [
            "./dsdgen",
            "-SCALE", str(scale_factor),
            "-DIR", os.path.join(output_dir, f"{scale_factor}gb"),
            "-FORCE"
        ]
        
        result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # Change back to original directory
        os.chdir(current_dir)
        
        logger.info(f"Successfully generated {scale_factor}GB data")
        return True
    
    except subprocess.CalledProcessError as e:
        logger.error(f"Error generating {scale_factor}GB data: {e}")
        # Change back to original directory
        os.chdir(current_dir)
        return False
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        # Change back to original directory
        os.chdir(current_dir)
        return False

def main():
    """Main function to generate TPC-DS data"""
    parser = argparse.ArgumentParser(description="Generate TPC-DS data")
    parser.add_argument("--dsdgen-path", required=True, help="Path to dsdgen binary")
    parser.add_argument("--output-dir", default="../data", help="Output directory")
    parser.add_argument("--scale-factors", nargs="+", type=int, default=[1, 10], 
                        help="Scale factors in GB (default: 1 10)")
    
    args = parser.parse_args()
    
    logger.info("Starting TPC-DS data generation...")
    
    for scale in args.scale_factors:
        success = generate_tpcds_data(args.dsdgen_path, args.output_dir, scale)
        if not success:
            logger.error(f"Failed to generate data for scale factor {scale}GB")
            sys.exit(1)
    
    logger.info("TPC-DS data generation completed successfully!")

if __name__ == "__main__":
    main() 