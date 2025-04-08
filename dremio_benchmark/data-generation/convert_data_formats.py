#!/usr/bin/env python3

"""
TPC-DS Data Format Conversion Script
This script converts TPC-DS data from raw format to CSV, JSON, Pipe-delimited, ORC, and Parquet
"""

from pyspark.sql import SparkSession
import os
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_conversion.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# TPC-DS Tables (all 24 tables)
TPC_DS_TABLES = [
    "call_center", "catalog_page", "catalog_returns", "catalog_sales",
    "customer", "customer_address", "customer_demographics", "date_dim",
    "household_demographics", "income_band", "inventory", "item",
    "promotion", "reason", "ship_mode", "store", "store_returns",
    "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
    "web_sales", "web_site"
]

# Output formats
FORMATS = ["csv", "json", "pipe", "orc", "parquet"]

def init_spark():
    """Initialize Spark session"""
    return (
        SparkSession.builder
        .appName("TPC-DS Data Conversion")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .getOrCreate()
    )

def convert_table(spark, scale, table, input_dir, output_base_dir):
    """Convert a single table to all formats"""
    logger.info(f"Processing table: {table} at scale {scale}GB")
    
    # Input file path
    input_path = f"{input_dir}/{scale}gb/{table}.dat"
    
    if not os.path.exists(input_path):
        logger.error(f"Input file not found: {input_path}")
        return False
    
    try:
        # Read raw data (pipe-delimited)
        df = spark.read.option("delimiter", "|").option("header", "false").csv(input_path)
        
        # Save in each format
        for fmt in FORMATS:
            output_dir = f"{output_base_dir}/{scale}gb/{fmt}/{table}"
            
            logger.info(f"Converting {table} to {fmt} format")
            
            if fmt == "csv":
                df.write.option("header", "true").csv(output_dir)
            elif fmt == "json":
                df.write.json(output_dir)
            elif fmt == "pipe":
                df.write.option("delimiter", "|").option("header", "true").csv(output_dir)
            elif fmt == "orc":
                df.write.orc(output_dir)
            elif fmt == "parquet":
                df.write.parquet(output_dir)
            
            logger.info(f"Successfully converted {table} to {fmt}")
        
        return True
    except Exception as e:
        logger.error(f"Error converting table {table}: {str(e)}")
        return False

def main():
    """Main function to convert TPC-DS data to different formats"""
    # Input and output directories
    input_dir = "../data"
    output_base_dir = "../data/formatted"
    
    # Scale factors
    scale_factors = [1, 10]
    
    logger.info("Starting TPC-DS data conversion")
    
    # Initialize Spark
    spark = init_spark()
    
    for scale in scale_factors:
        for table in TPC_DS_TABLES:
            convert_table(spark, scale, table, input_dir, output_base_dir)
    
    logger.info("TPC-DS data conversion completed")
    spark.stop()

if __name__ == "__main__":
    main() 