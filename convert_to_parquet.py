#!/usr/bin/env python3
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Dict, Any, List
import requests
from dotenv import load_dotenv
import logging
import time
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tpcds_conversion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# TPC-DS table schemas
TPCDS_SCHEMAS = {
    "call_center": """
        cc_call_center_sk BIGINT,
        cc_call_center_id VARCHAR(16),
        cc_rec_start_date DATE,
        cc_rec_end_date DATE,
        cc_closed_date_sk BIGINT,
        cc_open_date_sk BIGINT,
        cc_name VARCHAR(50),
        cc_class VARCHAR(50),
        cc_employees INTEGER,
        cc_sq_ft INTEGER,
        cc_hours VARCHAR(20),
        cc_manager VARCHAR(40),
        cc_mkt_id INTEGER,
        cc_mkt_class VARCHAR(50),
        cc_mkt_desc VARCHAR(100),
        cc_market_manager VARCHAR(40),
        cc_division INTEGER,
        cc_division_name VARCHAR(50),
        cc_company INTEGER,
        cc_company_name VARCHAR(50),
        cc_street_number VARCHAR(10),
        cc_street_name VARCHAR(60),
        cc_street_type VARCHAR(15),
        cc_suite_number VARCHAR(10),
        cc_city VARCHAR(60),
        cc_county VARCHAR(30),
        cc_state VARCHAR(2),
        cc_zip VARCHAR(10),
        cc_country VARCHAR(20),
        cc_gmt_offset DECIMAL(5,2),
        cc_tax_percentage DECIMAL(5,2)
    """,
    "catalog_sales": """
        cs_sold_date_sk BIGINT,
        cs_sold_time_sk BIGINT,
        cs_ship_date_sk BIGINT,
        cs_bill_customer_sk BIGINT,
        cs_bill_cdemo_sk BIGINT,
        cs_bill_hdemo_sk BIGINT,
        cs_bill_addr_sk BIGINT,
        cs_ship_customer_sk BIGINT,
        cs_ship_cdemo_sk BIGINT,
        cs_ship_hdemo_sk BIGINT,
        cs_ship_addr_sk BIGINT,
        cs_call_center_sk BIGINT,
        cs_catalog_page_sk BIGINT,
        cs_ship_mode_sk BIGINT,
        cs_warehouse_sk BIGINT,
        cs_item_sk BIGINT,
        cs_promo_sk BIGINT,
        cs_order_number BIGINT,
        cs_quantity INTEGER,
        cs_wholesale_cost DECIMAL(7,2),
        cs_list_price DECIMAL(7,2),
        cs_sales_price DECIMAL(7,2),
        cs_ext_discount_amt DECIMAL(7,2),
        cs_ext_sales_price DECIMAL(7,2),
        cs_ext_wholesale_cost DECIMAL(7,2),
        cs_ext_list_price DECIMAL(7,2),
        cs_ext_tax DECIMAL(7,2),
        cs_coupon_amt DECIMAL(7,2),
        cs_ext_ship_cost DECIMAL(7,2),
        cs_net_paid DECIMAL(7,2),
        cs_net_paid_inc_tax DECIMAL(7,2),
        cs_net_paid_inc_ship DECIMAL(7,2),
        cs_net_paid_inc_ship_tax DECIMAL(7,2),
        cs_net_profit DECIMAL(7,2)
    """
    # Add other table schemas here...
}

class DremioClient:
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.token = None
        logger.info(f"Attempting to connect to Dremio at: {self.base_url}")
        self._authenticate()

    def _authenticate(self) -> None:
        """Authenticate with Dremio and get token."""
        auth_url = f"{self.base_url}/apiv2/login"
        auth_data = {
            "userName": self.username,
            "password": self.password
        }
        
        try:
            logger.info(f"Attempting authentication to: {auth_url}")
            response = requests.post(auth_url, json=auth_data, timeout=10)
            response.raise_for_status()
            self.token = response.json()["token"]
            logger.info("Authentication successful!")
        except requests.exceptions.ConnectionError:
            logger.error(f"Connection Error: Could not connect to {auth_url}")
            logger.error("Please check:")
            logger.error("1. Is the Dremio server running?")
            logger.error("2. Is the URL correct? (should be like http://hostname:9047)")
            logger.error("3. Are you able to ping the host?")
            logger.error("4. Is port 9047 open and accessible?")
            raise

    def execute_query(self, sql: str) -> Dict[str, Any]:
        """Execute a SQL query and return the results."""
        headers = {
            "Authorization": f"_dremio{self.token}",
            "Content-Type": "application/json"
        }
        
        query_url = f"{self.base_url}/api/v3/sql"
        query_data = {
            "sql": sql,
            "context": []
        }
        
        try:
            logger.info(f"Executing query: {sql[:100]}...")
            response = requests.post(query_url, headers=headers, json=query_data, timeout=30)
            response.raise_for_status()
            job_id = response.json()["id"]
            logger.info(f"Query submitted successfully. Job ID: {job_id}")
            
            # Poll for results
            while True:
                status_url = f"{self.base_url}/api/v3/job/{job_id}"
                status_response = requests.get(status_url, headers=headers, timeout=10)
                status_response.raise_for_status()
                job_status = status_response.json()
                
                if job_status["jobState"] == "COMPLETED":
                    logger.info("Query completed successfully")
                    return job_status
                elif job_status["jobState"] in ["FAILED", "CANCELED"]:
                    error_msg = job_status.get("errorMessage", "Unknown error")
                    logger.error(f"Query failed: {error_msg}")
                    raise Exception(f"Query failed: {error_msg}")
                
                time.sleep(1)
        except requests.exceptions.RequestException as e:
            logger.error(f"Request Error: {str(e)}")
            raise

class TPCDataConverter:
    def __init__(self, dremio_url: str, username: str, password: str):
        self.dremio = DremioClient(dremio_url, username, password)
        self.input_base_dir = "./tpcds_data"
        self.output_base_dir = "./tpcds_parquet"
        self.scale_factors = [1, 10, 100]  # Scale factors for 1GB, 10GB, and 100GB
        self.tables = list(TPCDS_SCHEMAS.keys())

    def convert_to_parquet(self, scale_factor: int) -> None:
        """Convert TPC-DS data to Parquet format for a specific scale factor."""
        input_dir = os.path.join(self.input_base_dir, f"sf{scale_factor}")
        output_dir = os.path.join(self.output_base_dir, f"sf{scale_factor}")
        
        logger.info(f"Converting TPC-DS data to Parquet format for scale factor {scale_factor}...")
        logger.info(f"Input directory: {input_dir}")
        logger.info(f"Output directory: {output_dir}")
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        for table in self.tables:
            logger.info(f"Converting table: {table}")
            input_file = os.path.join(input_dir, f"{table}.dat")
            output_file = os.path.join(output_dir, f"{table}.parquet")
            
            if not os.path.exists(input_file):
                logger.warning(f"Input file not found: {input_file}")
                continue
            
            try:
                # Read the data file
                df = pd.read_csv(input_file, delimiter='|', header=None)
                
                # Convert to Parquet
                table = pa.Table.from_pandas(df)
                pq.write_table(table, output_file)
                logger.info(f"Converted {table} to Parquet format")
            except Exception as e:
                logger.error(f"Error converting table {table}: {str(e)}")
                raise

    def load_to_dremio(self, scale_factor: int) -> None:
        """Load Parquet data into Dremio tables for a specific scale factor."""
        logger.info(f"Loading data into Dremio for scale factor {scale_factor}...")
        
        # Create schema for this scale factor
        schema_name = f"tpcds_sf{scale_factor}"
        self.dremio.execute_query(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        
        for table in self.tables:
            logger.info(f"Loading table: {table}")
            parquet_file = os.path.join(self.output_base_dir, f"sf{scale_factor}", f"{table}.parquet")
            
            if not os.path.exists(parquet_file):
                logger.warning(f"Parquet file not found: {parquet_file}")
                continue
            
            try:
                # Create table in Dremio with proper schema
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema_name}.{table} (
                    {TPCDS_SCHEMAS[table]}
                )
                """
                self.dremio.execute_query(create_table_sql)
                
                # Load data into table using COPY INTO
                copy_sql = f"""
                COPY INTO {schema_name}.{table}
                FROM (SELECT * FROM parquet.`{parquet_file}`)
                """
                self.dremio.execute_query(copy_sql)
                logger.info(f"Loaded data for table {table}")
            except Exception as e:
                logger.error(f"Error loading table {table}: {str(e)}")
                raise

    def process_data(self) -> None:
        """Process TPC-DS data: convert to Parquet and load into Dremio for all scale factors."""
        try:
            for scale_factor in self.scale_factors:
                logger.info(f"\nProcessing scale factor {scale_factor}...")
                self.convert_to_parquet(scale_factor)
                self.load_to_dremio(scale_factor)
                logger.info(f"Completed processing scale factor {scale_factor}")
            
            logger.info("\nTPC-DS data processing completed successfully!")
        except Exception as e:
            logger.error(f"Error processing TPC-DS data: {str(e)}")
            raise

def main():
    try:
        # Load environment variables
        load_dotenv()
        
        # Get Dremio credentials
        dremio_url = os.getenv("DREMIO1_URL")
        username = os.getenv("DREMIO1_USERNAME")
        password = os.getenv("DREMIO1_PASSWORD")
        
        if not all([dremio_url, username, password]):
            raise ValueError("Please set DREMIO1_URL, DREMIO1_USERNAME, and DREMIO1_PASSWORD in .env file")
        
        # Create and run data converter
        converter = TPCDataConverter(dremio_url, username, password)
        converter.process_data()
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    main() 