#!/usr/bin/env python3

"""
Dremio DDL Generation Script
This script generates and executes DDL statements for creating TPC-DS tables in Dremio
"""

import os
import argparse
import logging
import sys
import requests
import json
from typing import List, Dict, Optional
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("ddl_generation.log"),
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

class DremioDDL:
    def __init__(self, host: str, port: int, username: str, password: str, use_ssl: bool = True):
        """
        Initialize Dremio DDL client
        
        Args:
            host (str): Dremio host
            port (int): Dremio port
            username (str): Dremio username
            password (str): Dremio password
            use_ssl (bool, optional): Whether to use SSL. Defaults to True.
        """
        self.host = host
        self.port = port
        self.base_url = f"{'https' if use_ssl else 'http'}://{host}:{port}/api/v3"
        self.username = username
        self.password = password
        self.token = None
        self.headers = {
            "Content-Type": "application/json"
        }
    
    def login(self) -> bool:
        """
        Authenticate with Dremio and get a token
        
        Returns:
            bool: True if successful, False otherwise
        """
        login_url = f"{self.base_url}/login"
        payload = {
            "userName": self.username,
            "password": self.password
        }
        
        try:
            response = requests.post(login_url, headers=self.headers, json=payload)
            response.raise_for_status()
            self.token = response.json()["token"]
            self.headers["Authorization"] = f"_dremio{self.token}"
            logger.info(f"Successfully authenticated to Dremio at {self.host}")
            return True
        except Exception as e:
            logger.error(f"Failed to authenticate to Dremio: {str(e)}")
            return False
    
    def execute_sql(self, sql: str) -> Optional[Dict]:
        """
        Execute a SQL statement in Dremio
        
        Args:
            sql (str): SQL statement to execute
        
        Returns:
            Optional[Dict]: Response from Dremio if successful, None otherwise
        """
        if not self.token:
            if not self.login():
                return None
        
        sql_url = f"{self.base_url}/sql"
        payload = {
            "sql": sql
        }
        
        try:
            response = requests.post(sql_url, headers=self.headers, json=payload)
            response.raise_for_status()
            result = response.json()
            
            # Poll for job completion
            job_id = result.get("id")
            job_status = self._poll_job_status(job_id)
            
            if job_status.get("jobState") == "COMPLETED":
                logger.info(f"SQL executed successfully: {sql[:50]}...")
                return job_status
            else:
                logger.error(f"SQL execution failed: {job_status}")
                return None
        
        except Exception as e:
            logger.error(f"Error executing SQL: {str(e)}")
            return None
    
    def _poll_job_status(self, job_id: str, timeout: int = 60, interval: int = 1) -> Dict:
        """
        Poll for job status until completion or timeout
        
        Args:
            job_id (str): Job ID to poll
            timeout (int, optional): Timeout in seconds. Defaults to 60.
            interval (int, optional): Polling interval in seconds. Defaults to 1.
        
        Returns:
            Dict: Job status
        """
        job_url = f"{self.base_url}/job/{job_id}"
        start_time = time.time()
        
        while True:
            try:
                response = requests.get(job_url, headers=self.headers)
                response.raise_for_status()
                job_status = response.json()
                
                if job_status.get("jobState") in ["COMPLETED", "FAILED", "CANCELED"]:
                    return job_status
                
                # Check timeout
                if time.time() - start_time > timeout:
                    logger.warning(f"Job {job_id} timed out after {timeout} seconds")
                    return {"jobState": "TIMEOUT"}
                
                time.sleep(interval)
            
            except Exception as e:
                logger.error(f"Error checking job status: {str(e)}")
                return {"jobState": "ERROR", "error": str(e)}

def generate_ddl_statements(hdfs_base_path: str, scale_factors: List[int], formats: List[str], tables: List[str]) -> Dict[str, str]:
    """
    Generate DDL statements for creating TPC-DS tables in Dremio
    
    Args:
        hdfs_base_path (str): Base HDFS path
        scale_factors (List[int]): Scale factors
        formats (List[str]): Data formats
        tables (List[str]): TPC-DS tables
    
    Returns:
        Dict[str, str]: Dictionary of schema names to SQL statements
    """
    ddl_statements = {}
    
    for scale in scale_factors:
        for fmt in formats:
            # Schema name
            schema_name = f"dfs.hdfs.tpcds_{scale}gb_{fmt}"
            
            # Initialize SQL for schema
            sql = f"-- DDL for {schema_name}\n"
            sql += f"CREATE SCHEMA IF NOT EXISTS {schema_name};\n"
            sql += f"USE {schema_name};\n\n"
            
            # Add table creation statements
            for table in tables:
                sql += f"-- {table}\n"
                sql += f"CREATE OR REPLACE TABLE {table} AS\n"
                sql += f"SELECT * FROM dfs.hdfs.`{hdfs_base_path}/{scale}gb/{fmt}/{table}/*`;\n\n"
            
            ddl_statements[schema_name] = sql
    
    return ddl_statements

def main():
    """Main function to generate and execute DDL statements"""
    parser = argparse.ArgumentParser(description="Generate and execute Dremio DDL statements")
    parser.add_argument("--dremio-host", required=True, help="Dremio host")
    parser.add_argument("--dremio-port", type=int, default=9047, help="Dremio port")
    parser.add_argument("--dremio-username", required=True, help="Dremio username")
    parser.add_argument("--dremio-password", required=True, help="Dremio password")
    parser.add_argument("--no-ssl", action="store_true", help="Disable SSL")
    parser.add_argument("--hdfs-base-path", default="/benchmark/tpcds", help="Base HDFS path")
    parser.add_argument("--scale-factors", nargs="+", type=int, default=[1, 10], 
                        help="Scale factors in GB (default: 1 10)")
    parser.add_argument("--formats", nargs="+", default=FORMATS, 
                        help=f"Data formats (default: {' '.join(FORMATS)})")
    parser.add_argument("--tables", nargs="+", default=TPC_DS_TABLES, 
                        help="TPC-DS tables to create")
    parser.add_argument("--output-dir", default="./sql", help="Output directory for SQL files")
    parser.add_argument("--execute", action="store_true", help="Execute DDL statements in Dremio")
    
    args = parser.parse_args()
    
    logger.info("Starting Dremio DDL generation...")
    
    # Generate DDL statements
    ddl_statements = generate_ddl_statements(
        args.hdfs_base_path,
        args.scale_factors,
        args.formats,
        args.tables
    )
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Write DDL statements to files
    for schema_name, sql in ddl_statements.items():
        file_path = os.path.join(args.output_dir, f"{schema_name}.sql")
        with open(file_path, "w") as f:
            f.write(sql)
        logger.info(f"Wrote DDL statements to {file_path}")
    
    # Execute DDL statements in Dremio if requested
    if args.execute:
        logger.info("Executing DDL statements in Dremio...")
        
        dremio = DremioDDL(
            host=args.dremio_host,
            port=args.dremio_port,
            username=args.dremio_username,
            password=args.dremio_password,
            use_ssl=not args.no_ssl
        )
        
        for schema_name, sql in ddl_statements.items():
            logger.info(f"Executing DDL for {schema_name}")
            result = dremio.execute_sql(sql)
            if not result:
                logger.error(f"Failed to execute DDL for {schema_name}")
    
    logger.info("Dremio DDL generation completed!")

if __name__ == "__main__":
    main() 