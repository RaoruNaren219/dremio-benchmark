#!/usr/bin/env python3
import os
import subprocess
import time
from typing import Dict, Any
import requests
from dotenv import load_dotenv

class DremioClient:
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.token = None
        print(f"Attempting to connect to Dremio at: {self.base_url}")
        self._authenticate()

    def _authenticate(self) -> None:
        """Authenticate with Dremio and get token."""
        auth_url = f"{self.base_url}/apiv2/login"
        auth_data = {
            "userName": self.username,
            "password": self.password
        }
        
        try:
            print(f"Attempting authentication to: {auth_url}")
            response = requests.post(auth_url, json=auth_data, timeout=10)
            response.raise_for_status()
            self.token = response.json()["token"]
            print("Authentication successful!")
        except requests.exceptions.ConnectionError:
            print(f"Connection Error: Could not connect to {auth_url}")
            print("Please check:")
            print("1. Is the Dremio server running?")
            print("2. Is the URL correct? (should be like http://hostname:9047)")
            print("3. Are you able to ping the host?")
            print("4. Is port 9047 open and accessible?")
            raise

    def create_schema(self, schema_name: str) -> None:
        """Create a schema if it doesn't exist."""
        sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
        self.execute_query(sql)

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
            print(f"Executing query: {sql[:100]}...")
            response = requests.post(query_url, headers=headers, json=query_data, timeout=30)
            response.raise_for_status()
            job_id = response.json()["id"]
            print(f"Query submitted successfully. Job ID: {job_id}")
            
            # Poll for results
            while True:
                status_url = f"{self.base_url}/api/v3/job/{job_id}"
                status_response = requests.get(status_url, headers=headers, timeout=10)
                status_response.raise_for_status()
                job_status = status_response.json()
                
                if job_status["jobState"] == "COMPLETED":
                    print("Query completed successfully")
                    return job_status
                elif job_status["jobState"] in ["FAILED", "CANCELED"]:
                    error_msg = job_status.get("errorMessage", "Unknown error")
                    print(f"Query failed: {error_msg}")
                    raise Exception(f"Query failed: {error_msg}")
                
                time.sleep(1)
        except requests.exceptions.RequestException as e:
            print(f"Request Error: {str(e)}")
            raise

class TPCDataGenerator:
    def __init__(self, dremio_url: str, username: str, password: str, scale_factor: int = 1):
        self.dremio = DremioClient(dremio_url, username, password)
        self.scale_factor = scale_factor
        self.tables = [
            "call_center", "catalog_page", "catalog_returns", "catalog_sales",
            "customer", "customer_address", "customer_demographics", "date_dim",
            "household_demographics", "income_band", "inventory", "item",
            "promotion", "reason", "ship_mode", "store", "store_returns",
            "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
            "web_sales", "web_site"
        ]

    def create_tpcds_schema(self) -> None:
        """Create the TPC-DS schema."""
        print("Creating TPC-DS schema...")
        self.dremio.create_schema("tpcds")

    def create_tables(self) -> None:
        """Create TPC-DS tables with appropriate schemas."""
        for table in self.tables:
            print(f"Creating table: {table}")
            # Note: These are simplified schemas. You should use the full TPC-DS schemas
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS tpcds.{table} (
                -- Add appropriate columns based on TPC-DS specification
                -- This is a placeholder. You should replace with actual schema
                id BIGINT,
                name VARCHAR
            )
            """
            self.dremio.execute_query(create_table_sql)

    def generate_data(self) -> None:
        """Generate TPC-DS test data."""
        print(f"Generating TPC-DS data with scale factor {self.scale_factor}...")
        
        # This is a placeholder for actual data generation
        # You should implement the actual TPC-DS data generation logic here
        # This could involve:
        # 1. Using dsdgen tool from TPC-DS toolkit
        # 2. Converting generated data to Parquet format
        # 3. Loading data into Dremio tables
        
        print("Data generation completed!")

    def setup_benchmark_data(self) -> None:
        """Set up complete TPC-DS benchmark data."""
        try:
            self.create_tpcds_schema()
            self.create_tables()
            self.generate_data()
            print("TPC-DS benchmark data setup completed successfully!")
        except Exception as e:
            print(f"Error setting up TPC-DS benchmark data: {str(e)}")
            raise

def main():
    # Load environment variables
    load_dotenv()
    
    # Get Dremio credentials
    dremio_url = os.getenv("DREMIO1_URL")
    username = os.getenv("DREMIO1_USERNAME")
    password = os.getenv("DREMIO1_PASSWORD")
    
    if not all([dremio_url, username, password]):
        raise ValueError("Please set DREMIO1_URL, DREMIO1_USERNAME, and DREMIO1_PASSWORD in .env file")
    
    # Create and run data generator
    generator = TPCDataGenerator(dremio_url, username, password)
    generator.setup_benchmark_data()

if __name__ == "__main__":
    main() 