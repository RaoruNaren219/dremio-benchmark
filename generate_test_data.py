#!/usr/bin/env python3
import os
import time
import json
import random
from datetime import datetime, timedelta
from typing import Dict, Any
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import requests

class DremioClient:
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.token = None
        self._authenticate()

    def _authenticate(self) -> None:
        """Authenticate with Dremio and get token."""
        auth_url = f"{self.base_url}/apiv2/login"
        auth_data = {
            "userName": self.username,
            "password": self.password
        }
        
        response = requests.post(auth_url, json=auth_data)
        response.raise_for_status()
        self.token = response.json()["token"]

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
        
        # Submit query
        response = requests.post(query_url, headers=headers, json=query_data)
        response.raise_for_status()
        job_id = response.json()["id"]
        
        # Poll for results
        while True:
            status_url = f"{self.base_url}/api/v3/job/{job_id}"
            status_response = requests.get(status_url, headers=headers)
            status_response.raise_for_status()
            job_status = status_response.json()
            
            if job_status["jobState"] == "COMPLETED":
                return job_status
            elif job_status["jobState"] in ["FAILED", "CANCELED"]:
                raise Exception(f"Query failed: {job_status.get('errorMessage', 'Unknown error')}")
            
            time.sleep(1)

    def create_schema(self, schema_name: str) -> None:
        """Create a schema if it doesn't exist."""
        sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
        self.execute_query(sql)

class TestDataGenerator:
    def __init__(self, dremio1_url: str, dremio1_username: str, dremio1_password: str,
                 dremio2_url: str, dremio2_username: str, dremio2_password: str):
        self.dremio1 = DremioClient(dremio1_url, dremio1_username, dremio1_password)
        self.dremio2 = DremioClient(dremio2_url, dremio2_username, dremio2_password)

    def generate_transactions_data(self, num_records: int = 1000000) -> pd.DataFrame:
        """Generate synthetic transaction data."""
        # Generate random dates
        dates = pd.date_range(start='2023-01-01', end='2023-12-31', periods=num_records)
        
        # Generate random transaction amounts
        amounts = np.random.normal(100, 50, num_records)
        amounts = np.maximum(amounts, 0)  # Ensure no negative amounts
        
        # Generate random customer IDs
        customer_ids = np.random.randint(1, 10000, num_records)
        
        # Generate random product categories
        categories = np.random.choice(['Electronics', 'Clothing', 'Food', 'Books', 'Sports'], num_records)
        
        # Create DataFrame
        df = pd.DataFrame({
            'transaction_id': range(1, num_records + 1),
            'customer_id': customer_ids,
            'transaction_date': dates,
            'amount': amounts,
            'category': categories,
            'created_at': datetime.now()
        })
        
        return df

    def ingest_data(self, df: pd.DataFrame, cluster: DremioClient, schema: str, table: str) -> None:
        """Ingest data into Dremio using SQL."""
        # Create schema if it doesn't exist
        cluster.create_schema(schema)
        
        # Convert DataFrame to SQL insert statements
        values = []
        for _, row in df.iterrows():
            values.append(f"({row['transaction_id']}, {row['customer_id']}, "
                        f"'{row['transaction_date']}', {row['amount']}, "
                        f"'{row['category']}', '{row['created_at']}')")
        
        # Create table and insert data
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            transaction_id BIGINT,
            customer_id BIGINT,
            transaction_date TIMESTAMP,
            amount DOUBLE,
            category VARCHAR,
            created_at TIMESTAMP
        )
        """
        
        cluster.execute_query(create_table_sql)
        
        # Insert data in batches
        batch_size = 1000
        for i in range(0, len(values), batch_size):
            batch = values[i:i + batch_size]
            insert_sql = f"""
            INSERT INTO {schema}.{table}
            VALUES {','.join(batch)}
            """
            cluster.execute_query(insert_sql)
            print(f"Inserted batch {i//batch_size + 1} of {(len(values) + batch_size - 1)//batch_size}")

    def setup_test_data(self) -> None:
        """Set up test data in both Dremio clusters."""
        print("Generating test data...")
        df = self.generate_transactions_data()
        
        print("\nIngesting data into Dremio 1...")
        self.ingest_data(df, self.dremio1, "sales_db", "transactions")
        
        print("\nIngesting data into Dremio 2...")
        self.ingest_data(df, self.dremio2, "sales_db", "transactions")
        
        print("\nCreating benchmark results schema in Dremio 2...")
        self.dremio2.create_schema("benchmark_results")
        
        print("\nTest data setup completed successfully!")

def main():
    # Load environment variables
    load_dotenv()
    
    # Get Dremio 1 credentials
    dremio1_url = os.getenv("DREMIO1_URL")
    dremio1_username = os.getenv("DREMIO1_USERNAME")
    dremio1_password = os.getenv("DREMIO1_PASSWORD")
    
    # Get Dremio 2 credentials
    dremio2_url = os.getenv("DREMIO2_URL")
    dremio2_username = os.getenv("DREMIO2_USERNAME")
    dremio2_password = os.getenv("DREMIO2_PASSWORD")
    
    if not all([dremio1_url, dremio1_username, dremio1_password,
                dremio2_url, dremio2_username, dremio2_password]):
        raise ValueError("Please set all required environment variables in .env file")
    
    # Create and run data generator
    generator = TestDataGenerator(
        dremio1_url, dremio1_username, dremio1_password,
        dremio2_url, dremio2_username, dremio2_password
    )
    generator.setup_test_data()

if __name__ == "__main__":
    main() 