#!/usr/bin/env python3
import os
import time
import json
from typing import Dict, Any, Optional
from datetime import datetime
import requests
from dotenv import load_dotenv
from tabulate import tabulate

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
        except requests.exceptions.Timeout:
            print(f"Timeout Error: Server at {auth_url} did not respond within 10 seconds")
            raise
        except requests.exceptions.RequestException as e:
            print(f"Request Error: {str(e)}")
            raise

    def verify_source(self, source_name: str) -> bool:
        """Verify if a source exists in Dremio."""
        headers = {
            "Authorization": f"_dremio{self.token}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.get(
                f"{self.base_url}/api/v3/catalog",
                headers=headers
            )
            response.raise_for_status()
            sources = response.json().get("data", [])
            return any(source.get("name") == source_name for source in sources)
        except Exception as e:
            print(f"Error verifying source: {str(e)}")
            return False

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
            print(f"Executing query at: {query_url}")
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
        except requests.exceptions.ConnectionError:
            print(f"Connection Error: Could not connect to {query_url}")
            print("Please check your connection and try again")
            raise
        except requests.exceptions.Timeout:
            print(f"Timeout Error: Server did not respond within the timeout period")
            raise
        except requests.exceptions.RequestException as e:
            print(f"Request Error: {str(e)}")
            raise

class CrossDremioBenchmark:
    def __init__(self, dremio1_url: str, dremio1_username: str, dremio1_password: str,
                 dremio2_url: str, dremio2_username: str, dremio2_password: str,
                 source_name: str):
        self.dremio1 = DremioClient(dremio1_url, dremio1_username, dremio1_password)
        self.dremio2 = DremioClient(dremio2_url, dremio2_username, dremio2_password)
        self.source_name = source_name
        self.results = []

    def verify_setup(self) -> bool:
        """Verify the setup for cross-Dremio operations."""
        print("\nVerifying setup...")
        
        # Verify Dremio 2 source exists in Dremio 1
        if not self.dremio1.verify_source(self.source_name):
            print(f"Error: Source '{self.source_name}' not found in Dremio 1")
            return False
            
        print("✓ Source verification successful")
        return True

    def benchmark_read(self) -> Dict[str, Any]:
        """Benchmark reading from Dremio 2 via source connector."""
        sql = f"SELECT * FROM {self.source_name}.sales_db.transactions"
        
        start_time = time.time()
        result = self.dremio1.execute_query(sql)
        end_time = time.time()
        
        return {
            "operation": "READ",
            "duration": end_time - start_time,
            "rows_processed": result.get("rowsProcessed", 0),
            "timestamp": datetime.now().isoformat()
        }

    def benchmark_write(self) -> Dict[str, Any]:
        """Benchmark writing to Dremio 2 via source connector."""
        sql = f"""
        CREATE TABLE {self.source_name}.benchmark_results.transactions_copy AS 
        SELECT * FROM {self.source_name}.sales_db.transactions
        """
        
        start_time = time.time()
        result = self.dremio1.execute_query(sql)
        end_time = time.time()
        
        return {
            "operation": "WRITE",
            "duration": end_time - start_time,
            "rows_processed": result.get("rowsProcessed", 0),
            "timestamp": datetime.now().isoformat()
        }

    def run_benchmark(self, iterations: int = 3) -> None:
        """Run benchmark for specified number of iterations."""
        if not self.verify_setup():
            print("Setup verification failed. Please check your configuration.")
            return

        print(f"\nStarting cross-Dremio benchmark with {iterations} iterations...")
        print(f"Using source: {self.source_name}")
        
        for i in range(iterations):
            print(f"\nIteration {i + 1}/{iterations}")
            
            # Read benchmark
            read_result = self.benchmark_read()
            self.results.append(read_result)
            print(f"READ operation completed in {read_result['duration']:.2f} seconds")
            
            # Write benchmark
            write_result = self.benchmark_write()
            self.results.append(write_result)
            print(f"WRITE operation completed in {write_result['duration']:.2f} seconds")
            
            if i < iterations - 1:
                print("Waiting 5 seconds before next iteration...")
                time.sleep(5)

    def print_results(self) -> None:
        """Print benchmark results in a formatted table."""
        headers = ["Operation", "Duration (s)", "Rows Processed", "Timestamp"]
        table_data = [
            [r["operation"], f"{r['duration']:.2f}", r["rows_processed"], r["timestamp"]]
            for r in self.results
        ]
        
        print("\nCross-Dremio Benchmark Results:")
        print(tabulate(table_data, headers=headers, tablefmt="grid"))
        
        # Calculate averages
        read_durations = [r["duration"] for r in self.results if r["operation"] == "READ"]
        write_durations = [r["duration"] for r in self.results if r["operation"] == "WRITE"]
        
        print("\nSummary:")
        print(f"Average READ duration: {sum(read_durations)/len(read_durations):.2f} seconds")
        print(f"Average WRITE duration: {sum(write_durations)/len(write_durations):.2f} seconds")

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
    
    # Get source name
    source_name = os.getenv("DREMIO2_SOURCE_NAME")
    
    if not all([dremio1_url, dremio1_username, dremio1_password,
                dremio2_url, dremio2_username, dremio2_password,
                source_name]):
        raise ValueError("Please set all required environment variables in .env file")
    
    # Create and run benchmark
    benchmark = CrossDremioBenchmark(
        dremio1_url, dremio1_username, dremio1_password,
        dremio2_url, dremio2_username, dremio2_password,
        source_name
    )
    benchmark.run_benchmark()
    benchmark.print_results()

if __name__ == "__main__":
    main() 