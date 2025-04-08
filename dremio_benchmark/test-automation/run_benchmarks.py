#!/usr/bin/env python3

"""
Dremio Benchmark Automation Script
This script automates the process of running TPC-DS queries against Dremio clusters and collecting performance metrics.
"""

import requests
import json
import time
import csv
import os
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("benchmark.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DremioBenchmark:
    def __init__(self, host, port, username, password, use_ssl=True):
        """Initialize Dremio benchmark client"""
        self.host = host
        self.port = port
        self.base_url = f"{'https' if use_ssl else 'http'}://{host}:{port}/api/v3"
        self.username = username
        self.password = password
        self.token = None
        self.headers = {
            "Content-Type": "application/json"
        }
        
    def login(self):
        """Authenticate with Dremio and get a token"""
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
    
    def run_query(self, query, query_name=None):
        """Run a query and measure execution time"""
        if not self.token:
            if not self.login():
                return None
        
        sql_url = f"{self.base_url}/sql"
        payload = {
            "sql": query
        }
        
        start_time = time.time()
        job_id = None
        
        try:
            # Submit the query
            response = requests.post(sql_url, headers=self.headers, json=payload)
            response.raise_for_status()
            result = response.json()
            job_id = result.get("id")
            
            # Poll for job completion
            job_status = self._poll_job_status(job_id)
            
            end_time = time.time()
            execution_time = end_time - start_time
            
            # Collect metrics
            metrics = {
                "query_name": query_name,
                "execution_time": execution_time,
                "job_id": job_id,
                "status": job_status.get("jobState"),
                "query": query,
                "timestamp": datetime.now().isoformat()
            }
            
            # Get detailed metrics if available
            if job_status.get("jobState") == "COMPLETED":
                profile = self._get_job_profile(job_id)
                if profile:
                    metrics.update({
                        "memory_used": profile.get("memoryUsed", 0),
                        "cpu_used": profile.get("cpuUsed", 0),
                        "io_used": profile.get("ioUsed", 0),
                        "records_processed": profile.get("outputRecords", 0)
                    })
            
            return metrics
        except Exception as e:
            end_time = time.time()
            execution_time = end_time - start_time
            logger.error(f"Error running query: {str(e)}")
            return {
                "query_name": query_name,
                "execution_time": execution_time,
                "job_id": job_id,
                "status": "FAILED",
                "error": str(e),
                "query": query,
                "timestamp": datetime.now().isoformat()
            }
    
    def _poll_job_status(self, job_id, timeout=3600, interval=1):
        """Poll for job status until completion or timeout"""
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
    
    def _get_job_profile(self, job_id):
        """Get detailed job profile for metrics"""
        profile_url = f"{self.base_url}/job/{job_id}/profile"
        
        try:
            response = requests.get(profile_url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error getting job profile: {str(e)}")
            return None

def load_queries(query_dir):
    """Load TPC-DS queries from files"""
    queries = {}
    for filename in os.listdir(query_dir):
        if filename.endswith(".sql"):
            query_name = os.path.splitext(filename)[0]
            with open(os.path.join(query_dir, filename), 'r') as f:
                query_text = f.read()
                queries[query_name] = query_text
    return queries

def run_benchmarks(dremio_client, queries, output_file, concurrency=1, iterations=1):
    """Run benchmark queries with specified concurrency and iterations"""
    results = []
    
    def execute_query(query_name, query_text):
        """Execute a single query and return metrics"""
        logger.info(f"Running query {query_name}")
        metrics = dremio_client.run_query(query_text, query_name)
        if metrics:
            results.append(metrics)
            logger.info(f"Query {query_name} completed in {metrics.get('execution_time', 0):.2f} seconds")
        return metrics
    
    for i in range(iterations):
        logger.info(f"Starting iteration {i+1} of {iterations}")
        
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = []
            for query_name, query_text in queries.items():
                futures.append(executor.submit(execute_query, query_name, query_text))
            
            for future in futures:
                future.result()  # Wait for completion
    
    # Write results to CSV
    if results:
        write_results_to_csv(results, output_file)
    
    return results

def write_results_to_csv(results, output_file):
    """Write benchmark results to CSV file"""
    if not results:
        logger.warning("No results to write to CSV")
        return
    
    # Get all possible keys from all result dictionaries
    fieldnames = set()
    for result in results:
        fieldnames.update(result.keys())
    
    fieldnames = sorted(list(fieldnames))
    
    try:
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for result in results:
                writer.writerow(result)
        
        logger.info(f"Results written to {output_file}")
    except Exception as e:
        logger.error(f"Error writing results to CSV: {str(e)}")

def main():
    """Main function to run benchmarks"""
    parser = argparse.ArgumentParser(description="Dremio Benchmark Tool")
    parser.add_argument("--host", required=True, help="Dremio host")
    parser.add_argument("--port", default=9047, type=int, help="Dremio port")
    parser.add_argument("--username", required=True, help="Dremio username")
    parser.add_argument("--password", required=True, help="Dremio password")
    parser.add_argument("--query-dir", required=True, help="Directory containing SQL query files")
    parser.add_argument("--output", required=True, help="Output CSV file")
    parser.add_argument("--concurrency", default=1, type=int, help="Number of concurrent queries")
    parser.add_argument("--iterations", default=1, type=int, help="Number of iterations")
    parser.add_argument("--no-ssl", action="store_true", help="Disable SSL")
    
    args = parser.parse_args()
    
    # Initialize Dremio client
    dremio = DremioBenchmark(
        host=args.host,
        port=args.port,
        username=args.username,
        password=args.password,
        use_ssl=not args.no_ssl
    )
    
    # Load queries
    queries = load_queries(args.query_dir)
    logger.info(f"Loaded {len(queries)} queries from {args.query_dir}")
    
    # Run benchmarks
    run_benchmarks(
        dremio_client=dremio,
        queries=queries,
        output_file=args.output,
        concurrency=args.concurrency,
        iterations=args.iterations
    )

if __name__ == "__main__":
    main() 