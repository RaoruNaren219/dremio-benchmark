#!/usr/bin/env python3

"""
Cross-Cluster Setup Script for Dremio
This script automates the setup of cross-cluster access between Dremio A and Dremio B
"""

import argparse
import logging
import sys
import requests
import json
import time
from typing import Dict, Optional, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("cross_cluster_setup.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class DremioClient:
    def __init__(self, host: str, port: int, username: str, password: str, use_ssl: bool = True):
        """
        Initialize Dremio client
        
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
    
    def create_source(self, source_name: str, config: Dict[str, Any]) -> bool:
        """
        Create a source in Dremio
        
        Args:
            source_name (str): Name of the source
            config (Dict[str, Any]): Source configuration
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.token:
            if not self.login():
                return False
        
        catalog_url = f"{self.base_url}/catalog"
        
        # Prepare source payload
        payload = {
            "entityType": "source",
            "name": source_name,
            "config": config
        }
        
        try:
            response = requests.post(catalog_url, headers=self.headers, json=payload)
            response.raise_for_status()
            logger.info(f"Successfully created source {source_name}")
            return True
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 409:
                logger.warning(f"Source {source_name} already exists")
                return True
            logger.error(f"Failed to create source {source_name}: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error creating source {source_name}: {str(e)}")
            return False
    
    def create_user(self, username: str, password: str, first_name: str, last_name: str, email: str) -> bool:
        """
        Create a user in Dremio
        
        Args:
            username (str): Username
            password (str): Password
            first_name (str): First name
            last_name (str): Last name
            email (str): Email
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.token:
            if not self.login():
                return False
        
        user_url = f"{self.base_url}/user"
        
        # Prepare user payload
        payload = {
            "userName": username,
            "firstName": first_name,
            "lastName": last_name,
            "email": email,
            "password": password
        }
        
        try:
            response = requests.post(user_url, headers=self.headers, json=payload)
            response.raise_for_status()
            logger.info(f"Successfully created user {username}")
            return True
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 409:
                logger.warning(f"User {username} already exists")
                return True
            logger.error(f"Failed to create user {username}: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error creating user {username}: {str(e)}")
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

def setup_cross_cluster(dremio_a: DremioClient, dremio_b: DremioClient, 
                       dremio_a_host: str, dremio_b_host: str, 
                       cross_user: str, cross_password: str) -> bool:
    """
    Set up cross-cluster access between Dremio A and Dremio B
    
    Args:
        dremio_a (DremioClient): Dremio A client
        dremio_b (DremioClient): Dremio B client
        dremio_a_host (str): Dremio A host
        dremio_b_host (str): Dremio B host
        cross_user (str): Username for cross-cluster access
        cross_password (str): Password for cross-cluster access
    
    Returns:
        bool: True if successful, False otherwise
    """
    # Step 1: Create cross-cluster user in both Dremio instances
    logger.info("Creating cross-cluster user in Dremio A")
    dremio_a.create_user(
        username=cross_user,
        password=cross_password,
        first_name="Cross",
        last_name="Cluster",
        email="cross.cluster@example.com"
    )
    
    logger.info("Creating cross-cluster user in Dremio B")
    dremio_b.create_user(
        username=cross_user,
        password=cross_password,
        first_name="Cross",
        last_name="Cluster",
        email="cross.cluster@example.com"
    )
    
    # Step 2: Configure Dremio B as a source in Dremio A
    logger.info("Configuring Dremio B as a source in Dremio A")
    dremio_b_config = {
        "type": "DREMIO",
        "config": {
            "hostname": dremio_b_host,
            "port": 9047,
            "authenticationType": "BASIC",
            "username": cross_user,
            "password": cross_password,
            "enableSSL": True
        }
    }
    
    dremio_a.create_source("DremioB", dremio_b_config)
    
    # Step 3: Configure Dremio A as a source in Dremio B
    logger.info("Configuring Dremio A as a source in Dremio B")
    dremio_a_config = {
        "type": "DREMIO",
        "config": {
            "hostname": dremio_a_host,
            "port": 9047,
            "authenticationType": "BASIC",
            "username": cross_user,
            "password": cross_password,
            "enableSSL": True
        }
    }
    
    dremio_b.create_source("DremioA", dremio_a_config)
    
    # Step 4: Create a virtual dataset for testing cross-cluster access in Dremio A
    logger.info("Creating a virtual dataset for testing cross-cluster access in Dremio A")
    test_vds_sql_a = """
    CREATE VDS IF NOT EXISTS cross_cluster_test AS 
    SELECT * FROM DremioB.hdfs.tpcds_1gb_parquet.customer LIMIT 10;
    """
    
    dremio_a.execute_sql(test_vds_sql_a)
    
    # Step 5: Create a virtual dataset for testing cross-cluster access in Dremio B
    logger.info("Creating a virtual dataset for testing cross-cluster access in Dremio B")
    test_vds_sql_b = """
    CREATE VDS IF NOT EXISTS cross_cluster_test AS 
    SELECT * FROM DremioA.hdfs.tpcds_1gb_parquet.customer LIMIT 10;
    """
    
    dremio_b.execute_sql(test_vds_sql_b)
    
    logger.info("Cross-cluster setup completed successfully!")
    return True

def main():
    """Main function to set up cross-cluster access"""
    parser = argparse.ArgumentParser(description="Set up cross-cluster access between Dremio A and Dremio B")
    
    # Dremio A options
    parser.add_argument("--dremio-a-host", required=True, help="Dremio A host")
    parser.add_argument("--dremio-a-port", type=int, default=9047, help="Dremio A port")
    parser.add_argument("--dremio-a-username", required=True, help="Dremio A username")
    parser.add_argument("--dremio-a-password", required=True, help="Dremio A password")
    
    # Dremio B options
    parser.add_argument("--dremio-b-host", required=True, help="Dremio B host")
    parser.add_argument("--dremio-b-port", type=int, default=9047, help="Dremio B port")
    parser.add_argument("--dremio-b-username", required=True, help="Dremio B username")
    parser.add_argument("--dremio-b-password", required=True, help="Dremio B password")
    
    # Cross-cluster options
    parser.add_argument("--cross-user", default="cross_cluster", help="Username for cross-cluster access")
    parser.add_argument("--cross-password", required=True, help="Password for cross-cluster access")
    
    # SSL options
    parser.add_argument("--no-ssl", action="store_true", help="Disable SSL")
    
    args = parser.parse_args()
    
    logger.info("Starting cross-cluster setup...")
    
    # Initialize Dremio clients
    dremio_a = DremioClient(
        host=args.dremio_a_host,
        port=args.dremio_a_port,
        username=args.dremio_a_username,
        password=args.dremio_a_password,
        use_ssl=not args.no_ssl
    )
    
    dremio_b = DremioClient(
        host=args.dremio_b_host,
        port=args.dremio_b_port,
        username=args.dremio_b_username,
        password=args.dremio_b_password,
        use_ssl=not args.no_ssl
    )
    
    # Set up cross-cluster access
    setup_cross_cluster(
        dremio_a=dremio_a,
        dremio_b=dremio_b,
        dremio_a_host=args.dremio_a_host,
        dremio_b_host=args.dremio_b_host,
        cross_user=args.cross_user,
        cross_password=args.cross_password
    )

if __name__ == "__main__":
    main() 