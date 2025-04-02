import os
import time
import argparse
import sys
from pathlib import Path
from typing import List, Dict, Optional
from loguru import logger
import requests
from dotenv import load_dotenv
from tqdm import tqdm
import json
import psutil
import hashlib
from metrics_collector import MetricsCollector

class DremioIngester:
    def __init__(self, dremio_url: str, username: str, password: str):
        """
        Initialize the Dremio ingester.
        
        Args:
            dremio_url (str): Dremio server URL
            username (str): Dremio username
            password (str): Dremio password
        """
        self.dremio_url = dremio_url.rstrip('/')
        self.username = username
        self.password = password
        self.token = None
        self.headers = None
        self.metrics_collector = MetricsCollector()
        self.authenticate()

    def authenticate(self) -> None:
        """Authenticate with Dremio server."""
        auth_url = f"{self.dremio_url}/apiv2/login"
        auth_data = {
            "userName": self.username,
            "password": self.password
        }
        
        try:
            response = requests.post(auth_url, json=auth_data, timeout=30)
            response.raise_for_status()
            self.token = response.json()['token']
            self.headers = {
                'Authorization': f'_dremio{self.token}',
                'Content-Type': 'application/json'
            }
            logger.info("Successfully authenticated with Dremio")
        except requests.exceptions.Timeout:
            logger.error("Authentication timed out")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Authentication failed: {str(e)}")
            raise
        except KeyError:
            logger.error("Invalid response format from Dremio server")
            raise

    def create_space(self, space_name: str) -> None:
        """
        Create a new space in Dremio if it doesn't exist.
        
        Args:
            space_name (str): Name of the space to create
        """
        try:
            # Check if space exists
            response = requests.get(
                f"{self.dremio_url}/api/v3/catalog",
                headers=self.headers,
                timeout=30
            )
            response.raise_for_status()
            spaces = response.json().get('data', [])
            
            if not any(space['path'][0] == space_name for space in spaces):
                # Create new space
                create_data = {
                    "entityType": "space",
                    "name": space_name
                }
                response = requests.post(
                    f"{self.dremio_url}/api/v3/catalog",
                    headers=self.headers,
                    json=create_data,
                    timeout=30
                )
                response.raise_for_status()
                logger.info(f"Created new space: {space_name}")
            else:
                logger.info(f"Space {space_name} already exists")
        except requests.exceptions.Timeout:
            logger.error("Space creation timed out")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to create space: {str(e)}")
            raise

    def validate_file(self, file_path: str, file_format: str) -> bool:
        """
        Validate file before ingestion.
        
        Args:
            file_path (str): Path to the file
            file_format (str): Format of the file
            
        Returns:
            bool: True if file is valid, False otherwise
        """
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                logger.error(f"File not found: {file_path}")
                return False
                
            if not file_path.is_file():
                logger.error(f"Path is not a file: {file_path}")
                return False
                
            # Check file size
            file_size = file_path.stat().st_size
            if file_size == 0:
                logger.error(f"File is empty: {file_path}")
                return False
                
            # Check available memory (need at least 2x file size)
            available_memory = psutil.virtual_memory().available
            if available_memory < file_size * 2:
                logger.error(f"Insufficient memory for file {file_path}")
                return False
                
            # Basic format validation
            if file_format.lower() == 'csv':
                # Check if file has header and data
                with open(file_path, 'r') as f:
                    header = f.readline().strip()
                    data = f.readline().strip()
                    if not header or not data:
                        logger.error(f"Invalid CSV format: {file_path}")
                        return False
                        
            elif file_format.lower() in ['parquet', 'orc']:
                # These formats will be validated during ingestion
                pass
                
            return True
            
        except Exception as e:
            logger.error(f"File validation failed: {str(e)}")
            return False

    def ingest_file(self, file_path: str, space_name: str, table_name: str, file_format: str) -> bool:
        """
        Ingest a file into Dremio.
        
        Args:
            file_path (str): Path to the file to ingest
            space_name (str): Name of the space to create table in
            table_name (str): Name of the table to create
            file_format (str): Format of the file (csv, parquet, orc)
            
        Returns:
            bool: True if ingestion was successful, False otherwise
        """
        try:
            # Validate file before ingestion
            if not self.validate_file(file_path, file_format):
                return False
                
            # Prepare the ingestion request
            ingestion_data = {
                "entityType": "dataset",
                "path": [space_name, table_name],
                "type": "PHYSICAL_DATASET",
                "format": file_format.upper(),
                "location": file_path
            }
            
            # Start ingestion
            logger.info(f"Starting ingestion of {file_path} into {space_name}.{table_name}")
            start_time = time.time()
            
            response = requests.post(
                f"{self.dremio_url}/api/v3/catalog",
                headers=self.headers,
                json=ingestion_data,
                timeout=30
            )
            response.raise_for_status()
            
            # Wait for ingestion to complete
            job_id = response.json().get('id')
            self._wait_for_job_completion(job_id)
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Record metrics
            file_size = Path(file_path).stat().st_size / (1024 * 1024 * 1024)  # Convert to GB
            self.metrics_collector.record_ingestion_metrics(
                file_format=file_format,
                file_size=file_size,
                ingestion_time=duration,
                success=True,
                data_lake="dremio",
                additional_metrics={
                    "space_name": space_name,
                    "table_name": table_name,
                    "job_id": job_id
                }
            )
            
            logger.info(f"Successfully ingested {file_path} in {duration:.2f} seconds")
            return True
            
        except requests.exceptions.Timeout:
            logger.error(f"Ingestion timed out for {file_path}")
            self._record_failed_ingestion(file_path, file_format, time.time() - start_time)
            return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to ingest {file_path}: {str(e)}")
            self._record_failed_ingestion(file_path, file_format, time.time() - start_time)
            return False
        except Exception as e:
            logger.error(f"Unexpected error during ingestion: {str(e)}")
            self._record_failed_ingestion(file_path, file_format, time.time() - start_time)
            return False

    def _wait_for_job_completion(self, job_id: str, timeout: int = 3600) -> bool:
        """
        Wait for a job to complete.
        
        Args:
            job_id (str): ID of the job to wait for
            timeout (int): Maximum time to wait in seconds
            
        Returns:
            bool: True if job completed successfully, False otherwise
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = requests.get(
                    f"{self.dremio_url}/api/v3/job/{job_id}",
                    headers=self.headers,
                    timeout=30
                )
                response.raise_for_status()
                job_state = response.json().get('jobState')
                
                if job_state == 'COMPLETED':
                    return True
                elif job_state in ['FAILED', 'CANCELED']:
                    raise Exception(f"Job failed with state: {job_state}")
                
                time.sleep(5)  # Wait 5 seconds before checking again
                
            except requests.exceptions.Timeout:
                logger.error("Job status check timed out")
                raise
            except requests.exceptions.RequestException as e:
                logger.error(f"Error checking job status: {str(e)}")
                raise
        
        raise TimeoutError("Job timed out")

    def _record_failed_ingestion(self, file_path: str, file_format: str, duration: float) -> None:
        """Record metrics for a failed ingestion."""
        try:
            file_size = Path(file_path).stat().st_size / (1024 * 1024 * 1024)  # Convert to GB
            self.metrics_collector.record_ingestion_metrics(
                file_format=file_format,
                file_size=file_size,
                ingestion_time=duration,
                success=False,
                data_lake="dremio",
                additional_metrics={
                    "error": "Ingestion failed"
                }
            )
        except Exception as e:
            logger.error(f"Failed to record metrics for failed ingestion: {str(e)}")

def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Ingest test data into Dremio')
    
    # File size options
    parser.add_argument(
        '--sizes',
        nargs='+',
        type=int,
        choices=[1, 10, 100],
        default=[1, 10, 100],
        help='File sizes to ingest (in GB). Choices: 1, 10, 100'
    )
    
    # Format options
    parser.add_argument(
        '--formats',
        nargs='+',
        choices=['csv', 'txt', 'parquet', 'orc'],
        default=['csv', 'txt', 'parquet', 'orc'],
        help='File formats to ingest. Choices: csv, txt, parquet, orc'
    )
    
    # Space name option
    parser.add_argument(
        '--space',
        type=str,
        default='test_data',
        help='Dremio space name for ingested data'
    )
    
    # Dry run option
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview what would be ingested without actually ingesting'
    )
    
    return parser.parse_args()

def validate_environment() -> bool:
    """
    Validate environment variables and system requirements.
    
    Returns:
        bool: True if environment is valid, False otherwise
    """
    try:
        # Check environment variables
        required_vars = ['DREMIO_URL', 'DREMIO_USERNAME', 'DREMIO_PASSWORD']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
            return False
            
        # Check system memory
        memory = psutil.virtual_memory()
        if memory.available < 2 * 1024 * 1024 * 1024:  # 2GB minimum
            logger.error("Insufficient system memory")
            return False
            
        # Check disk space
        disk = psutil.disk_usage('/')
        if disk.free < 10 * 1024 * 1024 * 1024:  # 10GB minimum
            logger.error("Insufficient disk space")
            return False
            
        return True
        
    except Exception as e:
        logger.error(f"Environment validation failed: {str(e)}")
        return False

def main() -> None:
    """Main function to run the ingestion process."""
    try:
        # Parse command line arguments
        args = parse_arguments()
        
        # Load environment variables
        load_dotenv()
        
        # Validate environment
        if not validate_environment():
            sys.exit(1)
        
        # Dremio configuration
        dremio_url = os.getenv('DREMIO_URL')
        username = os.getenv('DREMIO_USERNAME')
        password = os.getenv('DREMIO_PASSWORD')
        
        # Initialize ingester
        ingester = DremioIngester(dremio_url, username, password)
        
        # Create space for test data
        ingester.create_space(args.space)
        
        # Process selected sizes and formats
        results = {}
        for sf in args.sizes:
            results[f'sf{sf}'] = {}
            for fmt in args.formats:
                file_path = os.path.join('test_data', f'sf{sf}', f'data_{sf}gb.{fmt}')
                if os.path.exists(file_path):
                    table_name = f'data_{sf}gb_{fmt}'
                    if args.dry_run:
                        results[f'sf{sf}'][fmt] = 'Would ingest'
                    else:
                        success = ingester.ingest_file(file_path, args.space, table_name, fmt)
                        results[f'sf{sf}'][fmt] = 'Success' if success else 'Failed'
                else:
                    results[f'sf{sf}'][fmt] = 'File not found'
        
        # Print summary
        logger.info("\nIngestion Summary:")
        for sf, formats in results.items():
            logger.info(f"\nScale Factor: {sf}")
            for fmt, status in formats.items():
                logger.info(f"{fmt.upper()}: {status}")
                
    except KeyboardInterrupt:
        logger.warning("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 