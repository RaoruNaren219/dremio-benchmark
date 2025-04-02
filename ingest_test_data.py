import os
import sys
import time
import argparse
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
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyorc
import gc
import pkg_resources

def check_version_compatibility():
    """Check if installed package versions match required versions."""
    required_versions = {
        'pandas': '1.5.3',
        'numpy': '1.23.5',
        'pyarrow': '12.0.1',
        'pyorc': '1.7.0',
        'requests': '2.31.0',
        'python-dotenv': '1.0.0',
        'tqdm': '4.65.0',
        'psutil': '5.9.5',
        'loguru': '0.7.2'
    }
    
    incompatible_packages = []
    for package, required_version in required_versions.items():
        try:
            installed_version = pkg_resources.get_distribution(package).version
            if installed_version != required_version:
                incompatible_packages.append(f"{package}=={required_version} (installed: {installed_version})")
        except pkg_resources.DistributionNotFound:
            incompatible_packages.append(f"{package}=={required_version} (not installed)")
    
    if incompatible_packages:
        logger.error("Incompatible package versions detected:")
        for package in incompatible_packages:
            logger.error(f"  - {package}")
        logger.error("\nPlease install the correct versions using:")
        logger.error("pip install " + " ".join(incompatible_packages))
        sys.exit(1)

# Check version compatibility at startup
check_version_compatibility()

class DremioIngester:
    def __init__(self, dremio_url: str, username: str, password: str, space_name: str = "test_data"):
        """Initialize the Dremio ingester with connection details."""
        self.dremio_url = dremio_url.rstrip('/')
        self.username = username
        self.password = password
        self.space_name = space_name
        self.token = None
        self.headers = None
        self.metrics_collector = MetricsCollector()
        self.supported_formats = {
            'CSV': {'extensions': ['.csv'], 'mime_type': 'text/csv'},
            'TXT': {'extensions': ['.txt'], 'mime_type': 'text/plain'},
            'PARQUET': {'extensions': ['.parquet'], 'mime_type': 'application/x-parquet'},
            'ORC': {'extensions': ['.orc'], 'mime_type': 'application/x-orc'}
        }
        
        # Configure logging
        logger.add(
            "data_ingestion.log",
            rotation="1 day",
            retention="7 days",
            level="INFO",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
        )

    def authenticate(self) -> bool:
        """Authenticate with Dremio and get token."""
        try:
            auth_url = f"{self.dremio_url}/apiv2/login"
            response = requests.post(
                auth_url,
                json={"userName": self.username, "password": self.password}
            )
            response.raise_for_status()
            self.token = response.json()["token"]
            self.headers = {
                'Authorization': f'_dremio{self.token}',
                'Content-Type': 'application/json'
            }
            logger.info("Successfully authenticated with Dremio")
            return True
        except Exception as e:
            logger.error(f"Authentication failed: {str(e)}")
            return False

    def create_space(self) -> bool:
        """Create a space in Dremio if it doesn't exist."""
        try:
            catalog_url = f"{self.dremio_url}/api/v3/catalog"
            
            # Check if space exists
            response = requests.get(catalog_url, headers=self.headers)
            response.raise_for_status()
            spaces = response.json().get("data", [])
            
            if not any(space["name"] == self.space_name for space in spaces):
                # Create space
                create_url = f"{self.dremio_url}/api/v3/catalog"
                payload = {
                    "entityType": "space",
                    "name": self.space_name,
                    "description": "Space for test data ingestion"
                }
                response = requests.post(create_url, headers=self.headers, json=payload)
                response.raise_for_status()
                logger.info(f"Created space: {self.space_name}")
            else:
                logger.info(f"Space {self.space_name} already exists")
            
            return True
        except Exception as e:
            logger.error(f"Failed to create space: {str(e)}")
            return False

    def _validate_file_format(self, file_path: str) -> Optional[str]:
        """Validate and return the file format type."""
        try:
            file_ext = Path(file_path).suffix.lower()
            for format_type, format_info in self.supported_formats.items():
                if file_ext in format_info['extensions']:
                    return format_type
            logger.error(f"Unsupported file format: {file_ext}")
            return None
        except Exception as e:
            logger.error(f"Error validating file format: {str(e)}")
            return None

    def _validate_file_integrity(self, file_path: str, format_type: str) -> bool:
        """Validate file integrity based on format type."""
        try:
            if format_type == 'CSV':
                # Validate CSV structure
                with open(file_path, 'r') as f:
                    header = f.readline().strip()
                    if not header:
                        logger.error("CSV file has no header")
                        return False
                    data = f.readline().strip()
                    if not data:
                        logger.error("CSV file has no data")
                        return False
                    # Verify column count matches header
                    header_cols = len(header.split(','))
                    data_cols = len(data.split(','))
                    if header_cols != data_cols:
                        logger.error(f"CSV column count mismatch: header={header_cols}, data={data_cols}")
                        return False

            elif format_type == 'PARQUET':
                # Validate Parquet file
                try:
                    pq.read_table(file_path)
                except Exception as e:
                    logger.error(f"Invalid Parquet file: {str(e)}")
                    return False

            elif format_type == 'ORC':
                # Validate ORC file
                try:
                    with pyorc.Reader(file_path) as reader:
                        if reader.num_rows == 0:
                            logger.error("ORC file has no rows")
                            return False
                except Exception as e:
                    logger.error(f"Invalid ORC file: {str(e)}")
                    return False

            return True

        except Exception as e:
            logger.error(f"Error validating file integrity: {str(e)}")
            return False

    def ingest_file(self, file_path: str, format_type: str) -> bool:
        """Ingest a file into Dremio with improved validation and 64-bit optimization."""
        try:
            if not self.token:
                if not self.authenticate():
                    return False

            # Validate file format
            detected_format = self._validate_file_format(file_path)
            if not detected_format or detected_format != format_type:
                logger.error(f"File format mismatch: expected {format_type}, detected {detected_format}")
                return False

            # Validate file integrity
            if not self._validate_file_integrity(file_path, format_type):
                return False

            headers = {"Authorization": f"_dremio{self.token}"}
            file_name = Path(file_path).name
            table_name = Path(file_path).stem

            # Get file size for memory management
            file_size = os.path.getsize(file_path)
            logger.info(f"Processing file: {file_name} ({file_size/1024/1024/1024:.2f}GB)")

            # Prepare ingestion payload with format-specific settings
            payload = {
                "entityType": "dataset",
                "name": table_name,
                "path": [self.space_name, table_name],
                "type": format_type.upper(),
                "format": {
                    "type": format_type.lower(),
                    "extractHeader": format_type in ['CSV', 'TXT'],
                    "trimValues": True,
                    "hasHeader": format_type in ['CSV', 'TXT'],
                    "skipFirstLine": False,
                    "extractFieldNames": True
                },
                "location": file_path
            }

            # Add format-specific settings with 64-bit optimizations
            if format_type == 'PARQUET':
                payload["format"]["parquet"] = {
                    "autoCorrectCorruptDates": True,
                    "readInt96AsTimestamp": True,
                    "useParquetNativeReader": True  # Use native reader for better performance
                }
            elif format_type == 'ORC':
                payload["format"]["orc"] = {
                    "autoCorrectCorruptDates": True,
                    "useOrcNativeReader": True  # Use native reader for better performance
                }

            # Create dataset
            create_url = f"{self.dremio_url}/api/v3/catalog"
            response = requests.post(create_url, headers=headers, json=payload)
            response.raise_for_status()
            
            # Start refresh job with timeout
            job_url = f"{self.dremio_url}/api/v3/job"
            job_payload = {
                "sql": f"REFRESH DATASET {self.space_name}.{table_name}"
            }
            response = requests.post(job_url, headers=headers, json=job_payload)
            response.raise_for_status()
            job_id = response.json()["id"]

            # Monitor job status with timeout and memory management
            start_time = time.time()
            timeout = 3600  # 1 hour timeout
            while True:
                if time.time() - start_time > timeout:
                    logger.error(f"Job timed out after {timeout} seconds")
                    return False

                # Check memory usage
                current_memory = psutil.Process().memory_info().rss
                if current_memory > psutil.virtual_memory().available * 0.8:
                    logger.warning(f"High memory usage detected: {current_memory/1024/1024/1024:.2f}GB")
                    gc.collect(2)
                    gc.collect(1)
                    gc.collect(0)

                status_url = f"{self.dremio_url}/api/v3/job/{job_id}"
                response = requests.get(status_url, headers=headers)
                response.raise_for_status()
                job_state = response.json()["jobState"]
                
                if job_state == "COMPLETED":
                    logger.info(f"Successfully ingested {file_name}")
                    return True
                elif job_state in ["FAILED", "CANCELED"]:
                    logger.error(f"Job failed for {file_name}")
                    return False
                
                time.sleep(2)

        except Exception as e:
            logger.error(f"Failed to ingest {file_path}: {str(e)}")
            return False

    def ingest_all_files(self, data_dir: str) -> Dict[str, List[str]]:
        """Ingest all files from the data directory."""
        results = {
            "success": [],
            "failed": []
        }
        
        try:
            # Create space if it doesn't exist
            if not self.create_space():
                return results

            # Walk through the data directory
            for root, _, files in os.walk(data_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    format_type = self._validate_file_format(file_path)
                    
                    if format_type:
                        logger.info(f"Processing {file_path}")
                        if self.ingest_file(file_path, format_type):
                            results["success"].append(file_path)
                        else:
                            results["failed"].append(file_path)
                    
                    # Memory management
                    gc.collect()

        except Exception as e:
            logger.error(f"Error during ingestion: {str(e)}")
        
        return results

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
    Validate environment variables and system requirements for 64-bit system.
    
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
            
        # Check system memory (64-bit)
        memory = psutil.virtual_memory()
        if memory.available < 2 * 1024 * 1024 * 1024:  # 2GB minimum
            logger.error(f"Insufficient system memory. Available: {memory.available/1024/1024/1024:.2f}GB, Required: 2GB")
            return False
            
        # Check disk space
        disk = psutil.disk_usage('/')
        if disk.free < 10 * 1024 * 1024 * 1024:  # 10GB minimum
            logger.error(f"Insufficient disk space. Available: {disk.free/1024/1024/1024:.2f}GB, Required: 10GB")
            return False
            
        # Log system information
        logger.info("System Information:")
        logger.info(f"- Available Memory: {memory.available/1024/1024/1024:.2f}GB")
        logger.info(f"- Total Memory: {memory.total/1024/1024/1024:.2f}GB")
        logger.info(f"- Available Disk Space: {disk.free/1024/1024/1024:.2f}GB")
        logger.info(f"- Total Disk Space: {disk.total/1024/1024/1024:.2f}GB")
            
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
        space_name = args.space
        data_dir = os.getenv("DATA_DIR", "test_data")
        
        # Initialize ingester
        ingester = DremioIngester(dremio_url, username, password, space_name)
        
        # Start ingestion
        logger.info("Starting data ingestion...")
        results = ingester.ingest_all_files(data_dir)
        
        # Print summary
        logger.info("\nIngestion Summary:")
        logger.info(f"Successfully ingested: {len(results['success'])} files")
        logger.info(f"Failed to ingest: {len(results['failed'])} files")
        
        if results['failed']:
            logger.info("\nFailed files:")
            for file in results['failed']:
                logger.info(f"- {file}")
        
    except KeyboardInterrupt:
        logger.warning("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 