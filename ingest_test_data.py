import os
import sys
import gc
import time
import psutil
import requests
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pyorc
import pkg_resources
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
from loguru import logger
from tqdm import tqdm
from tabulate import tabulate
from colorama import init, Fore, Style
from dotenv import load_dotenv

# Initialize colorama for Windows
init()

# Configure logging
logger.remove()
logger.add(
    sys.stderr,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level="INFO"
)
logger.add(
    "data_ingestion.log",
    rotation="500 MB",
    retention="10 days",
    level="DEBUG"
)

def check_version_compatibility():
    """Check if installed package versions match required versions."""
    required_versions = {
        'pandas': '1.5.3',
        'numpy': '1.23.5',
        'pyarrow': '12.0.1',
        'pyorc': '0.10.0',
        'requests': '2.31.0',
        'python-dotenv': '1.0.0',
        'psutil': '5.9.5',
        'loguru': '0.7.2',
        'tqdm': '4.65.0',
        'tabulate': '0.9.0',
        'colorama': '0.4.6'
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
    """A class for ingesting data into Dremio with memory optimization."""
    
    def __init__(self, dremio_url: str, username: str, password: str):
        """
        Initialize the DremioIngester.
        
        Args:
            dremio_url: URL of the Dremio server
            username: Dremio username
            password: Dremio password
        """
        self.dremio_url = dremio_url.rstrip('/')
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.session.auth = (username, password)
        self.available_memory = None
        self.min_required_memory = 2 * 1024 * 1024 * 1024  # 2GB minimum
        self.performance_metrics = {
            'file_sizes': [],
            'ingestion_times': [],
            'memory_usage': []
        }
        
        # Check system resources
        self._check_system_resources()
        
        # Initialize supported formats
        self.supported_formats = {
            'csv': {'extensions': ['.csv'], 'mime_type': 'text/csv'},
            'txt': {'extensions': ['.txt'], 'mime_type': 'text/plain'},
            'parquet': {'extensions': ['.parquet'], 'mime_type': 'application/x-parquet'},
            'orc': {'extensions': ['.orc'], 'mime_type': 'application/x-orc'}
        }

    def _check_system_resources(self):
        """Check system resources and set optimal parameters for 64-bit system."""
        try:
            # Check available memory (64-bit system)
            self.available_memory = psutil.virtual_memory().available
            if self.available_memory < self.min_required_memory:
                raise RuntimeError(f"Insufficient memory. Available: {self.available_memory/1024/1024/1024:.2f}GB, Required: 2GB")
            
            # Log system information
            logger.info("System Information:")
            logger.info(f"- Available Memory: {self.available_memory/1024/1024/1024:.2f}GB")
            
        except Exception as e:
            logger.error(f"System resource check failed: {str(e)}")
            raise

    def _cleanup_memory(self):
        """Clean up memory and force garbage collection for 64-bit system."""
        gc.collect()
        current_memory = psutil.Process().memory_info().rss
        if current_memory > self.available_memory * 0.8:  # If using more than 80% of available memory
            logger.warning(f"High memory usage detected: {current_memory/1024/1024/1024:.2f}GB")
            gc.collect(2)  # Force garbage collection with generation 2 objects
            gc.collect(1)  # Also collect generation 1 objects
            gc.collect(0)  # And generation 0 objects

    def _validate_file_format(self, file_path: str) -> str:
        """Validate file format and return the format type."""
        try:
            file_ext = Path(file_path).suffix.lower()
            for fmt, info in self.supported_formats.items():
                if file_ext in info['extensions']:
                    return fmt
            raise ValueError(f"Unsupported file format: {file_ext}")
        except Exception as e:
            logger.error(f"File format validation failed: {str(e)}")
            raise

    def _validate_file_integrity(self, file_path: str, fmt: str):
        """Validate file integrity based on format."""
        try:
            if fmt == 'csv' or fmt == 'txt':
                # Check if file has header and data
                df = pd.read_csv(file_path, nrows=1)
                if df.empty:
                    raise ValueError("File is empty")
                if len(df.columns) != 10:  # Expected number of columns
                    raise ValueError(f"Invalid number of columns: {len(df.columns)}")
            
            elif fmt == 'parquet':
                # Check Parquet file structure
                table = pq.read_table(file_path)
                if table.num_rows == 0:
                    raise ValueError("File is empty")
                if len(table.column_names) != 10:  # Expected number of columns
                    raise ValueError(f"Invalid number of columns: {len(table.column_names)}")
            
            elif fmt == 'orc':
                # Check ORC file structure using pyorc 0.10.0
                with pyorc.Reader(file_path) as reader:
                    if reader.num_rows == 0:
                        raise ValueError("File is empty")
                    schema = reader.schema
                    if len(schema.fields) != 10:  # Expected number of columns
                        raise ValueError(f"Invalid number of columns: {len(schema.fields)}")
                    
                    # Verify schema types
                    expected_types = {
                        'id': 'bigint',
                        'name': 'string',
                        'age': 'int',
                        'salary': 'double',
                        'department': 'string',
                        'hire_date': 'string',
                        'is_active': 'boolean',
                        'performance_score': 'float',
                        'years_of_service': 'int',
                        'bonus': 'double'
                    }
                    
                    for field, expected_type in expected_types.items():
                        if str(schema.fields[field].type) != expected_type:
                            raise ValueError(f"Invalid type for column {field}: expected {expected_type}, got {schema.fields[field].type}")
            
            logger.info(f"File integrity check passed for {file_path}")
            
        except Exception as e:
            logger.error(f"File integrity check failed: {str(e)}")
            raise

    def _record_performance_metrics(self, file_size: int, ingestion_time: float):
        """Record performance metrics for analysis."""
        self.performance_metrics['file_sizes'].append(file_size)
        self.performance_metrics['ingestion_times'].append(ingestion_time)
        self.performance_metrics['memory_usage'].append(psutil.Process().memory_info().rss)

    def _print_performance_summary(self):
        """Print a summary of performance metrics."""
        if not self.performance_metrics['file_sizes']:
            logger.warning("No performance metrics recorded")
            return

        summary = {
            'Average File Size (MB)': np.mean(self.performance_metrics['file_sizes']) / (1024 * 1024),
            'Average Ingestion Time (s)': np.mean(self.performance_metrics['ingestion_times']),
            'Average Memory Usage (MB)': np.mean(self.performance_metrics['memory_usage']) / (1024 * 1024),
            'Total Files Ingested': len(self.performance_metrics['file_sizes'])
        }

        logger.info("\nPerformance Summary:")
        logger.info(tabulate(summary.items(), headers=['Metric', 'Value'], tablefmt='grid'))

    def ingest_file(self, file_path: str, space_name: str, table_name: str) -> bool:
        """Ingest a file into Dremio."""
        try:
            # Validate file format
            fmt = self._validate_file_format(file_path)
            
            # Validate file integrity
            self._validate_file_integrity(file_path, fmt)
            
            # Start ingestion
            start_time = time.time()
            file_size = Path(file_path).stat().st_size
            
            # Prepare ingestion request
            headers = {
                'Content-Type': self.supported_formats[fmt]['mime_type']
            }
            
            # Upload file
            with open(file_path, 'rb') as f:
                response = self.session.post(
                    f"{self.dremio_url}/api/v3/dataset/{space_name}/{table_name}",
                    headers=headers,
                    data=f
                )
            
            if response.status_code != 200:
                raise Exception(f"Failed to ingest file: {response.text}")
            
            ingestion_time = time.time() - start_time
            
            # Record metrics
            self._record_performance_metrics(file_size, ingestion_time)
            
            logger.info(f"Successfully ingested {file_path}")
            logger.info(f"Size: {file_size/1024/1024:.2f}MB")
            logger.info(f"Time: {ingestion_time:.2f}s")
            
            return True
            
        except Exception as e:
            logger.error(f"Error ingesting file: {str(e)}")
            return False

def main():
    """Main function to ingest test data into Dremio."""
    try:
        # Load environment variables
        load_dotenv()
        
        # Get Dremio credentials
        dremio_url = os.getenv('DREMIO_URL')
        username = os.getenv('DREMIO_USERNAME')
        password = os.getenv('DREMIO_PASSWORD')
        
        if not all([dremio_url, username, password]):
            raise ValueError("Missing required environment variables")
        
        # Create ingester
        ingester = DremioIngester(dremio_url, username, password)
        
        # Get list of files to ingest
        test_data_dir = Path("test_data")
        if not test_data_dir.exists():
            raise FileNotFoundError("Test data directory not found")
        
        files = list(test_data_dir.glob("*.*"))
        if not files:
            raise FileNotFoundError("No files found in test data directory")
        
        # Ingest each file
        for file_path in files:
            logger.info(f"\n{Fore.CYAN}Ingesting {file_path.name}...{Style.RESET_ALL}")
            try:
                ingester.ingest_file(str(file_path), "test_space", file_path.stem)
            except Exception as e:
                logger.error(f"Failed to ingest {file_path.name}: {str(e)}")
                continue
        
        # Print performance summary
        ingester._print_performance_summary()
        
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 