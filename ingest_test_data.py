# Standard library imports
import os
import sys
import gc
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

# Third-party imports
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import psutil
import requests
import pkg_resources
from loguru import logger
from tqdm import tqdm
from tabulate import tabulate
from colorama import init, Fore, Style
from dotenv import load_dotenv

# Initialize colorama for Windows
init()

# Configure logging with rotation and retention
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

def check_version_compatibility() -> None:
    """
    Check if installed package versions match required versions.
    Exits with status code 1 if incompatible versions are found.
    """
    required_versions = {
        'numpy': '1.23.5',
        'pandas': '1.4.4',
        'pyarrow': '10.0.1',
        'requests': '2.31.0',
        'python-dotenv': '0.21.1',
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

class Config:
    """Configuration management class."""
    
    def __init__(self):
        """Initialize configuration with environment variables."""
        self.load_env()
        self.validate_env()
        
    def load_env(self) -> None:
        """Load environment variables from .env file."""
        load_dotenv()
        self.dremio_url = os.getenv('DREMIO_URL')
        self.dremio_username = os.getenv('DREMIO_USERNAME')
        self.dremio_password = os.getenv('DREMIO_PASSWORD')
        self.dremio_space = os.getenv('DREMIO_SPACE', 'test_data')
        self.chunk_size = int(os.getenv('CHUNK_SIZE', '1048576'))  # Default 1MB
        self.max_retries = int(os.getenv('MAX_RETRIES', '3'))
        self.retry_delay = int(os.getenv('RETRY_DELAY', '5'))
        self.min_memory = int(os.getenv('MIN_MEMORY', '2147483648'))  # Default 2GB
        
    def validate_env(self) -> None:
        """Validate required environment variables."""
        required_vars = {
            'DREMIO_URL': self.dremio_url,
            'DREMIO_USERNAME': self.dremio_username,
            'DREMIO_PASSWORD': self.dremio_password
        }
        
        missing_vars = [var for var, value in required_vars.items() if not value]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
            
        # Validate URL format
        if not self.dremio_url.startswith(('http://', 'https://')):
            raise ValueError("DREMIO_URL must start with http:// or https://")
            
        # Validate numeric values
        if self.chunk_size < 1024 * 1024:  # Less than 1MB
            raise ValueError("CHUNK_SIZE must be at least 1MB")
        if self.max_retries < 1:
            raise ValueError("MAX_RETRIES must be at least 1")
        if self.retry_delay < 1:
            raise ValueError("RETRY_DELAY must be at least 1 second")
        if self.min_memory < 1024 * 1024 * 1024:  # Less than 1GB
            raise ValueError("MIN_MEMORY must be at least 1GB")

class DremioIngester:
    """A class for ingesting data into Dremio with memory optimization."""
    
    def __init__(self, config: Config):
        """Initialize the DremioIngester with configuration."""
        self.config = config
        self.dremio_url = config.dremio_url.rstrip('/')
        self.username = config.dremio_username
        self.password = config.dremio_password
        self.session = self._create_session()
        self.available_memory = None
        self.min_required_memory = config.min_memory
        self.performance_metrics = {
            'file_sizes': [],
            'ingestion_times': [],
            'memory_usage': []
        }
        self.max_retries = config.max_retries
        self.retry_delay = config.retry_delay
        
        # Initialize supported formats with validation
        self.supported_formats = {
            'csv': {'extensions': ['.csv'], 'mime_type': 'text/csv'},
            'txt': {'extensions': ['.txt'], 'mime_type': 'text/plain'},
            'parquet': {'extensions': ['.parquet'], 'mime_type': 'application/x-parquet'}
        }
        
        # Verify connection and resources
        self._check_system_resources()
        self._verify_dremio_connection()

    def _create_session(self) -> requests.Session:
        """Create and configure requests session with proper headers and auth."""
        try:
            session = requests.Session()
            session.auth = (self.username, self.password)
            session.headers.update({
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            })
            return session
        except Exception as e:
            logger.error(f"Failed to create session: {str(e)}")
            raise

    def _verify_dremio_connection(self) -> None:
        """Verify connection to Dremio with retry logic."""
        for attempt in range(self.max_retries):
            try:
                self._check_dremio_connection()
                return
            except ConnectionError as e:
                if attempt == self.max_retries - 1:
                    raise ConnectionError(f"Failed to connect to Dremio after {self.max_retries} attempts: {str(e)}")
                logger.warning(f"Connection attempt {attempt + 1} failed, retrying in {self.retry_delay} seconds...")
                time.sleep(self.retry_delay)

    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Make HTTP request with retry logic and error handling."""
        url = f"{self.dremio_url}{endpoint}"
        for attempt in range(self.max_retries):
            try:
                response = self.session.request(method, url, **kwargs)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                if attempt == self.max_retries - 1:
                    raise ConnectionError(f"Request failed after {self.max_retries} attempts: {str(e)}")
                logger.warning(f"Request attempt {attempt + 1} failed, retrying in {self.retry_delay} seconds...")
                time.sleep(self.retry_delay)

    def _check_dremio_connection(self) -> None:
        """Check connection to Dremio instance and verify data sharing capabilities."""
        try:
            # Check target Dremio connection
            response = self._make_request('GET', '/api/v3/info')
            
            # If source Dremio is specified, check data sharing capabilities
            if self.source_dremio_url:
                source_session = requests.Session()
                source_session.auth = (self.username, self.password)
                source_response = source_session.get(f"{self.source_dremio_url}/api/v3/info")
                if source_response.status_code != 200:
                    raise ConnectionError(f"Failed to connect to source Dremio instance: {source_response.text}")
                
                logger.info("Successfully verified both Dremio instances")
                logger.info("Data sharing capabilities confirmed")
            
        except Exception as e:
            logger.error(f"Dremio connection check failed: {str(e)}")
            raise

    def _check_system_resources(self) -> None:
        """
        Check system resources and set optimal parameters for 64-bit system.
        Raises RuntimeError if insufficient resources are available.
        """
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

    def _cleanup_memory(self) -> None:
        """Clean up memory and force garbage collection."""
        try:
            # Get initial memory usage
            initial_memory = psutil.Process().memory_info().rss
            
            # Force garbage collection
            gc.collect()
            
            # Clear any cached data
            if hasattr(pd, '_cache'):
                pd._cache.clear()
            
            # Get final memory usage
            final_memory = psutil.Process().memory_info().rss
            freed_memory = initial_memory - final_memory
            
            if freed_memory > 0:
                logger.info(f"Memory cleanup freed {freed_memory/1024/1024:.2f}MB")
            else:
                logger.warning("Memory cleanup did not free any memory")
                
        except Exception as e:
            logger.error(f"Error during memory cleanup: {str(e)}")
            raise

    def _validate_file_format(self, file_path: str) -> str:
        """
        Validate file format and return the format type.
        
        Args:
            file_path: Path to the file to validate
            
        Returns:
            Format type (csv, txt, or parquet)
            
        Raises:
            ValueError: If file format is not supported
        """
        try:
            file_ext = Path(file_path).suffix.lower()
            
            # First check by extension
            for fmt, info in self.supported_formats.items():
                if file_ext in info['extensions']:
                    # For parquet files, verify the format
                    if fmt == 'parquet':
                        try:
                            pq.read_table(file_path)
                            return fmt
                        except Exception as e:
                            raise ValueError(f"Invalid Parquet file format: {str(e)}")
                    return fmt
            
            # If extension doesn't match, try to detect format
            try:
                # Try reading as Parquet
                pq.read_table(file_path)
                return 'parquet'
            except:
                try:
                    # Try reading as CSV
                    pd.read_csv(file_path, nrows=1)
                    return 'csv'
                except:
                    try:
                        # Try reading as text
                        with open(file_path, 'r') as f:
                            f.readline()
                        return 'txt'
                    except:
                        raise ValueError(f"Unsupported file format: {file_ext}")
                        
        except Exception as e:
            logger.error(f"File format validation failed: {str(e)}")
            raise

    def _validate_file_integrity(self, file_path: str, fmt: str) -> None:
        """
        Validate file integrity based on format.
        
        Args:
            file_path: Path to the file to validate
            fmt: Format type of the file
            
        Raises:
            ValueError: If file integrity check fails
        """
        try:
            if fmt == 'csv' or fmt == 'txt':
                # Check if file has header and data
                df = pd.read_csv(file_path, nrows=1)
                if df.empty:
                    raise ValueError("File is empty")
                if len(df.columns) != 10:  # Expected number of columns
                    raise ValueError(f"Invalid number of columns: {len(df.columns)}")
                
                # Check data types
                expected_types = {
                    'id': np.int64,
                    'name': str,
                    'email': str,
                    'age': np.int64,
                    'salary': np.float64,
                    'department': str,
                    'hire_date': str,
                    'active': bool,
                    'performance_score': np.float64,
                    'last_review_date': str
                }
                
                for col, expected_type in expected_types.items():
                    if col not in df.columns:
                        raise ValueError(f"Missing required column: {col}")
                    if not pd.api.types.is_dtype_equal(df[col].dtype, expected_type):
                        raise ValueError(f"Invalid data type for column {col}: expected {expected_type}, got {df[col].dtype}")
            
            elif fmt == 'parquet':
                # Check Parquet file structure
                table = pq.read_table(file_path)
                if table.num_rows == 0:
                    raise ValueError("File is empty")
                if len(table.column_names) != 10:  # Expected number of columns
                    raise ValueError(f"Invalid number of columns: {len(table.column_names)}")
                
                # Check schema
                expected_schema = pa.schema([
                    ('id', pa.int64()),
                    ('name', pa.string()),
                    ('email', pa.string()),
                    ('age', pa.int64()),
                    ('salary', pa.float64()),
                    ('department', pa.string()),
                    ('hire_date', pa.string()),
                    ('active', pa.bool_()),
                    ('performance_score', pa.float64()),
                    ('last_review_date', pa.string())
                ])
                
                if not table.schema.equals(expected_schema):
                    raise ValueError("Invalid schema in Parquet file")
            
            logger.info(f"File integrity check passed for {file_path}")
            
        except Exception as e:
            logger.error(f"File integrity check failed: {str(e)}")
            raise

    def _record_performance_metrics(self, file_size: int, ingestion_time: float) -> None:
        """Record performance metrics for analysis."""
        try:
            current_memory = psutil.Process().memory_info().rss
            self.performance_metrics['file_sizes'].append(file_size)
            self.performance_metrics['ingestion_times'].append(ingestion_time)
            self.performance_metrics['memory_usage'].append(current_memory)
            
            # Calculate and log ingestion speed
            speed = file_size / (1024 * 1024 * ingestion_time)  # MB/s
            logger.info(f"Ingestion speed: {speed:.2f}MB/s")
            
        except Exception as e:
            logger.error(f"Error recording performance metrics: {str(e)}")
            # Don't raise the exception as this is not critical

    def _print_performance_summary(self) -> None:
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

    def _validate_file_path(self, file_path: str) -> None:
        """Validate file path and existence."""
        if not file_path or not isinstance(file_path, str):
            raise ValueError("File path must be a non-empty string")
            
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        if not path.is_file():
            raise ValueError(f"Path is not a file: {file_path}")
        if not os.access(file_path, os.R_OK):
            raise PermissionError(f"No read permission for file: {file_path}")

    def _validate_space_name(self, space_name: str) -> None:
        """Validate Dremio space name."""
        if not space_name or not isinstance(space_name, str):
            raise ValueError("Space name must be a non-empty string")
        if not space_name.isalnum() and '_' not in space_name:
            raise ValueError("Space name must contain only alphanumeric characters and underscores")

    def _validate_table_name(self, table_name: str) -> None:
        """Validate table name."""
        if not table_name or not isinstance(table_name, str):
            raise ValueError("Table name must be a non-empty string")
        if not table_name.isalnum() and '_' not in table_name:
            raise ValueError("Table name must contain only alphanumeric characters and underscores")

    def ingest_file(self, file_path: str, space_name: str, table_name: str) -> bool:
        """Ingest a file into Dremio with enhanced validation and error handling."""
        try:
            # Validate inputs
            self._validate_file_path(file_path)
            self._validate_space_name(space_name)
            self._validate_table_name(table_name)
            
            # Validate file and format
            fmt = self._validate_file_format(file_path)
            self._validate_file_integrity(file_path, fmt)
            
            # Start ingestion
            start_time = time.time()
            file_size = Path(file_path).stat().st_size
            chunk_size = self._calculate_optimal_chunk_size(file_size)
            
            # Prepare ingestion request
            headers = {
                'Content-Type': self.supported_formats[fmt]['mime_type']
            }
            
            # Upload file in chunks with memory monitoring
            with open(file_path, 'rb') as f:
                uploaded_size = 0
                with tqdm(total=file_size, unit='B', unit_scale=True) as pbar:
                    while uploaded_size < file_size:
                        # Check memory usage before reading chunk
                        self._monitor_memory_usage()
                        
                        # Read and upload chunk
                        chunk = f.read(chunk_size)
                        if not chunk:
                            break
                            
                        try:
                            response = self._make_request(
                                'POST',
                                f"/api/v3/dataset/{space_name}/{table_name}",
                                headers=headers,
                                data=chunk
                            )
                        except requests.exceptions.RequestException as e:
                            logger.error(f"Failed to upload chunk: {str(e)}")
                            raise
                        
                        # Update progress
                        chunk_len = len(chunk)
                        uploaded_size += chunk_len
                        pbar.update(chunk_len)
                        
                        # Clean up memory after chunk upload
                        del chunk
                        self._cleanup_memory()
            
            ingestion_time = time.time() - start_time
            
            # Record metrics
            self._record_performance_metrics(file_size, ingestion_time)
            
            logger.info(f"Successfully ingested {file_path}")
            logger.info(f"Size: {file_size/1024/1024:.2f}MB")
            logger.info(f"Time: {ingestion_time:.2f}s")
            
            return True
            
        except (ValueError, FileNotFoundError, PermissionError) as e:
            logger.error(f"Validation error: {str(e)}")
            return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error: {str(e)}")
            return False
        except MemoryError as e:
            logger.error(f"Memory error: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error ingesting file: {str(e)}")
            return False

    def _calculate_optimal_chunk_size(self, file_size: int) -> int:
        """Calculate optimal chunk size based on available memory and file size."""
        try:
            available_memory = psutil.virtual_memory().available
            total_memory = psutil.virtual_memory().total
            
            # Calculate base chunk size (1% of total memory or 8MB, whichever is smaller)
            base_chunk_size = min(total_memory // 100, 8 * 1024 * 1024)
            
            # Adjust based on available memory
            if available_memory < total_memory * 0.3:  # Less than 30% memory available
                chunk_size = min(base_chunk_size // 2, file_size)
            else:
                chunk_size = min(base_chunk_size, file_size)
            
            # Ensure minimum chunk size
            chunk_size = max(chunk_size, 1024 * 1024)  # Minimum 1MB
            
            logger.debug(f"Calculated chunk size: {chunk_size/1024/1024:.2f}MB")
            return chunk_size
            
        except Exception as e:
            logger.error(f"Error calculating chunk size: {str(e)}")
            return 1024 * 1024  # Default to 1MB on error

    def _monitor_memory_usage(self) -> None:
        """Monitor memory usage and trigger cleanup if needed."""
        try:
            current_memory = psutil.Process().memory_info().rss
            available_memory = psutil.virtual_memory().available
            total_memory = psutil.virtual_memory().total
            
            # Calculate memory usage percentage
            memory_percent = (current_memory / total_memory) * 100
            
            # Log memory usage if it's high
            if memory_percent > 70:
                logger.warning(f"High memory usage detected: {memory_percent:.1f}% ({current_memory/1024/1024/1024:.2f}GB)")
            
            # If using more than 75% of available memory, trigger cleanup
            if current_memory > available_memory * 0.75:
                logger.warning("Memory usage threshold exceeded, triggering cleanup")
                self._cleanup_memory()
                
            # If still using too much memory after cleanup, raise error
            if current_memory > available_memory * 0.9:
                raise MemoryError(f"Memory usage too high: {memory_percent:.1f}% ({current_memory/1024/1024/1024:.2f}GB)")
                
        except Exception as e:
            logger.error(f"Error monitoring memory usage: {str(e)}")
            raise

def main() -> None:
    """Main function to ingest test data into Dremio with robust error handling."""
    ingester = None
    start_time = time.time()
    
    try:
        # Initialize configuration
        logger.info(f"{Fore.CYAN}Loading configuration...{Style.RESET_ALL}")
        try:
            config = Config()
        except ValueError as e:
            logger.error(f"Configuration error: {str(e)}")
            sys.exit(1)
        
        # Create ingester with progress tracking
        logger.info(f"{Fore.CYAN}Initializing Dremio ingester...{Style.RESET_ALL}")
        try:
            ingester = DremioIngester(config)
        except Exception as e:
            raise ConnectionError(f"Failed to initialize Dremio ingester: {str(e)}")
        
        # Get and validate test data directory
        test_data_dir = Path("test_data")
        if not test_data_dir.exists():
            raise FileNotFoundError(f"Test data directory not found: {test_data_dir}")
        
        # Get list of files to ingest
        files = list(test_data_dir.glob("*.*"))
        if not files:
            raise FileNotFoundError(f"No files found in test data directory: {test_data_dir}")
        
        # Track overall progress
        total_files = len(files)
        successful_ingestions = 0
        failed_ingestions = []
        total_size = sum(f.stat().st_size for f in files)
        
        # Process each file with progress tracking
        logger.info(f"\n{Fore.CYAN}Starting ingestion of {total_files} files (Total size: {total_size/1024/1024:.2f}MB)...{Style.RESET_ALL}")
        
        for index, file_path in enumerate(files, 1):
            file_size = file_path.stat().st_size
            logger.info(f"\n{Fore.CYAN}[{index}/{total_files}] Ingesting {file_path.name} ({file_size/1024/1024:.2f}MB)...{Style.RESET_ALL}")
            
            # Attempt ingestion with retry logic
            max_retries = config.max_retries
            for attempt in range(max_retries):
                try:
                    if ingester.ingest_file(str(file_path), config.dremio_space, file_path.stem):
                        successful_ingestions += 1
                        break
                    elif attempt == max_retries - 1:
                        raise Exception("Maximum retry attempts reached")
                except Exception as e:
                    if attempt == max_retries - 1:
                        error_msg = f"Failed to ingest {file_path.name}: {str(e)}"
                        logger.error(error_msg)
                        failed_ingestions.append((file_path.name, str(e)))
                    else:
                        logger.warning(f"Attempt {attempt + 1} failed, retrying in {config.retry_delay} seconds...")
                        time.sleep(config.retry_delay)
            
            # Clean up memory after each file
            if ingester:
                ingester._cleanup_memory()
        
        # Calculate overall statistics
        total_time = time.time() - start_time
        total_processed = successful_ingestions + len(failed_ingestions)
        success_rate = (successful_ingestions / total_processed * 100) if total_processed > 0 else 0
        avg_speed = total_size / (1024 * 1024 * total_time) if total_time > 0 else 0  # MB/s
        
        # Print final summary
        logger.info(f"\n{Fore.GREEN}Ingestion Summary:{Style.RESET_ALL}")
        logger.info(f"Total time: {total_time:.2f}s")
        logger.info(f"Total files processed: {total_processed}")
        logger.info(f"Successfully ingested: {successful_ingestions}")
        logger.info(f"Failed ingestions: {len(failed_ingestions)}")
        logger.info(f"Success rate: {success_rate:.1f}%")
        logger.info(f"Average ingestion speed: {avg_speed:.2f}MB/s")
        
        if failed_ingestions:
            logger.info("\nFailed ingestions details:")
            for file_name, error in failed_ingestions:
                logger.info(f"- {file_name}: {error}")
        
        # Print performance metrics if available
        if ingester:
            ingester._print_performance_summary()
        
        # Exit with appropriate status code
        if failed_ingestions:
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"Critical error in main function: {str(e)}")
        sys.exit(1)
    finally:
        # Cleanup
        if ingester:
            try:
                ingester._cleanup_memory()
            except Exception as cleanup_error:
                logger.error(f"Error during cleanup: {str(cleanup_error)}")

if __name__ == "__main__":
    main() 