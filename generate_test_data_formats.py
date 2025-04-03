import os
import sys
import gc
import time
import psutil
import numpy as np
import pandas as pd
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
    "data_generation.log",
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

class DataGenerator:
    """A class for generating test data in various formats with memory optimization."""
    
    def __init__(self, output_dir: str = "test_data", chunk_size: int = 100000, source_dremio_url: str = None):
        """
        Initialize the DataGenerator.
        
        Args:
            output_dir: Directory to save generated files
            chunk_size: Number of rows per chunk for memory optimization
            source_dremio_url: URL of the source Dremio instance for data sharing
        """
        self.output_dir = Path(output_dir)
        self.chunk_size = chunk_size
        self.source_dremio_url = source_dremio_url
        self.available_memory = None
        self.min_required_memory = 2 * 1024 * 1024 * 1024  # 2GB minimum
        self.performance_metrics = {
            'file_sizes': [],
            'generation_times': [],
            'memory_usage': []
        }
        
        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Check system resources
        self._check_system_resources()
        
        # Initialize data types for 64-bit optimization
        self.dtypes = {
            'id': np.int64,
            'name': str,
            'age': np.int32,
            'salary': np.float64,
            'department': str,
            'hire_date': str,
            'is_active': bool,
            'performance_score': np.float32,
            'years_of_service': np.int32,
            'bonus': np.float64
        }

        # Log source Dremio information
        if self.source_dremio_url:
            logger.info(f"Source Dremio instance: {self.source_dremio_url}")
            logger.info("Data will be generated for cross-cluster sharing")

    def _check_system_resources(self):
        """Check system resources and set optimal parameters for 64-bit system."""
        try:
            # Check available memory (64-bit system)
            self.available_memory = psutil.virtual_memory().available
            if self.available_memory < self.min_required_memory:
                raise RuntimeError(f"Insufficient memory. Available: {self.available_memory/1024/1024/1024:.2f}GB, Required: 2GB")
            
            # Check disk space
            disk = psutil.disk_usage(self.output_dir)
            if disk.free < 10 * 1024 * 1024 * 1024:  # 10GB minimum
                raise RuntimeError(f"Insufficient disk space. Available: {disk.free/1024/1024/1024:.2f}GB, Required: 10GB")
            
            # Adjust chunk size based on available memory (optimized for 64-bit)
            max_chunk_size = int(self.available_memory * 0.2 / 1024 / 1024)  # Use 20% of available memory
            self.chunk_size = min(self.chunk_size, max_chunk_size)
            
            # Log system information
            logger.info("System Information:")
            logger.info(f"- Available Memory: {self.available_memory/1024/1024/1024:.2f}GB")
            logger.info(f"- Available Disk Space: {disk.free/1024/1024/1024:.2f}GB")
            logger.info(f"- Using chunk size: {self.chunk_size}")
            
        except Exception as e:
            logger.error(f"System resource check failed: {str(e)}")
            raise

    def _optimize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimize DataFrame memory usage for 64-bit system."""
        try:
            # Optimize numeric columns with 64-bit considerations
            for col in df.select_dtypes(include=['int64']).columns:
                if df[col].min() >= np.iinfo(np.int32).min and df[col].max() <= np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif df[col].min() >= np.iinfo(np.int16).min and df[col].max() <= np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif df[col].min() >= np.iinfo(np.int8).min and df[col].max() <= np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
            
            # Keep float64 for precision in 64-bit system
            for col in df.select_dtypes(include=['float64']).columns:
                if df[col].dtype == np.float64:
                    # Only convert to float32 if precision loss is acceptable
                    if df[col].nunique() / len(df) < 0.1:  # If less than 10% unique values
                        df[col] = df[col].astype(np.float32)
            
            # Optimize string columns
            for col in df.select_dtypes(include=['object']).columns:
                if df[col].nunique() / len(df) < 0.5:  # If less than 50% unique values
                    df[col] = df[col].astype('category')
            
            return df
            
        except Exception as e:
            logger.error(f"Error optimizing DataFrame: {str(e)}")
            return df

    def _cleanup_memory(self):
        """Clean up memory and force garbage collection for 64-bit system."""
        gc.collect()
        current_memory = psutil.Process().memory_info().rss
        if current_memory > self.available_memory * 0.8:  # If using more than 80% of available memory
            logger.warning(f"High memory usage detected: {current_memory/1024/1024/1024:.2f}GB")
            gc.collect(2)  # Force garbage collection with generation 2 objects
            gc.collect(1)  # Also collect generation 1 objects
            gc.collect(0)  # And generation 0 objects

    def _generate_chunk(self, start_idx: int, size: int) -> pd.DataFrame:
        """Generate a chunk of data with optimized memory usage."""
        try:
            # Pre-allocate arrays for better memory efficiency
            data = {
                'id': np.arange(start_idx, start_idx + size, dtype=np.int64),
                'name': np.array([f"Employee_{i}" for i in range(start_idx, start_idx + size)]),
                'age': np.random.randint(22, 65, size=size, dtype=np.int32),
                'salary': np.random.uniform(30000, 150000, size=size).astype(np.float64),
                'department': np.random.choice(['IT', 'HR', 'Finance', 'Marketing', 'Sales'], size=size),
                'hire_date': pd.date_range(start='2020-01-01', periods=size, freq='D').strftime('%Y-%m-%d'),
                'is_active': np.random.choice([True, False], size=size),
                'performance_score': np.random.uniform(0, 1, size=size).astype(np.float32),
                'years_of_service': np.random.randint(0, 20, size=size, dtype=np.int32),
                'bonus': np.random.uniform(0, 50000, size=size).astype(np.float64)
            }
            
            # Create DataFrame with optimized memory usage
            df = pd.DataFrame(data)
            return self._optimize_dataframe(df)
            
        except MemoryError:
            logger.error(f"Memory error while generating chunk starting at index {start_idx}")
            raise
        except Exception as e:
            logger.error(f"Error generating chunk: {str(e)}")
            raise

    def _record_performance_metrics(self, file_size: int, generation_time: float):
        """Record performance metrics for analysis."""
        self.performance_metrics['file_sizes'].append(file_size)
        self.performance_metrics['generation_times'].append(generation_time)
        self.performance_metrics['memory_usage'].append(psutil.Process().memory_info().rss)

    def _print_performance_summary(self):
        """Print a summary of performance metrics."""
        if not self.performance_metrics['file_sizes']:
            logger.warning("No performance metrics recorded")
            return

        summary = {
            'Average File Size (MB)': np.mean(self.performance_metrics['file_sizes']) / (1024 * 1024),
            'Average Generation Time (s)': np.mean(self.performance_metrics['generation_times']),
            'Average Memory Usage (MB)': np.mean(self.performance_metrics['memory_usage']) / (1024 * 1024),
            'Total Files Generated': len(self.performance_metrics['file_sizes'])
        }

        logger.info("\nPerformance Summary:")
        logger.info(tabulate(summary.items(), headers=['Metric', 'Value'], tablefmt='grid'))

    def generate_csv(self, num_rows: int = 1000000) -> str:
        """Generate a CSV file with the specified number of rows."""
        try:
            output_file = self.output_dir / "test_data.csv"
            start_time = time.time()
            
            # Calculate number of chunks
            num_chunks = (num_rows + self.chunk_size - 1) // self.chunk_size
            
            with tqdm(total=num_rows, desc="Generating CSV", unit="rows") as pbar:
                for i in range(num_chunks):
                    start_idx = i * self.chunk_size
                    chunk_size = min(self.chunk_size, num_rows - start_idx)
                    
                    # Generate and write chunk
                    df = self._generate_chunk(start_idx, chunk_size)
                    df.to_csv(output_file, mode='a', header=(i == 0), index=False)
                    
                    # Update progress
                    pbar.update(chunk_size)
                    
                    # Clean up memory
                    self._cleanup_memory()
            
            generation_time = time.time() - start_time
            file_size = output_file.stat().st_size
            
            # Record metrics
            self._record_performance_metrics(file_size, generation_time)
            
            logger.info(f"CSV file generated: {output_file}")
            logger.info(f"Size: {file_size/1024/1024:.2f}MB")
            logger.info(f"Time: {generation_time:.2f}s")
            
            return str(output_file)
            
        except Exception as e:
            logger.error(f"Error generating CSV file: {str(e)}")
            raise

    def generate_txt(self, num_rows: int = 1000000) -> str:
        """Generate a tab-separated text file with the specified number of rows."""
        try:
            output_file = self.output_dir / "test_data.txt"
            start_time = time.time()
            
            # Calculate number of chunks
            num_chunks = (num_rows + self.chunk_size - 1) // self.chunk_size
            
            with tqdm(total=num_rows, desc="Generating TXT", unit="rows") as pbar:
                for i in range(num_chunks):
                    start_idx = i * self.chunk_size
                    chunk_size = min(self.chunk_size, num_rows - start_idx)
                    
                    # Generate and write chunk
                    df = self._generate_chunk(start_idx, chunk_size)
                    df.to_csv(output_file, mode='a', header=(i == 0), index=False, sep='\t')
                    
                    # Update progress
                    pbar.update(chunk_size)
                    
                    # Clean up memory
                    self._cleanup_memory()
            
            generation_time = time.time() - start_time
            file_size = output_file.stat().st_size
            
            # Record metrics
            self._record_performance_metrics(file_size, generation_time)
            
            logger.info(f"TXT file generated: {output_file}")
            logger.info(f"Size: {file_size/1024/1024:.2f}MB")
            logger.info(f"Time: {generation_time:.2f}s")
            
            return str(output_file)
            
        except Exception as e:
            logger.error(f"Error generating TXT file: {str(e)}")
            raise

    def generate_parquet(self, num_rows: int = 1000000) -> str:
        """Generate a Parquet file with the specified number of rows."""
        try:
            output_file = self.output_dir / "test_data.parquet"
            start_time = time.time()
            
            # Calculate number of chunks
            num_chunks = (num_rows + self.chunk_size - 1) // self.chunk_size
            
            # Create empty table with schema
            schema = pa.Schema.from_pandas(self._generate_chunk(0, 1))
            with pq.ParquetWriter(output_file, schema) as writer:
                with tqdm(total=num_rows, desc="Generating Parquet", unit="rows") as pbar:
                    for i in range(num_chunks):
                        start_idx = i * self.chunk_size
                        chunk_size = min(self.chunk_size, num_rows - start_idx)
                        
                        # Generate and write chunk
                        df = self._generate_chunk(start_idx, chunk_size)
                        table = pa.Table.from_pandas(df)
                        writer.write_table(table)
                        
                        # Update progress
                        pbar.update(chunk_size)
                        
                        # Clean up memory
                        self._cleanup_memory()
            
            generation_time = time.time() - start_time
            file_size = output_file.stat().st_size
            
            # Record metrics
            self._record_performance_metrics(file_size, generation_time)
            
            logger.info(f"Parquet file generated: {output_file}")
            logger.info(f"Size: {file_size/1024/1024:.2f}MB")
            logger.info(f"Time: {generation_time:.2f}s")
            
            return str(output_file)
            
        except Exception as e:
            logger.error(f"Error generating Parquet file: {str(e)}")
            raise

    def generate_orc(self, num_rows: int = 1000000) -> str:
        """Generate an ORC file with the specified number of rows."""
        try:
            output_file = self.output_dir / "test_data.orc"
            start_time = time.time()
            
            # Calculate number of chunks
            num_chunks = (num_rows + self.chunk_size - 1) // self.chunk_size
            
            # Create schema for pyorc 0.10.0
            schema = pyorc.TypeDescription.from_string(
                "struct<id:bigint,name:string,age:int,salary:double,"
                "department:string,hire_date:string,is_active:boolean,"
                "performance_score:float,years_of_service:int,bonus:double>"
            )
            
            with pyorc.Writer(str(output_file), schema) as writer:
                with tqdm(total=num_rows, desc="Generating ORC", unit="rows") as pbar:
                    for i in range(num_chunks):
                        start_idx = i * self.chunk_size
                        chunk_size = min(self.chunk_size, num_rows - start_idx)
                        
                        # Generate and write chunk
                        df = self._generate_chunk(start_idx, chunk_size)
                        
                        # Convert DataFrame to list of tuples for pyorc 0.10.0
                        data = [
                            (
                                row['id'],
                                row['name'],
                                row['age'],
                                float(row['salary']),
                                row['department'],
                                row['hire_date'],
                                bool(row['is_active']),
                                float(row['performance_score']),
                                row['years_of_service'],
                                float(row['bonus'])
                            )
                            for _, row in df.iterrows()
                        ]
                        
                        # Write data in batches
                        writer.writerows(data)
                        
                        # Update progress
                        pbar.update(chunk_size)
                        
                        # Clean up memory
                        self._cleanup_memory()
            
            generation_time = time.time() - start_time
            file_size = output_file.stat().st_size
            
            # Record metrics
            self._record_performance_metrics(file_size, generation_time)
            
            logger.info(f"ORC file generated: {output_file}")
            logger.info(f"Size: {file_size/1024/1024:.2f}MB")
            logger.info(f"Time: {generation_time:.2f}s")
            
            return str(output_file)
            
        except Exception as e:
            logger.error(f"Error generating ORC file: {str(e)}")
            raise

def main():
    """Main function to generate test data in all formats."""
    try:
        # Load environment variables
        load_dotenv()
        
        # Get source Dremio URL
        source_dremio_url = os.getenv('SOURCE_DREMIO_URL')
        
        # Create data generator
        generator = DataGenerator(source_dremio_url=source_dremio_url)
        
        # Generate data in all formats
        formats = {
            'CSV': generator.generate_csv,
            'TXT': generator.generate_txt,
            'Parquet': generator.generate_parquet,
            'ORC': generator.generate_orc
        }
        
        for format_name, generate_func in formats.items():
            logger.info(f"\n{Fore.CYAN}Generating {format_name} file for cross-cluster sharing...{Style.RESET_ALL}")
            try:
                generate_func()
            except Exception as e:
                logger.error(f"Failed to generate {format_name} file: {str(e)}")
                continue
        
        # Print performance summary
        generator._print_performance_summary()
        
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 