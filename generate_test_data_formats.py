import os
import sys
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.orc as orc
import pyarrow.parquet as pq
from tqdm import tqdm
import psutil
import time
from loguru import logger
import math
import pyorc
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import gc
import pkg_resources

def check_version_compatibility():
    """Check if installed package versions are compatible."""
    required_versions = {
        'pandas': '1.5.3',
        'numpy': '1.23.5',
        'pyarrow': '12.0.1',
        'pyorc': '1.7.0'
    }
    
    incompatible_packages = []
    for package, required_version in required_versions.items():
        try:
            installed_version = pkg_resources.get_distribution(package).version
            if installed_version != required_version:
                incompatible_packages.append(f"{package} (required: {required_version}, installed: {installed_version})")
        except pkg_resources.DistributionNotFound:
            incompatible_packages.append(f"{package} (not installed)")
    
    if incompatible_packages:
        logger.error("Incompatible package versions detected:")
        for package in incompatible_packages:
            logger.error(f"- {package}")
        logger.error("\nPlease install the correct versions using:")
        logger.error("pip install pandas==1.5.3 numpy==1.23.5 pyarrow==12.0.1 pyorc==1.7.0")
        sys.exit(1)

# Check version compatibility before proceeding
check_version_compatibility()

class DataGenerator:
    def __init__(self, output_dir: str = "test_data"):
        """Initialize the data generator with output directory."""
        self.output_dir = output_dir
        self.chunk_size = 100_000  # Reduced chunk size for better memory management
        self.columns = {
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
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Configure logging
        logger.add(
            "data_generation.log",
            rotation="1 day",
            retention="7 days",
            level="INFO",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
        )
        
        # System resource checks
        self._check_system_resources()
        
        # Initialize performance metrics
        self.performance_metrics = {
            'memory_usage': [],
            'generation_times': [],
            'file_sizes': []
        }

    def _check_system_resources(self):
        """Check system resources and set optimal parameters."""
        try:
            # Check available memory
            self.available_memory = psutil.virtual_memory().available
            self.min_required_memory = 2 * 1024 * 1024 * 1024  # 2GB minimum
            
            if self.available_memory < self.min_required_memory:
                raise RuntimeError(f"Insufficient memory. Available: {self.available_memory/1024/1024/1024:.2f}GB, Required: 2GB")
            
            # Check disk space
            disk = psutil.disk_usage(self.output_dir)
            if disk.free < 10 * 1024 * 1024 * 1024:  # 10GB minimum
                raise RuntimeError(f"Insufficient disk space. Available: {disk.free/1024/1024/1024:.2f}GB, Required: 10GB")
            
            # Adjust chunk size based on available memory
            max_chunk_size = int(self.available_memory * 0.1 / 1024 / 1024)  # Use 10% of available memory
            self.chunk_size = min(self.chunk_size, max_chunk_size)
            
            logger.info(f"System resources checked. Using chunk size: {self.chunk_size}")
            
        except Exception as e:
            logger.error(f"System resource check failed: {str(e)}")
            raise

    def _optimize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimize DataFrame memory usage."""
        try:
            # Optimize numeric columns
            for col in df.select_dtypes(include=['int64']).columns:
                if df[col].min() >= np.iinfo(np.int32).min and df[col].max() <= np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif df[col].min() >= np.iinfo(np.int16).min and df[col].max() <= np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif df[col].min() >= np.iinfo(np.int8).min and df[col].max() <= np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
            
            for col in df.select_dtypes(include=['float64']).columns:
                if df[col].dtype == np.float64:
                    df[col] = df[col].astype(np.float32)
            
            # Optimize string columns
            for col in df.select_dtypes(include=['object']).columns:
                if df[col].nunique() / len(df) < 0.5:  # If less than 50% unique values
                    df[col] = df[col].astype('category')
            
            return df
            
        except Exception as e:
            logger.error(f"Error optimizing DataFrame: {str(e)}")
            return df

    def _generate_chunk(self, start_idx: int, chunk_size: int) -> pd.DataFrame:
        """Generate a chunk of random data with memory optimization."""
        try:
            # Pre-allocate arrays for better memory efficiency
            data = {
                'id': np.arange(start_idx, start_idx + chunk_size, dtype=np.int64),
                'name': [f'Employee_{i}' for i in range(start_idx, start_idx + chunk_size)],
                'age': np.random.randint(22, 65, chunk_size, dtype=np.int32),
                'salary': np.random.uniform(30000, 150000, chunk_size).astype(np.float64),
                'department': np.random.choice(['IT', 'HR', 'Finance', 'Marketing', 'Sales'], chunk_size),
                'hire_date': pd.date_range(start='2020-01-01', periods=chunk_size, freq='D').strftime('%Y-%m-%d'),
                'is_active': np.random.choice([True, False], chunk_size),
                'performance_score': np.random.uniform(0, 100, chunk_size).astype(np.float32),
                'years_of_service': np.random.randint(0, 20, chunk_size, dtype=np.int32),
                'bonus': np.random.uniform(0, 20000, chunk_size).astype(np.float64)
            }
            
            # Create DataFrame with optimized memory usage
            df = pd.DataFrame(data, columns=self.columns.keys())
            df = self._optimize_dataframe(df)
            
            # Verify memory usage
            chunk_memory = df.memory_usage(deep=True).sum()
            if chunk_memory > self.available_memory * 0.5:  # If chunk uses more than 50% of available memory
                raise MemoryError(f"Chunk size too large. Memory usage: {chunk_memory/1024/1024:.2f}MB")
            
            return df
            
        except Exception as e:
            logger.error(f"Error generating chunk: {str(e)}")
            raise

    def _cleanup_memory(self):
        """Clean up memory and force garbage collection."""
        gc.collect()
        current_memory = psutil.Process().memory_info().rss
        if current_memory > self.available_memory * 0.8:  # If using more than 80% of available memory
            logger.warning("High memory usage detected, forcing cleanup")
            gc.collect(2)  # Force garbage collection with generation 2 objects

    def _record_performance_metrics(self, file_size: int, generation_time: float):
        """Record performance metrics."""
        self.performance_metrics['file_sizes'].append(file_size)
        self.performance_metrics['generation_times'].append(generation_time)
        self.performance_metrics['memory_usage'].append(psutil.Process().memory_info().rss / 1024 / 1024)

    def _print_performance_summary(self):
        """Print performance summary."""
        if self.performance_metrics['file_sizes']:
            avg_file_size = sum(self.performance_metrics['file_sizes']) / len(self.performance_metrics['file_sizes'])
            avg_generation_time = sum(self.performance_metrics['generation_times']) / len(self.performance_metrics['generation_times'])
            avg_memory_usage = sum(self.performance_metrics['memory_usage']) / len(self.performance_metrics['memory_usage'])
            
            logger.info("\nPerformance Summary:")
            logger.info(f"Average file size: {avg_file_size/1024/1024/1024:.2f}GB")
            logger.info(f"Average generation time: {avg_generation_time:.2f} seconds")
            logger.info(f"Average memory usage: {avg_memory_usage:.2f}MB")

    def _estimate_chunks(self, target_size_gb: float) -> int:
        """Estimate number of chunks needed based on target size."""
        # Sample size for estimation
        sample_size = 1000
        df_sample = pd.DataFrame({
            'id': np.arange(sample_size, dtype=np.int64),
            'name': [f'Employee_{i}' for i in range(sample_size)],
            'age': np.random.randint(22, 65, sample_size, dtype=np.int32),
            'salary': np.random.uniform(30000, 150000, sample_size).astype(np.float64),
            'department': np.random.choice(['IT', 'HR', 'Finance', 'Marketing', 'Sales'], sample_size),
            'hire_date': pd.date_range(start='2020-01-01', periods=sample_size, freq='D').strftime('%Y-%m-%d'),
            'is_active': np.random.choice([True, False], sample_size),
            'performance_score': np.random.uniform(0, 100, sample_size).astype(np.float32),
            'years_of_service': np.random.randint(0, 20, sample_size, dtype=np.int32),
            'bonus': np.random.uniform(0, 20000, sample_size).astype(np.float64)
        })
        
        # Calculate size per row
        sample_size_bytes = df_sample.memory_usage(deep=True).sum()
        bytes_per_row = sample_size_bytes / sample_size
        
        # Calculate target size in bytes
        target_size_bytes = target_size_gb * 1024 * 1024 * 1024
        
        # Estimate number of chunks needed
        rows_needed = target_size_bytes / bytes_per_row
        chunks_needed = int(np.ceil(rows_needed / self.chunk_size))
        
        return max(1, chunks_needed)

    def generate_csv(self, target_size_gb: float, scale_factor: str) -> str:
        """Generate CSV file of specified size."""
        output_file = os.path.join(self.output_dir, f"sf{scale_factor}", f"data_{target_size_gb}gb.csv")
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        chunks = self._estimate_chunks(target_size_gb)
        logger.info(f"Generating {target_size_gb}GB CSV file with {chunks} chunks")
        
        start_time = time.time()
        memory_usage = []
        
        with tqdm(total=chunks, desc=f"Generating {target_size_gb}GB CSV") as pbar:
            for i in range(chunks):
                chunk_start = i * self.chunk_size
                df_chunk = self._generate_chunk(chunk_start, self.chunk_size)
                
                # Write chunk to CSV
                mode = 'w' if i == 0 else 'a'
                header = i == 0
                df_chunk.to_csv(output_file, mode=mode, header=header, index=False)
                
                # Memory management
                del df_chunk
                self._cleanup_memory()
                
                # Record memory usage
                memory_usage.append(psutil.Process().memory_info().rss / 1024 / 1024)
                
                pbar.update(1)
        
        end_time = time.time()
        duration = end_time - start_time
        avg_memory = sum(memory_usage) / len(memory_usage)
        
        logger.info(f"CSV generation completed in {duration:.2f} seconds")
        logger.info(f"Average memory usage: {avg_memory:.2f} MB")
        
        self._record_performance_metrics(os.path.getsize(output_file), duration)
        
        return output_file

    def generate_txt(self, target_size_gb: float, scale_factor: str) -> str:
        """Generate TXT file of specified size."""
        output_file = os.path.join(self.output_dir, f"sf{scale_factor}", f"data_{target_size_gb}gb.txt")
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        chunks = self._estimate_chunks(target_size_gb)
        logger.info(f"Generating {target_size_gb}GB TXT file with {chunks} chunks")
        
        start_time = time.time()
        memory_usage = []
        
        with tqdm(total=chunks, desc=f"Generating {target_size_gb}GB TXT") as pbar:
            for i in range(chunks):
                chunk_start = i * self.chunk_size
                df_chunk = self._generate_chunk(chunk_start, self.chunk_size)
                
                # Write chunk to TXT
                mode = 'w' if i == 0 else 'a'
                header = i == 0
                df_chunk.to_csv(output_file, mode=mode, header=header, index=False, sep='\t')
                
                # Memory management
                del df_chunk
                self._cleanup_memory()
                
                # Record memory usage
                memory_usage.append(psutil.Process().memory_info().rss / 1024 / 1024)
                
                pbar.update(1)
        
        end_time = time.time()
        duration = end_time - start_time
        avg_memory = sum(memory_usage) / len(memory_usage)
        
        logger.info(f"TXT generation completed in {duration:.2f} seconds")
        logger.info(f"Average memory usage: {avg_memory:.2f} MB")
        
        self._record_performance_metrics(os.path.getsize(output_file), duration)
        
        return output_file

    def generate_parquet(self, target_size_gb: float, scale_factor: str) -> str:
        """Generate Parquet file of specified size."""
        output_file = os.path.join(self.output_dir, f"sf{scale_factor}", f"data_{target_size_gb}gb.parquet")
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        chunks = self._estimate_chunks(target_size_gb)
        logger.info(f"Generating {target_size_gb}GB Parquet file with {chunks} chunks")
        
        start_time = time.time()
        memory_usage = []
        
        # Create schema
        schema = pa.Schema.from_pandas(self._generate_chunk(0, 1))
        
        with tqdm(total=chunks, desc=f"Generating {target_size_gb}GB Parquet") as pbar:
            for i in range(chunks):
                chunk_start = i * self.chunk_size
                df_chunk = self._generate_chunk(chunk_start, self.chunk_size)
                
                # Convert to Arrow table
                table = pa.Table.from_pandas(df_chunk, schema=schema)
                
                # Write chunk to Parquet
                pq.write_table(table, output_file, append=i > 0)
                
                # Memory management
                del df_chunk, table
                self._cleanup_memory()
                
                # Record memory usage
                memory_usage.append(psutil.Process().memory_info().rss / 1024 / 1024)
                
                pbar.update(1)
        
        end_time = time.time()
        duration = end_time - start_time
        avg_memory = sum(memory_usage) / len(memory_usage)
        
        logger.info(f"Parquet generation completed in {duration:.2f} seconds")
        logger.info(f"Average memory usage: {avg_memory:.2f} MB")
        
        self._record_performance_metrics(os.path.getsize(output_file), duration)
        
        return output_file

    def generate_orc(self, target_size_gb: float, scale_factor: str) -> str:
        """Generate ORC file of specified size."""
        output_file = os.path.join(self.output_dir, f"sf{scale_factor}", f"data_{target_size_gb}gb.orc")
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        chunks = self._estimate_chunks(target_size_gb)
        logger.info(f"Generating {target_size_gb}GB ORC file with {chunks} chunks")
        
        start_time = time.time()
        memory_usage = []
        
        # Define ORC schema
        schema = "struct<id:bigint,name:string,age:int,salary:double,department:string,hire_date:string,is_active:boolean,performance_score:float,years_of_service:int,bonus:double>"
        
        with tqdm(total=chunks, desc=f"Generating {target_size_gb}GB ORC") as pbar:
            for i in range(chunks):
                chunk_start = i * self.chunk_size
                df_chunk = self._generate_chunk(chunk_start, self.chunk_size)
                
                # Convert DataFrame to list of dictionaries for ORC
                records = df_chunk.to_dict('records')
                
                # Write chunk to ORC
                with pyorc.Writer(output_file, schema, stripe_size=64*1024*1024) as writer:
                    writer.writerows(records)
                
                # Memory management
                del df_chunk, records
                self._cleanup_memory()
                
                # Record memory usage
                memory_usage.append(psutil.Process().memory_info().rss / 1024 / 1024)
                
                pbar.update(1)
        
        end_time = time.time()
        duration = end_time - start_time
        avg_memory = sum(memory_usage) / len(memory_usage)
        
        logger.info(f"ORC generation completed in {duration:.2f} seconds")
        logger.info(f"Average memory usage: {avg_memory:.2f} MB")
        
        self._record_performance_metrics(os.path.getsize(output_file), duration)
        
        return output_file

    def generate_all_formats(self, scale_factors: List[str] = ["1", "10", "100"]):
        """Generate all formats for specified scale factors."""
        for sf in scale_factors:
            target_size = float(sf)
            logger.info(f"Generating data for scale factor {sf}GB")
            
            # Generate each format
            self.generate_csv(target_size, sf)
            self.generate_txt(target_size, sf)
            self.generate_parquet(target_size, sf)
            self.generate_orc(target_size, sf)
            
            logger.info(f"Completed generation for scale factor {sf}GB")

    def _print_performance_summary(self):
        """Print performance summary."""
        if self.performance_metrics['file_sizes']:
            avg_file_size = sum(self.performance_metrics['file_sizes']) / len(self.performance_metrics['file_sizes'])
            avg_generation_time = sum(self.performance_metrics['generation_times']) / len(self.performance_metrics['generation_times'])
            avg_memory_usage = sum(self.performance_metrics['memory_usage']) / len(self.performance_metrics['memory_usage'])
            
            logger.info("\nPerformance Summary:")
            logger.info(f"Average file size: {avg_file_size/1024/1024/1024:.2f}GB")
            logger.info(f"Average generation time: {avg_generation_time:.2f} seconds")
            logger.info(f"Average memory usage: {avg_memory_usage:.2f}MB")

def main():
    # Get available memory
    available_memory = psutil.virtual_memory().available / (1024**3)  # Convert to GB
    
    # Set output directory
    output_dir = "test_data"
    os.makedirs(output_dir, exist_ok=True)
    
    # Initialize generator
    generator = DataGenerator(output_dir)
    
    # Generate data
    logger.info("Starting data generation...")
    generator.generate_all_formats()
    
    # Print summary
    logger.info("\nData Generation Summary:")
    for sf, formats in generator.generate_all_formats().items():
        logger.info(f"\nScale Factor: {sf}")
        for fmt, file_path in formats.items():
            if file_path:
                size_gb = os.path.getsize(file_path) / (1024**3)
                logger.info(f"{fmt.upper()}: {file_path} ({size_gb:.2f}GB)")
            else:
                logger.warning(f"{fmt.upper()}: Failed to generate")

    # Print performance summary
    generator._print_performance_summary()

if __name__ == "__main__":
    main() 