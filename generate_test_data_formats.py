import os
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

    def _generate_chunk(self, start_idx: int, chunk_size: int) -> pd.DataFrame:
        """Generate a chunk of random data."""
        return pd.DataFrame({
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
        })

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
                gc.collect()
                
                # Record memory usage
                memory_usage.append(psutil.Process().memory_info().rss / 1024 / 1024)
                
                pbar.update(1)
        
        end_time = time.time()
        duration = end_time - start_time
        avg_memory = sum(memory_usage) / len(memory_usage)
        
        logger.info(f"CSV generation completed in {duration:.2f} seconds")
        logger.info(f"Average memory usage: {avg_memory:.2f} MB")
        
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
                gc.collect()
                
                # Record memory usage
                memory_usage.append(psutil.Process().memory_info().rss / 1024 / 1024)
                
                pbar.update(1)
        
        end_time = time.time()
        duration = end_time - start_time
        avg_memory = sum(memory_usage) / len(memory_usage)
        
        logger.info(f"TXT generation completed in {duration:.2f} seconds")
        logger.info(f"Average memory usage: {avg_memory:.2f} MB")
        
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
                gc.collect()
                
                # Record memory usage
                memory_usage.append(psutil.Process().memory_info().rss / 1024 / 1024)
                
                pbar.update(1)
        
        end_time = time.time()
        duration = end_time - start_time
        avg_memory = sum(memory_usage) / len(memory_usage)
        
        logger.info(f"Parquet generation completed in {duration:.2f} seconds")
        logger.info(f"Average memory usage: {avg_memory:.2f} MB")
        
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
                gc.collect()
                
                # Record memory usage
                memory_usage.append(psutil.Process().memory_info().rss / 1024 / 1024)
                
                pbar.update(1)
        
        end_time = time.time()
        duration = end_time - start_time
        avg_memory = sum(memory_usage) / len(memory_usage)
        
        logger.info(f"ORC generation completed in {duration:.2f} seconds")
        logger.info(f"Average memory usage: {avg_memory:.2f} MB")
        
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

if __name__ == "__main__":
    main() 