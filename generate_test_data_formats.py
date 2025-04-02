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

class DataGenerator:
    def __init__(self, output_dir, scale_factors=[1, 10, 100]):
        """
        Initialize the data generator.
        
        Args:
            output_dir (str): Base directory for output files
            scale_factors (list): List of scale factors (1GB, 10GB, 100GB)
        """
        self.output_dir = output_dir
        self.scale_factors = scale_factors
        self.chunk_size = 100_000  # Number of rows per chunk
        self.num_columns = 20  # Number of columns in the dataset
        
        # Create output directories
        for sf in scale_factors:
            os.makedirs(os.path.join(output_dir, f'sf{sf}'), exist_ok=True)
        
        # Configure logging
        logger.add(
            os.path.join(output_dir, "data_generation.log"),
            rotation="500 MB",
            level="INFO"
        )

    def generate_chunk(self):
        """Generate a chunk of random data."""
        return pd.DataFrame({
            f'col_{i}': np.random.randn(self.chunk_size) for i in range(self.num_columns)
        })

    def estimate_chunks_needed(self, target_size_gb):
        """Estimate number of chunks needed for target size."""
        # Generate a sample chunk to measure size
        sample_chunk = self.generate_chunk()
        sample_size_bytes = len(sample_chunk.to_csv(index=False).encode('utf-8'))
        chunks_needed = math.ceil((target_size_gb * 1024 * 1024 * 1024) / sample_size_bytes)
        return chunks_needed

    def generate_csv(self, scale_factor):
        """Generate CSV file of specified size."""
        target_size_gb = scale_factor
        chunks_needed = self.estimate_chunks_needed(target_size_gb)
        output_file = os.path.join(self.output_dir, f'sf{scale_factor}', f'data_{scale_factor}gb.csv')
        
        logger.info(f"Generating {target_size_gb}GB CSV file...")
        start_time = time.time()
        
        # Write header
        self.generate_chunk().to_csv(output_file, index=False)
        
        # Append chunks
        with open(output_file, 'a') as f:
            for _ in tqdm(range(chunks_needed - 1), desc=f"Generating {target_size_gb}GB CSV"):
                chunk = self.generate_chunk()
                chunk.to_csv(f, header=False, index=False)
        
        end_time = time.time()
        logger.info(f"CSV generation completed in {end_time - start_time:.2f} seconds")
        return output_file

    def generate_txt(self, scale_factor):
        """Generate TXT file of specified size."""
        target_size_gb = scale_factor
        chunks_needed = self.estimate_chunks_needed(target_size_gb)
        output_file = os.path.join(self.output_dir, f'sf{scale_factor}', f'data_{scale_factor}gb.txt')
        
        logger.info(f"Generating {target_size_gb}GB TXT file...")
        start_time = time.time()
        
        # Write header
        self.generate_chunk().to_string(output_file, index=False)
        
        # Append chunks
        with open(output_file, 'a') as f:
            for _ in tqdm(range(chunks_needed - 1), desc=f"Generating {target_size_gb}GB TXT"):
                chunk = self.generate_chunk()
                chunk.to_string(f, header=False, index=False)
        
        end_time = time.time()
        logger.info(f"TXT generation completed in {end_time - start_time:.2f} seconds")
        return output_file

    def generate_parquet(self, scale_factor):
        """Generate Parquet file of specified size."""
        target_size_gb = scale_factor
        chunks_needed = self.estimate_chunks_needed(target_size_gb)
        output_file = os.path.join(self.output_dir, f'sf{scale_factor}', f'data_{scale_factor}gb.parquet')
        
        logger.info(f"Generating {target_size_gb}GB Parquet file...")
        start_time = time.time()
        
        # Generate and write chunks
        for i in tqdm(range(chunks_needed), desc=f"Generating {target_size_gb}GB Parquet"):
            chunk = self.generate_chunk()
            if i == 0:
                chunk.to_parquet(output_file, index=False)
            else:
                chunk.to_parquet(output_file, append=True, index=False)
        
        end_time = time.time()
        logger.info(f"Parquet generation completed in {end_time - start_time:.2f} seconds")
        return output_file

    def generate_orc(self, scale_factor):
        """Generate ORC file of specified size."""
        target_size_gb = scale_factor
        chunks_needed = self.estimate_chunks_needed(target_size_gb)
        output_file = os.path.join(self.output_dir, f'sf{scale_factor}', f'data_{scale_factor}gb.orc')
        
        logger.info(f"Generating {target_size_gb}GB ORC file...")
        start_time = time.time()
        
        # Generate and write chunks
        for i in tqdm(range(chunks_needed), desc=f"Generating {target_size_gb}GB ORC"):
            chunk = self.generate_chunk()
            if i == 0:
                table = pa.Table.from_pandas(chunk)
                orc.write_table(table, output_file)
            else:
                table = pa.Table.from_pandas(chunk)
                orc.write_table(table, output_file, append=True)
        
        end_time = time.time()
        logger.info(f"ORC generation completed in {end_time - start_time:.2f} seconds")
        return output_file

    def generate_all_formats(self):
        """Generate all formats for all scale factors."""
        formats = {
            'csv': self.generate_csv,
            'txt': self.generate_txt,
            'parquet': self.generate_parquet,
            'orc': self.generate_orc
        }
        
        results = {}
        for sf in self.scale_factors:
            results[f'sf{sf}'] = {}
            for fmt, generator in formats.items():
                try:
                    output_file = generator(sf)
                    results[f'sf{sf}'][fmt] = output_file
                    logger.info(f"Successfully generated {fmt.upper()} file for scale factor {sf}GB")
                except Exception as e:
                    logger.error(f"Error generating {fmt.upper()} file for scale factor {sf}GB: {str(e)}")
                    results[f'sf{sf}'][fmt] = None
        
        return results

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
    results = generator.generate_all_formats()
    
    # Print summary
    logger.info("\nData Generation Summary:")
    for sf, formats in results.items():
        logger.info(f"\nScale Factor: {sf}")
        for fmt, file_path in formats.items():
            if file_path:
                size_gb = os.path.getsize(file_path) / (1024**3)
                logger.info(f"{fmt.upper()}: {file_path} ({size_gb:.2f}GB)")
            else:
                logger.warning(f"{fmt.upper()}: Failed to generate")

if __name__ == "__main__":
    main() 