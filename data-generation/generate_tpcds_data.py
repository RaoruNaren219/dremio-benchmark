#!/usr/bin/env python3

"""
TPC-DS Data Generation Script
This script generates TPC-DS data using platform-specific strategies
"""

import os
import logging
import sys
import platform
import subprocess
import ctypes
from pathlib import Path
from abc import ABC, abstractmethod
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_generation.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class DataGenerationStrategy(ABC):
    """Abstract base class for platform-specific data generation strategies"""
    
    def __init__(self, dsdgen_path: str):
        """
        Initialize the strategy with dsdgen path
        
        Args:
            dsdgen_path: Path to dsdgen binary
        """
        self.dsdgen_path = dsdgen_path
        self.dsdgen_dir = os.path.dirname(dsdgen_path)
    
    @abstractmethod
    def generate_data(self, output_dir: str, scale_factor: int) -> bool:
        """
        Generate TPC-DS data using platform-specific approach
        
        Args:
            output_dir: Directory to output generated data
            scale_factor: Scale factor in GB
            
        Returns:
            bool: Success status
        """
        pass

class WindowsStrategy(DataGenerationStrategy):
    """Windows-specific data generation strategy"""
    
    def generate_data(self, output_dir: str, scale_factor: int) -> bool:
        try:
            # Convert paths to Windows format
            dsdgen_exe = os.path.join(self.dsdgen_dir, "dsdgen.exe")
            output_dir_win = output_dir.replace('/', '\\')
            
            # Use ctypes to call the executable
            result = ctypes.windll.shell32.ShellExecuteW(
                None,  # hwnd
                "open",  # operation
                dsdgen_exe,  # file
                f'-SCALE {scale_factor} -DIR "{output_dir_win}" -FORCE',  # parameters
                self.dsdgen_dir,  # directory
                1  # show command (1 = normal)
            )
            
            # ShellExecute returns a value > 32 if successful
            success = (result > 32)
            
            if success:
                logger.info(f"Successfully generated {scale_factor}GB data")
            else:
                logger.error(f"Failed to generate {scale_factor}GB data, return code: {result}")
                
            return success
        except Exception as e:
            logger.error(f"Error using Windows native execution: {e}")
            return self._fallback_generation(output_dir, scale_factor)
    
    def _fallback_generation(self, output_dir: str, scale_factor: int) -> bool:
        """Fallback to subprocess-based generation if native approach fails"""
        try:
            cmd = [
                os.path.join(self.dsdgen_dir, "dsdgen.exe"),
                "-SCALE", str(scale_factor),
                "-DIR", output_dir,
                "-FORCE"
            ]
            
            result = subprocess.run(
                cmd,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=self.dsdgen_dir
            )
            
            logger.info(f"Successfully generated {scale_factor}GB data using fallback method")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Fallback generation failed: {e}")
            logger.error(f"stdout: {e.stdout.decode('utf-8')}")
            logger.error(f"stderr: {e.stderr.decode('utf-8')}")
            return False

class LinuxStrategy(DataGenerationStrategy):
    """Linux-specific data generation strategy"""
    
    def __init__(self, dsdgen_path: str):
        super().__init__(dsdgen_path)
        self.is_wsl = 'microsoft-standard' in platform.uname().release.lower()
        
        # Ensure dsdgen is executable
        try:
            os.chmod(dsdgen_path, 0o755)
            logger.info(f"Made dsdgen executable: {dsdgen_path}")
        except Exception as e:
            logger.warning(f"Could not make dsdgen executable: {e}")
    
    def generate_data(self, output_dir: str, scale_factor: int) -> bool:
        try:
            cmd = [
                self.dsdgen_path,
                "-SCALE", str(scale_factor),
                "-DIR", output_dir,
                "-FORCE"
            ]
            
            result = subprocess.run(
                cmd,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=self.dsdgen_dir
            )
            
            logger.info(f"Successfully generated {scale_factor}GB data")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Data generation failed: {e}")
            logger.error(f"stdout: {e.stdout.decode('utf-8')}")
            logger.error(f"stderr: {e.stderr.decode('utf-8')}")
            return False

class DSDGenWrapper:
    """Python wrapper around dsdgen executable"""
    
    def __init__(self, dsdgen_path: str):
        """
        Initialize the wrapper
        
        Args:
            dsdgen_path: Path to dsdgen binary
        """
        self.strategy = self._create_strategy(dsdgen_path)
    
    def _create_strategy(self, dsdgen_path: str) -> DataGenerationStrategy:
        """Create appropriate strategy based on platform"""
        if platform.system() == 'Windows':
            return WindowsStrategy(dsdgen_path)
        else:
            return LinuxStrategy(dsdgen_path)
    
    def generate_data(self, output_dir: str, scale_factor: int) -> bool:
        """
        Generate TPC-DS data using platform-appropriate strategy
        
        Args:
            output_dir: Directory to output generated data
            scale_factor: Scale factor in GB
            
        Returns:
            bool: Success status
        """
        return self.strategy.generate_data(output_dir, scale_factor)

def generate_tpcds_data(data_dir: str, scale_factor: int) -> bool:
    """
    Generate TPC-DS data at specified scale factor
    
    Args:
        data_dir: Base directory for data
        scale_factor: Scale factor in GB
        
    Returns:
        bool: Success status
    """
    try:
        # Ensure data directory exists
        os.makedirs(data_dir, exist_ok=True)
        
        # Initialize generator
        dsdgen_path = os.path.join(
            os.path.dirname(__file__),
            "dsdgen",
            "dsdgen.exe" if platform.system() == "Windows" else "dsdgen"
        )
        
        generator = DSDGenWrapper(dsdgen_path)
        return generator.generate_data(data_dir, scale_factor)
        
    except Exception as e:
        logger.error(f"Error in data generation process: {e}")
        return False

def main():
    """Main function to generate TPC-DS data"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate TPC-DS data")
    parser.add_argument("--dsdgen-path", required=True, help="Path to dsdgen binary")
    parser.add_argument("--output-dir", default="../data", help="Output directory")
    parser.add_argument("--scale-factors", nargs="+", type=int, default=[1, 10], 
                        help="Scale factors in GB (default: 1 10)")
    
    args = parser.parse_args()
    
    logger.info("Starting TPC-DS data generation...")
    
    for scale in args.scale_factors:
        success = generate_tpcds_data(args.output_dir, scale)
        if not success:
            logger.error(f"Failed to generate data for scale factor {scale}GB")
            sys.exit(1)
    
    logger.info("TPC-DS data generation completed successfully!")

if __name__ == "__main__":
    main() 