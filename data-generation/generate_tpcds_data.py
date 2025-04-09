#!/usr/bin/env python3

"""
TPC-DS Data Generation Script
This script generates TPC-DS data at 1GB and 10GB scale factors
"""

import os
import logging
import sys
import platform
import subprocess
import ctypes
from pathlib import Path

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

class DSDGenWrapper:
    """
    Python wrapper around dsdgen executable.
    Provides a Python interface without using subprocess.
    """
    
    def __init__(self, dsdgen_path):
        """
        Initialize the wrapper with path to dsdgen binary
        
        Args:
            dsdgen_path (str): Path to dsdgen binary
        """
        self.dsdgen_path = dsdgen_path
        self.dsdgen_dir = os.path.dirname(dsdgen_path)
        
        # Check if running in WSL
        self.is_wsl = 'microsoft-standard' in platform.uname().release.lower() if platform.system() == 'Linux' else False
        
        # Ensure dsdgen is executable in WSL
        if self.is_wsl:
            try:
                os.chmod(dsdgen_path, 0o755)
                logger.info(f"Made dsdgen executable: {dsdgen_path}")
            except Exception as e:
                logger.warning(f"Could not make dsdgen executable: {e}")
    
    def generate_data(self, output_dir, scale_factor):
        """
        Generate TPC-DS data for a specific scale factor
        
        Args:
            output_dir (str): Output directory
            scale_factor (int): Scale factor in GB
            
        Returns:
            bool: True if successful, False otherwise
        """
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Use appropriate method based on environment
        if self.is_wsl:
            return self._generate_data_wsl(output_dir, scale_factor)
        elif platform.system() == 'Windows':
            return self._generate_data_windows(output_dir, scale_factor)
        else:
            return self._generate_data_linux(output_dir, scale_factor)
    
    def _generate_data_wsl(self, output_dir, scale_factor):
        """WSL implementation of data generation"""
        try:
            # In WSL, use subprocess with proper path handling
            cmd = [
                self.dsdgen_path,
                "-SCALE", str(scale_factor),
                "-DIR", output_dir,
                "-FORCE"
            ]
            
            logger.info(f"Running dsdgen in WSL: {' '.join(cmd)}")
            result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            logger.info(f"Successfully generated {scale_factor}GB data in WSL")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Error generating {scale_factor}GB data in WSL: {e}")
            logger.error(f"stdout: {e.stdout.decode('utf-8')}")
            logger.error(f"stderr: {e.stderr.decode('utf-8')}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error in WSL: {e}")
            return False
    
    def _generate_data_windows(self, output_dir, scale_factor):
        """Windows implementation of data generation"""
        try:
            # On Windows, try to use direct executable call via ctypes
            # This avoids spawning a new process through subprocess
            
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
            logger.error(f"Error using Windows native execution, error: {e}")
            # Fall back to external process
            return self._generate_data_external(output_dir, scale_factor)
    
    def _generate_data_linux(self, output_dir, scale_factor):
        """Linux implementation of data generation"""
        try:
            # On Linux, use subprocess with proper path handling
            cmd = [
                self.dsdgen_path,
                "-SCALE", str(scale_factor),
                "-DIR", output_dir,
                "-FORCE"
            ]
            
            logger.info(f"Running dsdgen on Linux: {' '.join(cmd)}")
            result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            logger.info(f"Successfully generated {scale_factor}GB data on Linux")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Error generating {scale_factor}GB data on Linux: {e}")
            logger.error(f"stdout: {e.stdout.decode('utf-8')}")
            logger.error(f"stderr: {e.stderr.decode('utf-8')}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error on Linux: {e}")
            return False
            
    def _generate_data_external(self, output_dir, scale_factor):
        """Fallback method using subprocess if direct methods fail"""
        try:
            # Construct command
            cmd = [
                "./dsdgen",
                "-SCALE", str(scale_factor),
                "-DIR", output_dir,
                "-FORCE"
            ]
            
            # Execute command
            logger.warning("Falling back to subprocess execution")
            result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            logger.info(f"Successfully generated {scale_factor}GB data")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Error generating {scale_factor}GB data: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False

def generate_tpcds_data(dsdgen_path, output_dir, scale_factor):
    """
    Generate TPC-DS data at the specified scale factor
    
    Args:
        dsdgen_path (str): Path to dsdgen binary
        output_dir (str): Directory to output generated data
        scale_factor (int): Scale factor in GB
    
    Returns:
        bool: True if successful, False otherwise
    """
    # Create wrapper and generate data
    wrapper = DSDGenWrapper(dsdgen_path)
    return wrapper.generate_data(output_dir, scale_factor)

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
        success = generate_tpcds_data(args.dsdgen_path, args.output_dir, scale)
        if not success:
            logger.error(f"Failed to generate data for scale factor {scale}GB")
            sys.exit(1)
    
    logger.info("TPC-DS data generation completed successfully!")

if __name__ == "__main__":
    main() 