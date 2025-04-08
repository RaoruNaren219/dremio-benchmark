"""
Command execution utilities.
Provides both shell-based and Python-native command execution methods.
"""

import subprocess
import logging
import os
import shutil
from typing import List, Dict, Optional, Union, Tuple, Callable, Any

logger = logging.getLogger(__name__)


def run_command(
    cmd: List[str],
    description: str,
    env: Optional[Dict[str, str]] = None,
    cwd: Optional[str] = None
) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Run a command and log the output.
    
    Args:
        cmd: Command to run as a list of strings
        description: Description of the command for logging
        env: Environment variables to set
        cwd: Current working directory for command execution
    
    Returns:
        Tuple containing:
        - Success status (bool)
        - Standard output (str or None)
        - Standard error (str or None)
    """
    logger.info(f"Running {description}...")
    logger.debug(f"Command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            cwd=cwd
        )
        
        stdout = result.stdout.decode('utf-8')
        stderr = result.stderr.decode('utf-8')
        
        logger.info(f"{description} completed successfully")
        return True, stdout, stderr
    
    except subprocess.CalledProcessError as e:
        logger.error(f"{description} failed: {e}")
        logger.error(f"stdout: {e.stdout.decode('utf-8')}")
        logger.error(f"stderr: {e.stderr.decode('utf-8')}")
        return False, e.stdout.decode('utf-8'), e.stderr.decode('utf-8')
    
    except Exception as e:
        logger.error(f"Error running {description}: {str(e)}")
        return False, None, str(e)


class PythonCommand:
    """
    A Python-native command execution class that replaces shell commands with
    equivalent Python functionality.
    """

    @staticmethod
    def run_python_command(
        cmd_func: Callable[..., Any],
        args: List[Any],
        kwargs: Dict[str, Any],
        description: str
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Run a Python function instead of a shell command.
        
        Args:
            cmd_func: Python function to execute
            args: Positional arguments for the function
            kwargs: Keyword arguments for the function
            description: Description of the operation for logging
            
        Returns:
            Tuple containing:
            - Success status (bool)
            - Standard output (str or None)
            - Standard error (str or None)
        """
        logger.info(f"Running {description} using Python native functionality...")
        
        try:
            result = cmd_func(*args, **kwargs)
            logger.info(f"{description} completed successfully")
            return True, str(result) if result is not None else "", ""
        except Exception as e:
            logger.error(f"{description} failed: {str(e)}")
            return False, None, str(e)
            
    @staticmethod
    def mkdir(path: str, create_parents: bool = False) -> Tuple[bool, str, str]:
        """
        Create directory using Python instead of mkdir shell command.
        
        Args:
            path: Path to create
            create_parents: Create parent directories if they don't exist
            
        Returns:
            Tuple of success status, stdout-like message, stderr-like message
        """
        try:
            if create_parents:
                os.makedirs(path, exist_ok=True)
            else:
                os.mkdir(path)
            return True, f"Created directory: {path}", ""
        except Exception as e:
            return False, "", f"Error creating directory {path}: {str(e)}"
            
    @staticmethod
    def copy_file(src: str, dst: str) -> Tuple[bool, str, str]:
        """
        Copy file using Python instead of cp shell command.
        
        Args:
            src: Source file path
            dst: Destination file path
            
        Returns:
            Tuple of success status, stdout-like message, stderr-like message
        """
        try:
            shutil.copy2(src, dst)
            return True, f"Copied {src} to {dst}", ""
        except Exception as e:
            return False, "", f"Error copying {src} to {dst}: {str(e)}" 