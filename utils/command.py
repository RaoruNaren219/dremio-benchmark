"""
Command execution utilities.
Provides a unified command execution system for both shell and Python-native operations.
"""

import subprocess
import logging
import os
import shutil
from typing import List, Dict, Optional, Union, Tuple, Callable, Any
from pathlib import Path

logger = logging.getLogger(__name__)

class CommandExecutor:
    """
    Unified command execution system that handles both shell commands and Python-native operations.
    """
    
    def __init__(self, working_dir: Optional[str] = None, env: Optional[Dict[str, str]] = None):
        """
        Initialize the command executor.
        
        Args:
            working_dir: Working directory for command execution
            env: Environment variables to use
        """
        self.working_dir = working_dir
        self.env = env or os.environ.copy()
    
    def run_shell_command(
        self,
        cmd: List[str],
        description: str,
        capture_output: bool = True
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Run a shell command with proper logging and error handling.
        
        Args:
            cmd: Command to run as a list of strings
            description: Description of the command for logging
            capture_output: Whether to capture and return command output
            
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
                stdout=subprocess.PIPE if capture_output else None,
                stderr=subprocess.PIPE if capture_output else None,
                env=self.env,
                cwd=self.working_dir
            )
            
            stdout = result.stdout.decode('utf-8') if capture_output else None
            stderr = result.stderr.decode('utf-8') if capture_output else None
            
            logger.info(f"{description} completed successfully")
            return True, stdout, stderr
            
        except subprocess.CalledProcessError as e:
            logger.error(f"{description} failed: {e}")
            if capture_output:
                logger.error(f"stdout: {e.stdout.decode('utf-8')}")
                logger.error(f"stderr: {e.stderr.decode('utf-8')}")
            return False, e.stdout.decode('utf-8') if capture_output else None, e.stderr.decode('utf-8') if capture_output else None
            
        except Exception as e:
            logger.error(f"Error running {description}: {str(e)}")
            return False, None, str(e)
    
    def run_python_operation(
        self,
        operation: Callable[..., Any],
        args: List[Any],
        kwargs: Dict[str, Any],
        description: str
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Execute a Python function with proper logging and error handling.
        
        Args:
            operation: Python function to execute
            args: Positional arguments for the function
            kwargs: Keyword arguments for the function
            description: Description of the operation for logging
            
        Returns:
            Tuple containing:
            - Success status (bool)
            - Operation output (str or None)
            - Error message (str or None)
        """
        logger.info(f"Running {description} using Python native functionality...")
        
        try:
            result = operation(*args, **kwargs)
            logger.info(f"{description} completed successfully")
            return True, str(result) if result is not None else "", ""
        except Exception as e:
            logger.error(f"{description} failed: {str(e)}")
            return False, None, str(e)
    
    def mkdir(self, path: Union[str, Path], create_parents: bool = False) -> Tuple[bool, str, str]:
        """
        Create directory using Python native functionality.
        
        Args:
            path: Path to create
            create_parents: Create parent directories if they don't exist
            
        Returns:
            Tuple of success status, output message, error message
        """
        path_str = str(path)
        return self.run_python_operation(
            os.makedirs if create_parents else os.mkdir,
            [path_str],
            {"exist_ok": True} if create_parents else {},
            f"Creating directory: {path_str}"
        )
    
    def copy_file(self, src: Union[str, Path], dst: Union[str, Path]) -> Tuple[bool, str, str]:
        """
        Copy file using Python native functionality.
        
        Args:
            src: Source file path
            dst: Destination file path
            
        Returns:
            Tuple of success status, output message, error message
        """
        src_str, dst_str = str(src), str(dst)
        return self.run_python_operation(
            shutil.copy2,
            [src_str, dst_str],
            {},
            f"Copying {src_str} to {dst_str}"
        )
    
    def remove(self, path: Union[str, Path], recursive: bool = False) -> Tuple[bool, str, str]:
        """
        Remove file or directory using Python native functionality.
        
        Args:
            path: Path to remove
            recursive: Recursively remove directories
            
        Returns:
            Tuple of success status, output message, error message
        """
        path_str = str(path)
        if recursive:
            return self.run_python_operation(
                shutil.rmtree,
                [path_str],
                {},
                f"Removing directory recursively: {path_str}"
            )
        else:
            operation = os.rmdir if os.path.isdir(path_str) else os.remove
            return self.run_python_operation(
                operation,
                [path_str],
                {},
                f"Removing {'directory' if os.path.isdir(path_str) else 'file'}: {path_str}"
            )

# For backward compatibility
def run_command(
    cmd: List[str],
    description: str,
    env: Optional[Dict[str, str]] = None,
    cwd: Optional[str] = None
) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Backward compatibility wrapper for running shell commands.
    """
    executor = CommandExecutor(working_dir=cwd, env=env)
    return executor.run_shell_command(cmd, description) 