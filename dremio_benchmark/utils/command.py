"""
Command execution utilities.
"""

import subprocess
import logging
from typing import List, Dict, Optional, Union, Tuple

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