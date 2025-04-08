# Utility Modules

This directory contains utility modules that are used throughout the Dremio benchmarking project.

## Overview

- **command.py**: Utilities for running shell commands with proper logging and error handling
- **config.py**: Configuration utilities for loading and manipulating YAML configuration files
- **constants.py**: Constants used throughout the project
- **filesystem.py**: Utilities for file system operations
- **logging_config.py**: Logging configuration utilities

## Usage

```python
# Example: Command utilities
from utils.command import run_command

success, stdout, stderr = run_command(
    ["python", "my_script.py", "--arg1", "value1"],
    "Running my script"
)

# Example: Logging configuration
from utils.logging_config import setup_logging

logger = setup_logging(
    log_file="my_module.log",
    module_name="dremio_benchmark.my_module"
)

# Example: Config loading
from utils.config import load_config, get_cluster_config

config = load_config("config/my_config.yml")
dremio_a_config = get_cluster_config(config, "dremio_a")

# Example: File system utilities
from utils.filesystem import create_directories
from utils.constants import DEFAULT_DIRECTORIES

dirs = create_directories("./my_pipeline", DEFAULT_DIRECTORIES)
``` 