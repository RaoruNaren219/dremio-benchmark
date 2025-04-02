# Cross-Dremio Benchmarking Tool

[![Python Version](https://img.shields.io/badge/python-3.10.11-blue.svg)](https://www.python.org/downloads/)

A comprehensive tool for generating test data in various formats (CSV, TXT, Parquet, ORC) and ingesting it into Dremio. This tool provides efficient data generation with memory optimization and robust ingestion capabilities.

## Table of Contents
- [Architecture](#architecture)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Data Generation](#data-generation)
- [Data Ingestion](#data-ingestion)
- [Performance Monitoring](#performance-monitoring)
- [Troubleshooting](#troubleshooting)

## Architecture

The solution consists of two main components:

1. **Data Generation Script** (`generate_test_data_formats.py`):
   - Generates test data in multiple formats (CSV, TXT, Parquet, ORC)
   - Memory-optimized data generation
   - Configurable data size and structure
   - Performance monitoring and metrics

2. **Data Ingestion Script** (`ingest_test_data.py`):
   - Handles authentication with Dremio
   - Validates file formats and integrity
   - Supports multiple file formats
   - Provides detailed ingestion metrics

## Features

- 📊 Multiple data format support (CSV, TXT, Parquet, ORC)
- 💾 Memory-optimized data generation
- 📈 Performance monitoring and metrics
- 🔍 File format validation
- 📝 Comprehensive logging
- ⚡ Batch processing
- 🔒 Secure credential management
- 🧹 Automatic memory cleanup
- 📊 Performance metrics collection
- 🔄 Configurable scale factors

## Prerequisites

- Python 3.10.11 or later
- Dremio cluster with proper access
- Sufficient disk space (minimum 10GB)
- Sufficient memory (minimum 2GB)
- Required Python packages (see requirements.txt)

## Installation

1. Clone the repository:
   ```powershell
   git clone https://github.com/yourusername/dremio-benchmark.git
   cd dremio-benchmark
   ```

2. Create and activate a virtual environment:
   ```powershell
   python -m venv venv
   .\venv\Scripts\Activate.ps1
   ```

3. Update pip and install prerequisite packages:
   ```powershell
   python -m pip install --upgrade pip
   pip install --upgrade setuptools wheel
   pip install --upgrade cython numpy
   ```

4. Install project dependencies:
   ```powershell
   pip install -r requirements.txt
   ```

5. Create a `.env` file with your Dremio credentials:
   ```env
   DREMIO_URL=http://your-dremio-host:9047
   DREMIO_USERNAME=your-username
   DREMIO_PASSWORD=your-password
   DATA_DIR=test_data
   ```

### Installation Notes

- The prerequisite packages (setuptools, wheel, cython, numpy) are required for building some of the dependencies
- Required package versions:
  - pandas==1.5.3
  - numpy==1.23.5
  - pyarrow==12.0.1
  - pyorc==1.7.0
  - requests==2.31.0
  - python-dotenv==1.0.0
  - tqdm==4.65.0
  - psutil==5.9.5
  - loguru==0.7.2

- If you encounter any installation issues, try:
  ```powershell
  pip install --upgrade pip setuptools wheel
  pip install --upgrade cython numpy
  pip install -r requirements.txt
  ```

- For Windows users:
  - Install Visual C++ Build Tools if not already installed
  - If you encounter build errors, try installing the pre-built wheels:
    ```powershell
    pip install --only-binary :all: numpy==1.23.5
    pip install --only-binary :all: pandas==1.5.3
    ```

- The scripts will automatically check for compatible package versions at startup
- If version mismatches are detected, the scripts will provide instructions for installing the correct versions

## Configuration

The tool supports the following configuration options:

1. **Environment Variables**:
   - `DREMIO_URL`: Dremio server URL
   - `DREMIO_USERNAME`: Dremio username
   - `DREMIO_PASSWORD`: Dremio password
   - `DATA_DIR`: Directory for generated data

2. **Command Line Arguments**:
   ```powershell
   python ingest_test_data.py --sizes 1 10 100 --formats csv parquet orc --space test_data --dry-run
   ```
   - `--sizes`: File sizes to generate (1, 10, or 100 GB)
   - `--formats`: File formats to generate
   - `--space`: Dremio space name
   - `--dry-run`: Preview without actual ingestion

## Usage

### Data Generation

1. Generate test data in all formats:
   ```powershell
   python generate_test_data_formats.py
   ```

2. The script will:
   - Check system resources
   - Generate data in chunks
   - Optimize memory usage
   - Create files in the specified directory
   - Record performance metrics

3. Generated data structure:
   ```python
   {
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
   ```

### Data Ingestion

1. Ingest generated data into Dremio:
   ```powershell
   python ingest_test_data.py
   ```

2. The script will:
   - Validate file formats
   - Check file integrity
   - Create necessary Dremio spaces
   - Ingest data with proper settings
   - Monitor ingestion progress

3. Supported ingestion formats:
   - CSV: Text-based with headers
   - TXT: Tab-separated values
   - Parquet: Columnar format with optimizations
   - ORC: Optimized Row Columnar format

## Performance Monitoring

The tool provides comprehensive performance metrics:

1. **Generation Metrics**:
   - File sizes
   - Generation times
   - Memory usage
   - Chunk processing times

2. **Ingestion Metrics**:
   - Success/failure rates
   - Ingestion times
   - File validation results
   - Resource usage

3. **Logging**:
   - Detailed operation logs
   - Error tracking
   - Performance summaries
   - Resource utilization

## Troubleshooting

1. **Memory Issues**:
   - Check available system memory
   - Adjust chunk size in configuration
   - Monitor memory usage in logs
   - Use memory cleanup functions

2. **Format Issues**:
   - Verify file format compatibility
   - Check file integrity
   - Review format-specific settings
   - Validate data structure

3. **Ingestion Issues**:
   - Check Dremio connectivity
   - Verify credentials
   - Review space permissions
   - Check file access rights

4. **Performance Issues**:
   - Monitor system resources
   - Check disk space
   - Review chunk sizes
   - Analyze performance metrics

5. **Installation Issues**:
   - Ensure pip is up to date
   - Install prerequisite packages first
   - Check for Visual C++ Build Tools on Windows
   - Verify Python version compatibility

## License

This project is proprietary and confidential. All rights reserved.