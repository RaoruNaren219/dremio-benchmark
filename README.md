# Cross-Dremio Benchmarking Tool

[![Python Version](https://img.shields.io/badge/python-3.10.11-blue.svg)](https://www.python.org/downloads/)

A comprehensive tool for benchmarking read and write operations between two Dremio clusters using the Dremio REST API. This tool helps measure and analyze the performance of cross-cluster operations in a Dremio environment.

## Table of Contents
- [Architecture](#architecture)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [IDE Setup](#ide-setup)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Data Generation](#data-generation)
- [Benchmarking](#benchmarking)
- [Results](#results)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

## Architecture

The solution consists of two main components:

1. **Data Generation Script** (`generate_test_data.py`):
   - Generates test data in CSV format
   - Configurable data size and structure
   - Supports multiple data types and patterns

2. **Benchmark Script** (`dremio_benchmark.py`):
   - Handles authentication with Dremio clusters
   - Performs read and write operations
   - Measures execution times
   - Provides detailed benchmarking results

## Features

- 🔄 Cross-cluster read/write operations
- 📊 Detailed performance metrics
- 📈 Synthetic data generation
- 🔍 Source verification
- 📝 Comprehensive logging
- ⚡ Batch processing
- 🔒 Secure credential management
- Configurable data generation
- Secure authentication handling
- Error handling and logging
- Support for multiple Dremio clusters
- TPC-DS benchmark data generation and ingestion

## Prerequisites

- Python 3.10.11 or later
- Dremio 1 cluster with Dremio 2 configured as a source
- Access to both Dremio clusters
- Dremio 2 source properly configured in Dremio 1
- Sufficient permissions on both clusters

## IDE Setup

### PyCharm Configuration

1. **Open Project in PyCharm**:
   - Launch PyCharm
   - Go to `File > Open`
   - Navigate to your project directory and select it
   - Choose "Open as Project"

2. **Configure Python Interpreter**:
   - Go to `File > Settings > Project: dremio-benchmark > Python Interpreter`
   - Click the gear icon and select "Add"
   - Choose "New Environment" and select "Virtual Environment"
   - Set the base interpreter to Python 3.10.11
   - Click "OK" to create the virtual environment

3. **Install Dependencies**:
   - Open the integrated terminal in PyCharm (View > Tool Windows > Terminal)
   - Run the following command:
   ```powershell
   pip install -r requirements.txt
   ```

4. **Configure Run Configurations**:
   - Click "Add Configuration" in the run configurations dropdown
   - Add two Python configurations:
     
     a. For data generation:
     ```
     Name: Generate Test Data
     Script path: generate_test_data.py
     Working directory: <project_root>
     Python interpreter: <your_virtual_env>
     ```

     b. For benchmarking:
     ```
     Name: Run Benchmark
     Script path: dremio_benchmark.py
     Working directory: <project_root>
     Python interpreter: <your_virtual_env>
     ```

5. **Enable Auto-Import**:
   - Go to `File > Settings > Editor > General > Auto Import`
   - Check "Optimize imports on the fly"
   - Check "Add unambiguous imports on the fly"

### Windows PowerShell Setup

1. **Open PowerShell as Administrator**:
   - Right-click on PowerShell
   - Select "Run as Administrator"

2. **Set Execution Policy** (if needed):
   ```powershell
   Set-ExecutionPolicy RemoteSigned -Scope CurrentUser
   ```

3. **Navigate to Project Directory**:
   ```powershell
   cd C:\path\to\your\project
   ```

4. **Create and Activate Virtual Environment**:
   ```powershell
   python -m venv venv
   .\venv\Scripts\Activate.ps1
   ```

5. **Install Dependencies**:
   ```powershell
   pip install -r requirements.txt
   ```

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

3. Install dependencies:
   ```powershell
   pip install -r requirements.txt
   ```

4. Create a `.env` file with your Dremio credentials:
   ```env
   # Dremio 1 Cluster Configuration (Main Cluster)
   DREMIO1_URL=http://your-dremio1-host:9047
   DREMIO1_USERNAME=your-username
   DREMIO1_PASSWORD=your-password

   # Dremio 2 Cluster Configuration (Source Cluster)
   DREMIO2_URL=http://your-dremio2-host:9047
   DREMIO2_USERNAME=your-username
   DREMIO2_PASSWORD=your-password

   # Source Configuration
   DREMIO2_SOURCE_NAME=dremio2_source
   ```

## Configuration

1. Copy the environment template:
   ```bash
   cp .env.template .env
   ```

2. Configure your environment variables in `.env`:
   ```env
   # Dremio 1 Cluster Configuration (Main Cluster)
   DREMIO1_URL=http://your-dremio1-host:9047
   DREMIO1_USERNAME=your-username
   DREMIO1_PASSWORD=your-password

   # Dremio 2 Cluster Configuration (Source Cluster)
   DREMIO2_URL=http://your-dremio2-host:9047
   DREMIO2_USERNAME=your-username
   DREMIO2_PASSWORD=your-password

   # Source Configuration
   DREMIO2_SOURCE_NAME=dremio2_source
   ```

## Usage

### Running Scripts in PyCharm

1. **Generate Test Data**:
   - Select "Generate Test Data" from the run configurations dropdown
   - Click the green "Run" button or press Shift+F10

2. **Run Benchmark**:
   - Select "Run Benchmark" from the run configurations dropdown
   - Click the green "Run" button or press Shift+F10

### Running Scripts in PowerShell

1. **Generate Test Data**:
   ```powershell
   python generate_test_data.py
   ```

2. **Run Benchmark**:
   ```powershell
   python dremio_benchmark.py
   ```

### Data Generation

Generate and ingest test data into both Dremio clusters:
```bash
python generate_test_data.py
```

This script will:
1. Generate 1 million synthetic transaction records
2. Create the `sales_db` schema in both clusters
3. Create the `transactions` table with the following schema:
   ```sql
   CREATE TABLE sales_db.transactions (
       transaction_id BIGINT,
       customer_id BIGINT,
       transaction_date TIMESTAMP,
       amount DOUBLE,
       category VARCHAR,
       created_at TIMESTAMP
   )
   ```
4. Ingest the data into both clusters
5. Create the `benchmark_results` schema in Dremio 2

### Benchmarking

Run the benchmark script:
```bash
python dremio_benchmark.py
```

The script will:
1. Verify the Dremio 2 source configuration
2. Execute 3 iterations of read/write operations
3. Measure and report performance metrics

## Results

The benchmark provides:
- ⏱️ Operation duration
- 📊 Number of rows processed
- 🕒 Timestamps
- 📈 Average performance metrics

Sample output:
```
Cross-Dremio Benchmark Results:
+----------+-------------+----------------+---------------------+
| Operation| Duration (s)| Rows Processed | Timestamp          |
+----------+-------------+----------------+---------------------+
| READ     | 2.34        | 1000000        | 2024-01-20T10:30:00|
| WRITE    | 3.45        | 1000000        | 2024-01-20T10:30:05|
+----------+-------------+----------------+---------------------+

Summary:
Average READ duration: 2.34 seconds
Average WRITE duration: 3.45 seconds
```

## Troubleshooting

### PyCharm-Specific Issues

1. **Virtual Environment Not Found**:
   - Go to `File > Settings > Project: dremio-benchmark > Python Interpreter`
   - Click "Add" and select "Existing Environment"
   - Navigate to `venv\Scripts\python.exe` in your project directory

2. **Terminal Not Activating Virtual Environment**:
   - Open PyCharm settings
   - Go to `Tools > Terminal`
   - Set "Shell path" to: `C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe`
   - Add the following to "Shell integration": `.\venv\Scripts\Activate.ps1`

3. **Run Configuration Issues**:
   - Verify script paths are correct
   - Ensure working directory is set to project root
   - Check that Python interpreter is selected

### PowerShell-Specific Issues

1. **Execution Policy Errors**:
   ```powershell
   Set-ExecutionPolicy RemoteSigned -Scope CurrentUser
   ```

2. **Virtual Environment Activation Issues**:
   ```powershell
   # If activation script is blocked
   Set-ExecutionPolicy RemoteSigned -Scope Process
   .\venv\Scripts\Activate.ps1
   ```

3. **Path Issues**:
   ```powershell
   # Add Python to PATH if needed
   $env:Path += ";C:\Python310"
   ```

### Common Issues

1. **Authentication Errors**
   - Verify credentials in `.env`
   - Check user permissions
   - Ensure correct API endpoints

2. **Source Configuration**
   - Verify Dremio 2 source name
   - Check source connectivity
   - Validate source permissions

3. **Schema/Table Issues**
   - Verify CREATE permissions
   - Check existing schemas/tables
   - Validate SQL syntax

### Performance Optimization

- Adjust batch size in `generate_test_data.py`
- Monitor memory usage
- Consider network latency
- Optimize query patterns

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## Support

For support, please open an issue in the GitHub repository or contact the maintainers.

## Notes

- The data generation script creates realistic test data with:
  - Random transaction dates throughout 2023
  - Normally distributed transaction amounts
  - Random customer IDs
  - Product categories
- Data is ingested in batches of 1000 records to prevent memory issues
- The script uses a 5-second delay between benchmark iterations to prevent system overload
- All operations are executed from Dremio 1, which communicates with Dremio 2 via the source connector
- The script verifies the source configuration before starting the benchmark

## TPC-DS Data Preparation and Ingestion

### Prerequisites

1. Install TPC-DS tools:
   - Download and install the TPC-DS tools from the official repository
   - Ensure `dsdgen` and `dsqgen` are in your system PATH

2. Configure Hadoop environment:
   - Ensure Hadoop is running and accessible
   - Configure HDFS paths and permissions
   - Set up necessary Hadoop configurations

### Execution Steps

1. **Generate TPC-DS Data**:
   ```bash
   # Generate data for different scale factors (1GB, 10GB, 100GB)
   ./generate_tpcds_data.sh
   ```
   This will create data in the following structure:
   ```
   /tpcds_data/
   ├── sf1/
   │   ├── call_center.dat
   │   ├── catalog_sales.dat
   │   └── ...
   ├── sf10/
   │   ├── call_center.dat
   │   ├── catalog_sales.dat
   │   └── ...
   └── sf100/
       ├── call_center.dat
       ├── catalog_sales.dat
       └── ...
   ```

2. **Convert Data to Parquet Format**:
   ```bash
   # Convert all generated data to Parquet format
   python convert_to_parquet.py
   ```
   This will create Parquet files in:
   ```
   /tpcds_parquet/
   ├── sf1/
   │   ├── call_center.parquet
   │   ├── catalog_sales.parquet
   │   └── ...
   ├── sf10/
   │   ├── call_center.parquet
   │   ├── catalog_sales.parquet
   │   └── ...
   └── sf100/
       ├── call_center.parquet
       ├── catalog_sales.parquet
       └── ...
   ```

3. **Create Tables in Hadoop**:
   ```sql
   -- Execute the table creation script
   \i create_tpcds_tables.sql
   ```
   This will create the following schemas and tables:
   - `tpcds_sf1.*`
   - `tpcds_sf10.*`
   - `tpcds_sf100.*`

4. **Ingest Data into Hadoop**:
   ```sql
   -- Execute the data ingestion script
   \i ingest_tpcds_data.sql
   ```
   This will:
   - Load data from Parquet files into the corresponding tables
   - Verify the data ingestion with row counts
   - Display a summary of loaded data

5. **Verify Data**:
   ```sql
   -- Check row counts for all tables
   SELECT schema_name, table_name, COUNT(*) as row_count
   FROM (
       SELECT 'tpcds_sf1' as schema_name, 'call_center' as table_name, COUNT(*) FROM tpcds_sf1.call_center
       UNION ALL
       -- ... other tables ...
   ) counts
   ORDER BY schema_name, table_name;
   ```

### Expected Results

After successful execution, you should see:
- Generated data files in CSV format
- Converted Parquet files
- Created tables in Hadoop
- Ingested data with proper row counts
- Verification results showing the number of rows in each table

### Troubleshooting

1. **Data Generation Issues**:
   - Check TPC-DS tools installation
   - Verify disk space availability
   - Check file permissions

2. **Parquet Conversion Issues**:
   - Verify Python dependencies
   - Check memory availability
   - Review error logs

3. **Hadoop Ingestion Issues**:
   - Verify Hadoop connectivity
   - Check table permissions
   - Review HDFS paths and permissions

## License

This project is proprietary and confidential. All rights reserved.