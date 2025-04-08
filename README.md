# Dremio Cross-Cluster Benchmarking

A comprehensive Python framework for benchmarking Dremio clusters using TPC-DS data across multiple formats. It supports benchmarking between a Simple-auth HDFS cluster and a Kerberized HDFS cluster.

## Features

- Generate TPC-DS datasets at 1GB and 10GB scale factors
- Convert raw data to multiple formats (CSV, JSON, Pipe-delimited, ORC, Parquet)
- Upload datasets to both HDFS clusters with structured directories
- Create Dremio-compatible SQL DDLs for all 24 TPC-DS tables across all formats
- Setup cross-cluster access between Dremio instances
- Run benchmarks on individual clusters and across clusters
- Generate comprehensive performance reports with visualizations

## Project Structure

```
dremio-benchmark/
│
├── config/                # Configuration files
│   └── default_config.yml # Default configuration
│
├── data-generation/       # Python scripts for generating and converting TPC-DS data
│   ├── generate_tpcds_data.py
│   └── convert_data_formats.py
│
├── dremio-ddls/           # Scripts for generating and executing Dremio DDLs
│   └── create_tables.py
│
├── hdfs-upload/           # Python script for uploading data to HDFS clusters
│   └── upload_to_hdfs.py
│
├── cross-cluster-setup/   # Scripts for setting up cross-cluster access
│   └── setup_cross_cluster.py
│
├── test-automation/       # Scripts for running benchmarks
│   ├── run_benchmarks.py
│   └── example_queries/
│
├── reports/               # Scripts for generating reports
│   └── generate_report.py
│
├── utils/                 # Utility modules
│   ├── command.py         # Command execution utilities
│   ├── config.py          # Configuration utilities
│   ├── constants.py       # Constants
│   ├── filesystem.py      # File system utilities
│   └── logging_config.py  # Logging utilities
│
├── main.py                # Main orchestration script
└── requirements.txt       # Python dependencies
```

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/RaoruNaren219/dremio-benchmark.git
cd dremio-benchmark
```

### 2. Set up a Python virtual environment (recommended)

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment (Windows)
venv\Scripts\activate

# Activate virtual environment (Linux/Mac)
source venv/bin/activate
```

### 3. Install Python dependencies

```bash
pip install -r requirements.txt
```

Alternatively, you can install the package in development mode:

```bash
pip install -e .
```

### 4. Set up TPC-DS toolkit

```bash
# Download and build the TPC-DS toolkit
git clone https://github.com/gregrahn/tpcds-kit.git
cd tpcds-kit/tools
make
cd ../..
```

### 5. Configure the benchmark

Copy the sample configuration files:

```bash
cp .env.sample .env
cp config/default_config.yml.sample config/default_config.yml
```

Edit the `.env` file and `config/default_config.yml` file with your specific settings.

## Configuration

### Configuration Files

The benchmark uses two types of configuration:

1. **YAML Configuration**: `config/default_config.yml` contains the main configuration settings.
2. **Environment Variables**: `.env` file contains sensitive credentials and can override YAML settings.

### Using Environment Variables

For sensitive information like passwords and authentication details, you can use environment variables instead of storing them in the configuration file.

Environment variables are automatically loaded from the `.env` file and will override the corresponding configuration in the YAML file.

Example environment variables:

```
DREMIO_DREMIO_A_HOST=dremio-a.example.com
DREMIO_DREMIO_A_USERNAME=admin
DREMIO_DREMIO_A_PASSWORD=your_password_here
DREMIO_KERBERIZED_PRINCIPAL=hdfs@EXAMPLE.COM
```

## Pipeline Execution Steps

The benchmarking pipeline consists of the following steps that can be executed individually or all together:

### 1. Data Generation (Step: `data`)

Generates TPC-DS data at specified scale factors using the TPC-DS toolkit.

```bash
python main.py --steps data
```

**Key Settings**:
- `data_generation.dsdgen_path`: Path to the TPC-DS dsdgen executable
- `pipeline.scale_factors`: List of scale factors to generate (e.g., [1, 10])

**Progress**: The script will show progress for each table being generated and log completion status.

### 2. Data Conversion (Step: `convert`)

Converts the raw data into various formats (CSV, JSON, Pipe-delimited, ORC, Parquet).

```bash
python main.py --steps convert
```

**Key Settings**:
- `pipeline.formats`: List of formats to convert to (e.g., ["csv", "parquet", "orc"])
- `pipeline.scale_factors`: Scale factors to process

**Progress**: The script will show conversion progress for each table and format.

### 3. HDFS Upload (Step: `upload`)

Uploads the formatted data to both HDFS clusters (Simple-auth and Kerberized).

```bash
python main.py --steps upload
```

**Key Settings**:
- `hdfs.simple_auth`: Simple authentication HDFS configuration
- `hdfs.kerberized`: Kerberized HDFS configuration
- `pipeline.hdfs_target_dir`: Target directory in HDFS

**Progress**: The script will show upload progress for each table and format.

### 4. DDL Generation (Step: `ddl`)

Generates and optionally executes Dremio DDL statements for all tables.

```bash
python main.py --steps ddl
```

**Key Settings**:
- `clusters.dremio_a`: Configuration for Dremio cluster A
- `clusters.dremio_b`: Configuration for Dremio cluster B
- `execute_ddl`: Whether to execute the generated DDL statements

**Progress**: The script will show DDL generation progress and execution status if enabled.

### 5. Cross-Cluster Setup (Step: `cross`)

Sets up cross-cluster access between Dremio instances.

```bash
python main.py --steps cross
```

**Key Settings**:
- `clusters.dremio_a`: Configuration for Dremio cluster A
- `clusters.dremio_b`: Configuration for Dremio cluster B

**Progress**: The script will show setup progress and connection status.

### 6. Run Benchmarks (Step: `benchmark`)

Runs benchmark queries on both clusters and across clusters.

```bash
python main.py --steps benchmark
```

**Key Settings**:
- `pipeline.query_dir`: Directory containing benchmark queries
- `pipeline.timeout_seconds`: Query timeout in seconds
- `pipeline.num_iterations`: Number of iterations for each query

**Progress**: The script will show query execution progress, timing information, and success/failure status for each query.

### 7. Generate Reports (Step: `report`)

Generates performance reports and visualizations.

```bash
python main.py --steps report
```

**Key Settings**:
- `reports.output_dir`: Directory for report output
- `reports.generate_charts`: Whether to generate charts
- `reports.format`: Report format (e.g., "html", "csv")

**Progress**: The script will show report generation progress and output file locations.

## Running the Complete Pipeline

To run the entire pipeline at once:

```bash
python main.py --steps all
```

To run specific steps:

```bash
python main.py --steps data convert upload
```

To use a custom configuration file:

```bash
python main.py --config config/my_custom_config.yml --steps all
```

## Pipeline Progress

When running the pipeline, you'll see progress information for each step:

1. **Initialization**: Loading configuration and setting up directories
2. **Step Execution**: For each step, you'll see:
   - Step start notification
   - Progress for each substep (e.g., table processing)
   - Success/failure status
3. **Summary**: At the end, a summary of all steps executed and their status

All output is also logged to `benchmark_pipeline.log` for later review.

## Security Considerations

- Use environment variables for sensitive information like passwords and tokens
- Never commit the `.env` file to version control
- Consider using a credentials manager for production deployments
- Ensure proper access controls on keytab files and configuration directories

## Troubleshooting

If you encounter issues:

1. Check the log file: `benchmark_pipeline.log`
2. Verify your configuration settings
3. Ensure all required services (Dremio, HDFS) are accessible
4. For data generation issues, verify the TPC-DS toolkit is properly compiled