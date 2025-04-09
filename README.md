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

## Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/RaoruNaren219/dremio-benchmark.git
cd dremio-benchmark
```

### 2. Set up a Python virtual environment

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

### 6. Run the complete pipeline

```bash
python main.py --steps all
```

## Git Workflow

This project follows a Git-based workflow for version control and collaboration. Here's how to work with the repository:

### Basic Git Operations

```bash
# Check repository status
git status

# Pull latest changes
git pull origin main

# Create a new branch for your feature
git checkout -b feature/your-feature-name

# Stage changes
git add <modified-files>

# Commit changes
git commit -m "Description of your changes"

# Push changes to remote repository
git push origin feature/your-feature-name
```

### Branching Strategy

- `main`: Production-ready code
- `develop`: Development branch for integration
- `feature/*`: Feature branches for new functionality
- `bugfix/*`: Branches for bug fixes
- `release/*`: Branches for release preparation

### Git Best Practices

1. **Commit Messages**:
   - Use clear, descriptive commit messages
   - Follow the format: "Type: Description" (e.g., "Feature: Add HDFS upload functionality")
   - Types: Feature, Bugfix, Docs, Refactor, Test, Chore

2. **Branching**:
   - Create feature branches from `develop`
   - Keep branches focused on a single feature or fix
   - Delete branches after merging

3. **Pull Requests**:
   - Create pull requests for all changes
   - Include a clear description of changes
   - Reference related issues in the description

4. **Code Review**:
   - Request reviews from team members
   - Address review comments promptly
   - Ensure CI checks pass before merging

### Git Configuration

```bash
# Configure your Git identity
git config --global user.name "Your Name"
git config --global user.email "your.email@example.com"

# Configure Git to use LF line endings (recommended for cross-platform development)
git config --global core.autocrlf input
```

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

### 2. Data Conversion (Step: `convert`)

Converts the raw data into various formats (CSV, JSON, Pipe-delimited, ORC, Parquet).

```bash
python main.py --steps convert
```

### 3. HDFS Upload (Step: `upload`)

Uploads the formatted data to both HDFS clusters (Simple-auth and Kerberized).

```bash
python main.py --steps upload
```

### 4. DDL Generation (Step: `ddl`)

Generates and optionally executes Dremio DDL statements for all tables.

```bash
python main.py --steps ddl
```

### 5. Cross-Cluster Setup (Step: `cross`)

Sets up cross-cluster access between Dremio instances.

```bash
python main.py --steps cross
```

### 6. Run Benchmarks (Step: `benchmark`)

Runs benchmark queries on both clusters and across clusters.

```bash
python main.py --steps benchmark
```

### 7. Generate Reports (Step: `report`)

Generates performance reports and visualizations.

```bash
python main.py --steps report
```

## Running Specific Steps

To run specific steps:

```bash
python main.py --steps data convert upload
```

To use a custom configuration file:

```bash
python main.py --config config/my_custom_config.yml --steps all
```

## Python-Native Command Execution

The codebase has been refactored to eliminate dependencies on shell scripts and subprocess-based command execution in favor of Python-native approaches:

1. **Added Python-Native Command Execution**: The `utils/command.py` module now includes a `PythonCommand` class that provides Python-native alternatives to common shell commands.

2. **HDFS Operations**: Added a Python-native HDFS client in `hdfs-upload/upload_to_hdfs.py` using libraries like `pyarrow.hdfs`, `pydoop.hdfs`, or `hdfs` instead of shell commands.

3. **TPC-DS Data Generation**: Refactored `data-generation/generate_tpcds_data.py` to use Python-native execution of the dsdgen tool using `ctypes` instead of subprocess.

### Benefits of This Approach

- **Cross-Platform Compatibility**: Better support for running on Windows, Linux, and macOS without shell-specific dependencies.
- **Better Error Handling**: More robust error handling with Python exceptions.
- **Enhanced Security**: Reduced risk of command injection vulnerabilities.
- **Improved Testing**: Easier to mock and test Python functions compared to shell commands.
- **Maintainability**: Unified Python codebase without mixed programming paradigms.

## Configuration Checklist

Below is a comprehensive checklist of configurations and prerequisites needed to run the benchmark scripts:

### Environment Setup

- [ ] Python 3.6+ installed
- [ ] Required Python packages installed: `pip install -r requirements.txt`
- [ ] Sufficient disk space for TPC-DS data generation
- [ ] Network access to Dremio and HDFS clusters

### TPC-DS Data Generation (generate_tpcds_data.py)

- [ ] TPC-DS toolkit compiled and available
- [ ] Path to dsdgen binary specified: `--dsdgen-path`
- [ ] Output directory defined: `--output-dir`
- [ ] Scale factors configured: `--scale-factors`

### HDFS Upload (upload_to_hdfs.py)

- [ ] Local data directory with generated data: `--data-dir`
- [ ] HDFS target directory: `--hdfs-target-dir`
- [ ] Scale factors to upload: `--scale-factors`
- [ ] Data formats to upload: `--formats`

#### Simple Authentication HDFS
- [ ] Path to Simple-auth Hadoop configuration: `--simple-auth-hadoop-conf`
- [ ] Simple-auth username: `--simple-auth-user`

#### Kerberos Authentication HDFS
- [ ] Path to Kerberized Hadoop configuration: `--kerberized-hadoop-conf`
- [ ] Path to keytab file: `--keytab`
- [ ] Kerberos principal: `--principal`

### Dremio DDL Generation (create_tables.py)

- [ ] Dremio host: `--dremio-host`
- [ ] Dremio port: `--dremio-port`
- [ ] Dremio username: `--dremio-username`
- [ ] Dremio password: `--dremio-password`
- [ ] HDFS base path: `--hdfs-base-path`
- [ ] SQL output directory: `--output-dir`
- [ ] Whether to execute DDL: `--execute`

### Benchmark Testing (run_benchmarks.py)

- [ ] Dremio host: `--host`
- [ ] Dremio port: `--port`
- [ ] Dremio username: `--username`
- [ ] Dremio password: `--password`
- [ ] Directory with SQL queries: `--query-dir`
- [ ] CSV output file: `--output`
- [ ] Concurrency level: `--concurrency`
- [ ] Number of iterations: `--iterations`

### Cross-Cluster Setup (setup_cross_cluster.py)

- [ ] Dremio A host, port, username, password
- [ ] Dremio B host, port, username, password
- [ ] Cross-cluster user credentials

## Sample Command Templates

```bash
# Generate TPC-DS data
python data-generation/generate_tpcds_data.py --dsdgen-path /path/to/dsdgen --output-dir ./data --scale-factors 1 10

# Upload to HDFS
python hdfs-upload/upload_to_hdfs.py --data-dir ./data/formatted --hdfs-target-dir /benchmark/tpcds \
  --simple-auth-hadoop-conf /path/to/hadoop/conf --simple-auth-user hdfs \
  --kerberized-hadoop-conf /path/to/kerb/conf --keytab /path/to/user.keytab --principal user@REALM

# Create Dremio tables
python dremio-ddls/create_tables.py --dremio-host localhost --dremio-port 9047 \
  --dremio-username admin --dremio-password password --hdfs-base-path /benchmark/tpcds \
  --output-dir ./sql --execute

# Run benchmarks
python test-automation/run_benchmarks.py --host localhost --port 9047 \
  --username admin --password password --query-dir ./queries --output results.csv
```

## Troubleshooting

If you encounter issues:

1. Check the log file: `benchmark_pipeline.log`
2. Verify your configuration settings
3. Ensure all required services (Dremio, HDFS) are accessible
4. For data generation issues, verify the TPC-DS toolkit is properly compiled

## Security Considerations

- Use environment variables for sensitive information like passwords and tokens
- Never commit the `.env` file to version control
- Consider using a credentials manager for production deployments
- Ensure proper access controls on keytab files and configuration directories