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

1. Clone the repository:

```bash
git clone https://github.com/RaoruNaren219/dremio-benchmark.git
cd dremio-benchmark
```

2. Install Python dependencies:

```bash
pip install -r requirements.txt
```

Alternatively, you can install the package:

```bash
pip install -e .
```

3. Copy and configure environment files:

```bash
cp .env.sample .env
cp config/default_config.yml.sample config/default_config.yml
```

4. Edit `.env` and `config/default_config.yml` with your specific settings.

5. Obtain and set up the TPC-DS toolkit:

```bash
# Download and build the TPC-DS toolkit
git clone https://github.com/gregrahn/tpcds-kit.git
cd tpcds-kit/tools
make
```

## Configuration

Create or modify the configuration file in `config/default_config.yml` with your specific settings. You can also create a custom configuration file and specify it with the `--config` option.

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

## Usage

Run the entire pipeline:

```bash
python main.py --config config/default_config.yml --steps all
```

Run specific steps:

```bash
python main.py --steps data convert upload
```

Run with a custom configuration:

```bash
python main.py --config config/my_custom_config.yml
```

## Pipeline Steps

1. **data**: Generate TPC-DS data at specified scale factors
2. **convert**: Convert raw data to various formats
3. **upload**: Upload formatted data to both HDFS clusters
4. **ddl**: Generate and optionally execute Dremio DDL statements
5. **cross**: Set up cross-cluster access between Dremio instances
6. **benchmark**: Run benchmark queries on both clusters and across clusters
7. **report**: Generate performance reports and visualizations

## Security Considerations

- Use environment variables for sensitive information like passwords and tokens
- Never commit the `.env` file to version control
- Consider using a credentials manager for production deployments
- Ensure proper access controls on keytab files and configuration directories