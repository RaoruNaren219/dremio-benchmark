# Dremio Cross-Cluster Benchmarking

This project provides a comprehensive framework for benchmarking Dremio clusters using TPC-DS data across multiple formats. It supports benchmarking between a Simple-auth HDFS cluster and a Kerberized HDFS cluster.

## Table of Contents

- [Project Overview](#project-overview)
- [Features](#features)
- [Directory Structure](#directory-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Pipeline Steps](#pipeline-steps)
- [Example Commands](#example-commands)
- [Output Reports](#output-reports)
- [Troubleshooting](#troubleshooting)

## Project Overview

This benchmarking framework is designed to evaluate the performance of Dremio clusters with different HDFS authentication methods (Simple-auth and Kerberos). It follows the official Dremio Benchmarking Methodology and includes cross-cluster query benchmarking.

## Features

- Generate TPC-DS datasets at 1GB and 10GB scale factors
- Convert raw data to multiple formats (CSV, JSON, Pipe-delimited, ORC, Parquet)
- Upload datasets to both HDFS clusters with structured directories
- Create Dremio-compatible SQL DDLs for all 24 TPC-DS tables across all formats
- Setup cross-cluster access between Dremio instances
- Run benchmarks on individual clusters and across clusters
- Generate comprehensive performance reports with visualizations

## Directory Structure

```
dremio_benchmark/
│
├── data-generation/       # Python scripts for generating and converting TPC-DS data
│   ├── generate_tpcds_data.py
│   └── convert_data_formats.py
│
├── hdfs-upload/           # Python script for uploading data to HDFS clusters
│   └── upload_to_hdfs.py
│
├── dremio-ddls/           # Scripts for generating and executing Dremio DDLs
│   └── create_tables.py
│
├── cross-cluster-setup/   # Scripts for setting up cross-cluster access
│   └── setup_cross_cluster.py
│
├── test-automation/       # Scripts for running benchmarks
│   ├── run_benchmarks.py
│   └── example_queries/
│       ├── query01.sql
│       └── cross_cluster_query.sql
│
├── reports/               # Scripts for generating reports
│   └── generate_report.py
│
└── main.py                # Main orchestration script
```

## Prerequisites

- Python 3.6 or higher
- PySpark
- Hadoop CLI
- dsdgen (TPC-DS data generator)
- Two Dremio clusters (one with Simple-auth HDFS, one with Kerberized HDFS)
- Network connectivity between the clusters

## Installation

1. Clone the repository:

```bash
git clone https://github.com/yourusername/dremio-benchmark.git
cd dremio-benchmark
```

2. Install Python dependencies:

```bash
pip install -r requirements.txt
```

3. Obtain and set up the TPC-DS toolkit:

```bash
# Download and build the TPC-DS toolkit
git clone https://github.com/gregrahn/tpcds-kit.git
cd tpcds-kit/tools
make
```

## Usage

The main script (`main.py`) orchestrates the entire benchmarking pipeline. You can run the entire pipeline or individual steps.

```bash
python main.py --steps all
```

Or run specific steps:

```bash
python main.py --steps data convert upload
```

## Pipeline Steps

1. **data**: Generate TPC-DS data at specified scale factors
2. **convert**: Convert raw data to various formats (CSV, JSON, Pipe-delimited, ORC, Parquet)
3. **upload**: Upload formatted data to both HDFS clusters
4. **ddl**: Generate and optionally execute Dremio DDL statements
5. **cross**: Set up cross-cluster access between Dremio instances
6. **benchmark**: Run benchmark queries on both clusters and across clusters
7. **report**: Generate performance reports and visualizations

## Example Commands

### Generate TPC-DS Data

```bash
python main.py --steps data --dsdgen-path /path/to/tpcds-kit/tools --scale-factors 1 10
```

### Run Full Pipeline

```bash
python main.py \
  --steps all \
  --dsdgen-path /path/to/tpcds-kit/tools \
  --simple-auth-hadoop-conf /path/to/simple/auth/hadoop/conf \
  --kerberized-hadoop-conf /path/to/kerberos/hadoop/conf \
  --keytab /path/to/keytab \
  --principal username@REALM.COM \
  --dremio-a-host dremio-a-host \
  --dremio-a-username admin \
  --dremio-a-password password \
  --dremio-b-host dremio-b-host \
  --dremio-b-username admin \
  --dremio-b-password password \
  --cross-password cross-cluster-password \
  --concurrency 4 \
  --iterations 3 \
  --execute-ddl
```

### Run Benchmarks Only

```bash
python main.py \
  --steps benchmark \
  --dremio-a-host dremio-a-host \
  --dremio-a-username admin \
  --dremio-a-password password \
  --dremio-b-host dremio-b-host \
  --dremio-b-username admin \
  --dremio-b-password password \
  --concurrency 4 \
  --iterations 3
```

## Output Reports

After running the benchmarking pipeline, reports will be generated in the `./pipeline/reports` directory (or the directory specified with `--base-dir`). The reports include:

- HTML report with summary statistics and visualizations
- CSV files with detailed performance metrics
- Performance charts for execution time, memory usage, CPU usage, and success rate
- Comparison charts between clusters

## Troubleshooting

### Common Issues

1. **TPC-DS Data Generation Failures**: Ensure the dsdgen binary is compiled correctly and the path is correct.
2. **HDFS Upload Failures**: Verify Hadoop configurations and permissions.
3. **Dremio Authentication Failures**: Check Dremio credentials and ensure the Dremio API is accessible.
4. **Cross-Cluster Access Issues**: Ensure network connectivity between clusters and proper credentials.

### Logging

The benchmark pipeline creates detailed logs in the following files:

- `benchmark_pipeline.log`: Main pipeline log
- `data_generation.log`: Data generation log
- `data_conversion.log`: Data conversion log
- `hdfs_upload.log`: HDFS upload log
- `ddl_generation.log`: DDL generation log
- `cross_cluster_setup.log`: Cross-cluster setup log
- `benchmark.log`: Benchmarking log
- `report_generation.log`: Report generation log

Check these logs for detailed error information if any step fails. 