# Dremio Data Sharing Project

This project provides tools for generating test data in various formats and ingesting it into Dremio for cross-cluster data sharing.

## Requirements

- Python 3.8.17 (64-bit)
- WSL with RHEL 8 (recommended)
- At least 2GB of available memory
- At least 10GB of free disk space

## Installation

### Option 1: Using Conda (Recommended)

1. Install Miniconda or Anaconda if you don't have it already.

2. Create and activate the conda environment:
```bash
conda env create -f environment.yml
conda activate dremio-data-sharing
```

### Option 2: Using pip

1. Create and activate a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On Linux/WSL
# OR
.\venv\Scripts\activate  # On Windows
```

2. Upgrade pip and install build tools:
```bash
python -m pip install --upgrade pip setuptools wheel
```

3. Install the required packages:
```bash
pip install -r requirements.txt
```

## Configuration

1. Create a `.env` file in the project root directory with your Dremio credentials:
```
SOURCE_DREMIO_URL=http://your-source-dremio:9047
TARGET_DREMIO_URL=http://your-target-dremio:9047
DREMIO_USERNAME=your_username
DREMIO_PASSWORD=your_password
```

## Usage

### Generating Test Data

Run the data generation script to create test data in various formats:
```bash
python generate_test_data_formats.py
```

This will generate the following files in the `test_data` directory:
- `test_data.csv` - CSV format
- `test_data.txt` - Tab-separated text format
- `test_data.parquet` - Parquet format
- `test_data.orc` - ORC format

### Ingesting Data into Dremio

Run the ingestion script to upload the generated data to Dremio:
```bash
python ingest_test_data.py
```

This will:
1. Validate the file formats
2. Check file integrity
3. Upload the data to the specified Dremio instance
4. Record performance metrics

## Performance Monitoring

The scripts include built-in performance monitoring that tracks:
- File sizes
- Generation/ingestion times
- Memory usage

Performance metrics are logged to:
- `data_generation.log` - For data generation
- `data_ingestion.log` - For data ingestion

## Troubleshooting

### Memory Issues

If you encounter memory-related errors:
1. Reduce the chunk size in the `DataGenerator` class
2. Close other memory-intensive applications
3. Increase your system's available memory

### Build Errors

If you encounter build errors with `pyorc`:
1. Ensure you have the required build tools installed
2. Try installing the pre-built wheel if available
3. Check that you're using Python 3.8.17 64-bit

### Connection Issues

If you have trouble connecting to Dremio:
1. Verify your credentials in the `.env` file
2. Check that the Dremio server is running and accessible
3. Ensure your network allows connections to the Dremio ports

## License

This project is licensed under the MIT License - see the LICENSE file for details.