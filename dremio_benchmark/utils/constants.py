"""
Constants used throughout the Dremio benchmark pipeline
"""

# TPC-DS Tables
TPC_DS_TABLES = [
    "call_center", "catalog_page", "catalog_returns", "catalog_sales",
    "customer", "customer_address", "customer_demographics", "date_dim",
    "household_demographics", "income_band", "inventory", "item",
    "promotion", "reason", "ship_mode", "store", "store_returns",
    "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
    "web_sales", "web_site"
]

# Data formats
DATA_FORMATS = ["csv", "json", "pipe", "orc", "parquet"]

# Default scale factors (in GB)
DEFAULT_SCALE_FACTORS = [1, 10]

# Pipeline steps
PIPELINE_STEPS = ["data", "convert", "upload", "ddl", "cross", "benchmark", "report"]

# Directory structure
DEFAULT_DIRECTORIES = {
    "data": "data",
    "formatted_data": "data/formatted",
    "logs": "logs",
    "results": "results", 
    "reports": "reports"
} 