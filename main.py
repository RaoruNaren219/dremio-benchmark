#!/usr/bin/env python3

"""
Dremio Benchmark Pipeline
This script orchestrates the entire benchmarking pipeline for cross-cluster Dremio benchmark testing
"""

import argparse
import sys
import os
import time
from typing import List, Dict, Optional
from datetime import datetime

# Import utility modules
from utils.logging_config import setup_logging
from utils.command import run_command
from utils.config import load_config, get_cluster_config, get_hdfs_config, merge_configs
from utils.filesystem import create_directories
from utils.constants import (
    TPC_DS_TABLES, 
    DATA_FORMATS, 
    DEFAULT_SCALE_FACTORS,
    PIPELINE_STEPS,
    DEFAULT_DIRECTORIES
)

# Set up logging
logger = setup_logging(
    log_file="benchmark_pipeline.log",
    module_name="dremio_benchmark.main"
)

def run_data_generation_step(config: Dict, dirs: Dict[str, str]) -> bool:
    """
    Run the data generation step
    
    Args:
        config (Dict): Configuration dictionary
        dirs (Dict[str, str]): Directory paths
    
    Returns:
        bool: True if successful, False otherwise
    """
    data_gen_config = config.get("data_generation", {})
    dsdgen_path = data_gen_config.get("dsdgen_path")
    
    if not dsdgen_path:
        logger.error("dsdgen_path not specified in configuration")
        return False
    
    pipeline_config = config.get("pipeline", {})
    scale_factors = pipeline_config.get("scale_factors", DEFAULT_SCALE_FACTORS)
    
    cmd = [
        "python", os.path.join("data-generation", "generate_tpcds_data.py"),
        "--dsdgen-path", dsdgen_path,
        "--output-dir", dirs["data"]
    ]
    
    # Add scale factors if specified
    if scale_factors:
        cmd.extend(["--scale-factors"] + [str(sf) for sf in scale_factors])
    
    success, stdout, stderr = run_command(cmd, "Data generation")
    return success

def run_data_conversion_step(config: Dict, dirs: Dict[str, str]) -> bool:
    """
    Run the data conversion step
    
    Args:
        config (Dict): Configuration dictionary
        dirs (Dict[str, str]): Directory paths
    
    Returns:
        bool: True if successful, False otherwise
    """
    cmd = [
        "python", os.path.join("data-generation", "convert_data_formats.py"),
        "--input-dir", dirs["data"],
        "--output-dir", dirs["formatted_data"]
    ]
    
    pipeline_config = config.get("pipeline", {})
    scale_factors = pipeline_config.get("scale_factors", DEFAULT_SCALE_FACTORS)
    formats = pipeline_config.get("formats", DATA_FORMATS)
    
    # Add scale factors and formats if specified
    if scale_factors:
        cmd.extend(["--scale-factors"] + [str(sf) for sf in scale_factors])
    
    if formats:
        cmd.extend(["--formats"] + formats)
    
    success, stdout, stderr = run_command(cmd, "Data conversion")
    return success

def run_hdfs_upload_step(config: Dict, dirs: Dict[str, str]) -> bool:
    """
    Run the HDFS upload step
    
    Args:
        config (Dict): Configuration dictionary
        dirs (Dict[str, str]): Directory paths
    
    Returns:
        bool: True if successful, False otherwise
    """
    pipeline_config = config.get("pipeline", {})
    hdfs_target_dir = pipeline_config.get("hdfs_target_dir", "/benchmark/tpcds")
    scale_factors = pipeline_config.get("scale_factors", DEFAULT_SCALE_FACTORS)
    formats = pipeline_config.get("formats", DATA_FORMATS)
    
    # Get HDFS configurations
    simple_auth_hdfs = get_hdfs_config(config, "simple_auth")
    kerberized_hdfs = get_hdfs_config(config, "kerberized")
    
    if not simple_auth_hdfs.get("hadoop_conf"):
        logger.error("simple_auth.hadoop_conf not specified in configuration")
        return False
    
    if not kerberized_hdfs.get("hadoop_conf"):
        logger.error("kerberized.hadoop_conf not specified in configuration")
        return False
    
    if not kerberized_hdfs.get("keytab"):
        logger.error("kerberized.keytab not specified in configuration")
        return False
    
    if not kerberized_hdfs.get("principal"):
        logger.error("kerberized.principal not specified in configuration")
        return False
    
    cmd = [
        "python", os.path.join("hdfs-upload", "upload_to_hdfs.py"),
        "--data-dir", dirs["formatted_data"],
        "--hdfs-target-dir", hdfs_target_dir,
        "--simple-auth-hadoop-bin", simple_auth_hdfs.get("hadoop_bin", "hadoop"),
        "--simple-auth-hadoop-conf", simple_auth_hdfs.get("hadoop_conf"),
        "--simple-auth-user", simple_auth_hdfs.get("user", "hdfs"),
        "--kerberized-hadoop-bin", kerberized_hdfs.get("hadoop_bin", "hadoop"),
        "--kerberized-hadoop-conf", kerberized_hdfs.get("hadoop_conf"),
        "--keytab", kerberized_hdfs.get("keytab"),
        "--principal", kerberized_hdfs.get("principal")
    ]
    
    # Add scale factors if specified
    if scale_factors:
        cmd.extend(["--scale-factors"] + [str(sf) for sf in scale_factors])
    
    # Add formats if specified
    if formats:
        cmd.extend(["--formats"] + formats)
    
    success, stdout, stderr = run_command(cmd, "HDFS upload")
    return success

def run_ddl_generation_step(config: Dict, dirs: Dict[str, str]) -> bool:
    """
    Run the DDL generation step
    
    Args:
        config (Dict): Configuration dictionary
        dirs (Dict[str, str]): Directory paths
    
    Returns:
        bool: True if successful, False otherwise
    """
    pipeline_config = config.get("pipeline", {})
    hdfs_target_dir = pipeline_config.get("hdfs_target_dir", "/benchmark/tpcds")
    scale_factors = pipeline_config.get("scale_factors", DEFAULT_SCALE_FACTORS)
    formats = pipeline_config.get("formats", DATA_FORMATS)
    
    # Get Dremio configurations
    dremio_a = get_cluster_config(config, "dremio_a")
    dremio_b = get_cluster_config(config, "dremio_b")
    
    if not dremio_a.get("host"):
        logger.error("dremio_a.host not specified in configuration")
        return False
    
    if not dremio_b.get("host"):
        logger.error("dremio_b.host not specified in configuration")
        return False
    
    # DDL generation for Dremio A
    cmd_a = [
        "python", os.path.join("dremio-ddls", "create_tables.py"),
        "--dremio-host", dremio_a.get("host"),
        "--dremio-port", str(dremio_a.get("port", 9047)),
        "--dremio-username", dremio_a.get("username", "admin"),
        "--dremio-password", dremio_a.get("password", ""),
        "--hdfs-base-path", hdfs_target_dir,
        "--output-dir", os.path.join(dirs["logs"], "ddl_a")
    ]
    
    # Add --execute if requested
    if config.get("execute_ddl", False):
        cmd_a.append("--execute")
    
    # Add --no-ssl if specified
    if not dremio_a.get("ssl", True):
        cmd_a.append("--no-ssl")
    
    # Add scale factors if specified
    if scale_factors:
        cmd_a.extend(["--scale-factors"] + [str(sf) for sf in scale_factors])
    
    # Add formats if specified
    if formats:
        cmd_a.extend(["--formats"] + formats)
    
    success_a, stdout_a, stderr_a = run_command(cmd_a, "DDL generation for Dremio A")
    
    # DDL generation for Dremio B
    cmd_b = [
        "python", os.path.join("dremio-ddls", "create_tables.py"),
        "--dremio-host", dremio_b.get("host"),
        "--dremio-port", str(dremio_b.get("port", 9047)),
        "--dremio-username", dremio_b.get("username", "admin"),
        "--dremio-password", dremio_b.get("password", ""),
        "--hdfs-base-path", hdfs_target_dir,
        "--output-dir", os.path.join(dirs["logs"], "ddl_b")
    ]
    
    # Add --execute if requested
    if config.get("execute_ddl", False):
        cmd_b.append("--execute")
    
    # Add --no-ssl if specified
    if not dremio_b.get("ssl", True):
        cmd_b.append("--no-ssl")
    
    # Add scale factors if specified
    if scale_factors:
        cmd_b.extend(["--scale-factors"] + [str(sf) for sf in scale_factors])
    
    # Add formats if specified
    if formats:
        cmd_b.extend(["--formats"] + formats)
    
    success_b, stdout_b, stderr_b = run_command(cmd_b, "DDL generation for Dremio B")
    
    return success_a and success_b

def run_cross_cluster_setup_step(config: Dict, dirs: Dict[str, str]) -> bool:
    """
    Run the cross-cluster setup step
    
    Args:
        config (Dict): Configuration dictionary
        dirs (Dict[str, str]): Directory paths
    
    Returns:
        bool: True if successful, False otherwise
    """
    # Get Dremio configurations
    dremio_a = get_cluster_config(config, "dremio_a")
    dremio_b = get_cluster_config(config, "dremio_b")
    
    # Get cross-cluster configuration
    cross_cluster = config.get("cross_cluster", {})
    
    if not dremio_a.get("host"):
        logger.error("dremio_a.host not specified in configuration")
        return False
    
    if not dremio_b.get("host"):
        logger.error("dremio_b.host not specified in configuration")
        return False
    
    if not cross_cluster.get("password"):
        logger.error("cross_cluster.password not specified in configuration")
        return False
    
    cmd = [
        "python", os.path.join("cross-cluster-setup", "setup_cross_cluster.py"),
        "--dremio-a-host", dremio_a.get("host"),
        "--dremio-a-port", str(dremio_a.get("port", 9047)),
        "--dremio-a-username", dremio_a.get("username", "admin"),
        "--dremio-a-password", dremio_a.get("password", ""),
        "--dremio-b-host", dremio_b.get("host"),
        "--dremio-b-port", str(dremio_b.get("port", 9047)),
        "--dremio-b-username", dremio_b.get("username", "admin"),
        "--dremio-b-password", dremio_b.get("password", ""),
        "--cross-password", cross_cluster.get("password")
    ]
    
    # Add cross-user if specified
    if cross_cluster.get("user"):
        cmd.extend(["--cross-user", cross_cluster.get("user")])
    
    # Add --no-ssl if specified
    if not dremio_a.get("ssl", True) or not dremio_b.get("ssl", True):
        cmd.append("--no-ssl")
    
    success, stdout, stderr = run_command(cmd, "Cross-cluster setup")
    return success

def run_benchmark_step(config: Dict, dirs: Dict[str, str]) -> bool:
    """
    Run the benchmarking step
    
    Args:
        config (Dict): Configuration dictionary
        dirs (Dict[str, str]): Directory paths
    
    Returns:
        bool: True if successful, False otherwise
    """
    # Get Dremio configurations
    dremio_a = get_cluster_config(config, "dremio_a")
    dremio_b = get_cluster_config(config, "dremio_b")
    
    # Get benchmark configuration
    benchmark_config = config.get("benchmark", {})
    concurrency = benchmark_config.get("concurrency", 1)
    iterations = benchmark_config.get("iterations", 1)
    query_dir = benchmark_config.get("query_dir", "test-automation/example_queries")
    
    if not dremio_a.get("host"):
        logger.error("dremio_a.host not specified in configuration")
        return False
    
    if not dremio_b.get("host"):
        logger.error("dremio_b.host not specified in configuration")
        return False
    
    # Run benchmarks on Dremio A
    cmd_a = [
        "python", os.path.join("test-automation", "run_benchmarks.py"),
        "--host", dremio_a.get("host"),
        "--port", str(dremio_a.get("port", 9047)),
        "--username", dremio_a.get("username", "admin"),
        "--password", dremio_a.get("password", ""),
        "--query-dir", query_dir,
        "--output", os.path.join(dirs["results"], "dremio_a_results.csv"),
        "--concurrency", str(concurrency),
        "--iterations", str(iterations)
    ]
    
    # Add --no-ssl if specified
    if not dremio_a.get("ssl", True):
        cmd_a.append("--no-ssl")
    
    success_a, stdout_a, stderr_a = run_command(cmd_a, "Benchmarking on Dremio A")
    
    # Run benchmarks on Dremio B
    cmd_b = [
        "python", os.path.join("test-automation", "run_benchmarks.py"),
        "--host", dremio_b.get("host"),
        "--port", str(dremio_b.get("port", 9047)),
        "--username", dremio_b.get("username", "admin"),
        "--password", dremio_b.get("password", ""),
        "--query-dir", query_dir,
        "--output", os.path.join(dirs["results"], "dremio_b_results.csv"),
        "--concurrency", str(concurrency),
        "--iterations", str(iterations)
    ]
    
    # Add --no-ssl if specified
    if not dremio_b.get("ssl", True):
        cmd_b.append("--no-ssl")
    
    success_b, stdout_b, stderr_b = run_command(cmd_b, "Benchmarking on Dremio B")
    
    # Run cross-cluster benchmark
    cmd_cross = [
        "python", os.path.join("test-automation", "run_benchmarks.py"),
        "--host", dremio_a.get("host"),
        "--port", str(dremio_a.get("port", 9047)),
        "--username", dremio_a.get("username", "admin"),
        "--password", dremio_a.get("password", ""),
        "--query-dir", os.path.join(query_dir, "cross_cluster"),
        "--output", os.path.join(dirs["results"], "cross_cluster_results.csv"),
        "--concurrency", str(concurrency),
        "--iterations", str(iterations)
    ]
    
    # Add --no-ssl if specified
    if not dremio_a.get("ssl", True):
        cmd_cross.append("--no-ssl")
    
    success_cross, stdout_cross, stderr_cross = run_command(cmd_cross, "Cross-cluster benchmarking")
    
    return success_a and success_b and success_cross

def run_report_generation_step(config: Dict, dirs: Dict[str, str]) -> bool:
    """
    Run the report generation step
    
    Args:
        config (Dict): Configuration dictionary
        dirs (Dict[str, str]): Directory paths
    
    Returns:
        bool: True if successful, False otherwise
    """
    # Get report configuration
    report_config = config.get("report", {})
    title = report_config.get("title", "Dremio Cross-Cluster Benchmark Report")
    include_charts = report_config.get("include_charts", True)
    
    cmd = [
        "python", os.path.join("reports", "generate_report.py"),
        "--results-a", os.path.join(dirs["results"], "dremio_a_results.csv"),
        "--results-b", os.path.join(dirs["results"], "dremio_b_results.csv"),
        "--results-cross", os.path.join(dirs["results"], "cross_cluster_results.csv"),
        "--output-dir", dirs["reports"],
        "--title", title
    ]
    
    # Add --include-charts if specified
    if include_charts:
        cmd.append("--include-charts")
    
    success, stdout, stderr = run_command(cmd, "Report generation")
    return success

def main():
    """Main function to orchestrate the benchmark pipeline"""
    parser = argparse.ArgumentParser(description="Run the Dremio benchmark pipeline")
    
    # Configuration option
    parser.add_argument("--config", default="config/default_config.yml", 
                        help="Path to configuration file")
    
    # Pipeline options
    parser.add_argument("--steps", nargs="+", choices=["all"] + PIPELINE_STEPS,
                        default=["all"], help="Pipeline steps to run")
    
    # Override options
    parser.add_argument("--base-dir", help="Override base directory for the pipeline")
    parser.add_argument("--execute-ddl", action="store_true", 
                        help="Execute DDL statements in Dremio")
    
    args = parser.parse_args()
    
    # Load configuration
    logger.info(f"Loading configuration from {args.config}")
    config = load_config(args.config)
    
    if not config:
        logger.error(f"Failed to load configuration from {args.config}")
        sys.exit(1)
    
    # Override configuration with command-line arguments
    if args.base_dir:
        if "pipeline" not in config:
            config["pipeline"] = {}
        config["pipeline"]["base_dir"] = args.base_dir
    
    if args.execute_ddl:
        config["execute_ddl"] = True
    
    # Get pipeline configuration
    pipeline_config = config.get("pipeline", {})
    base_dir = pipeline_config.get("base_dir", "./pipeline")
    
    # Resolve pipeline steps
    steps = args.steps
    if "all" in steps:
        steps = PIPELINE_STEPS.copy()
    
    # Create pipeline directories
    dirs = create_directories(base_dir, DEFAULT_DIRECTORIES)
    
    # Run pipeline steps
    for step in steps:
        if step == "data":
            if not run_data_generation_step(config, dirs):
                logger.error("Data generation step failed")
                sys.exit(1)
        
        elif step == "convert":
            if not run_data_conversion_step(config, dirs):
                logger.error("Data conversion step failed")
                sys.exit(1)
        
        elif step == "upload":
            if not run_hdfs_upload_step(config, dirs):
                logger.error("HDFS upload step failed")
                sys.exit(1)
        
        elif step == "ddl":
            if not run_ddl_generation_step(config, dirs):
                logger.error("DDL generation step failed")
                sys.exit(1)
        
        elif step == "cross":
            if not run_cross_cluster_setup_step(config, dirs):
                logger.error("Cross-cluster setup step failed")
                sys.exit(1)
        
        elif step == "benchmark":
            if not run_benchmark_step(config, dirs):
                logger.error("Benchmarking step failed")
                sys.exit(1)
        
        elif step == "report":
            if not run_report_generation_step(config, dirs):
                logger.error("Report generation step failed")
                sys.exit(1)
    
    logger.info("Dremio benchmark pipeline completed successfully!")
    logger.info(f"Results and reports are available in the {base_dir} directory")

if __name__ == "__main__":
    main() 