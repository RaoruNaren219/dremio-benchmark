#!/usr/bin/env python3

"""
Dremio Benchmark Pipeline
This script orchestrates the entire benchmarking pipeline for cross-cluster Dremio benchmark testing
"""

import argparse
import sys
import os
import time
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime

# Import utility modules
from utils.logging_config import setup_logging
from utils.command import run_command
from utils.config import Config
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

class BenchmarkPipeline:
    """
    Dremio Benchmark Pipeline orchestrator
    
    This class manages the execution of the benchmark pipeline steps.
    """
    
    def __init__(self, config_file: str):
        """
        Initialize the benchmark pipeline
        
        Args:
            config_file: Path to configuration file
        """
        self.config = Config(config_file)
        self.dirs = {}
        self.step_results = {}
    
    def initialize(self) -> bool:
        """
        Initialize the pipeline by creating required directories
        
        Returns:
            True if successful, False otherwise
        """
        logger.info("Initializing benchmark pipeline...")
        
        # Create directories
        self.dirs = create_directories(DEFAULT_DIRECTORIES)
        if not self.dirs:
            logger.error("Failed to create required directories")
            return False
        
        logger.info("Benchmark pipeline initialized successfully")
        return True
    
    def run_step(self, step: str) -> bool:
        """
        Run a specific pipeline step
        
        Args:
            step: Pipeline step to run
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Running step: {step}")
        
        # Validate configuration for this step
        is_valid, error_msgs = self.config.validate(step)
        if not is_valid:
            for error in error_msgs:
                logger.error(f"Configuration error: {error}")
            return False
        
        # Run the appropriate step
        success = False
        if step == "data":
            success = self._run_data_generation_step()
        elif step == "convert":
            success = self._run_data_conversion_step()
        elif step == "upload":
            success = self._run_hdfs_upload_step()
        elif step == "ddl":
            success = self._run_ddl_generation_step()
        elif step == "cross":
            success = self._run_cross_cluster_setup_step()
        elif step == "benchmark":
            success = self._run_benchmark_step()
        elif step == "report":
            success = self._run_report_generation_step()
        else:
            logger.error(f"Unknown step: {step}")
            return False
        
        # Record the result
        self.step_results[step] = success
        
        if success:
            logger.info(f"Step {step} completed successfully")
        else:
            logger.error(f"Step {step} failed")
        
        return success
    
    def run_pipeline(self, steps: List[str]) -> bool:
        """
        Run the specified pipeline steps
        
        Args:
            steps: List of steps to run, or ["all"] for all steps
            
        Returns:
            True if all steps were successful, False otherwise
        """
        if not self.initialize():
            return False
        
        # Determine which steps to run
        pipeline_steps = steps
        if "all" in steps:
            pipeline_steps = PIPELINE_STEPS
        
        # Run each step
        success = True
        for step in pipeline_steps:
            step_success = self.run_step(step)
            if not step_success:
                success = False
                if self.config.get("stop_on_error", True):
                    logger.error(f"Stopping pipeline execution due to failure in step {step}")
                    break
        
        # Log summary
        self._log_summary()
        
        return success
    
    def _run_data_generation_step(self) -> bool:
        """
        Run the data generation step
        
        Returns:
            True if successful, False otherwise
        """
        dsdgen_path = self.config.get("data_generation.dsdgen_path")
        if not dsdgen_path:
            logger.error("dsdgen_path not specified in configuration")
            return False
        
        scale_factors = self.config.get("pipeline.scale_factors", DEFAULT_SCALE_FACTORS)
        
        cmd = [
            "python", os.path.join("data-generation", "generate_tpcds_data.py"),
            "--dsdgen-path", dsdgen_path,
            "--output-dir", self.dirs["data"]
        ]
        
        # Add scale factors if specified
        if scale_factors:
            cmd.extend(["--scale-factors"] + [str(sf) for sf in scale_factors])
        
        success, stdout, stderr = run_command(cmd, "Data generation")
        return success
    
    def _run_data_conversion_step(self) -> bool:
        """
        Run the data conversion step
        
        Returns:
            True if successful, False otherwise
        """
        cmd = [
            "python", os.path.join("data-generation", "convert_data_formats.py"),
            "--input-dir", self.dirs["data"],
            "--output-dir", self.dirs["formatted_data"]
        ]
        
        scale_factors = self.config.get("pipeline.scale_factors", DEFAULT_SCALE_FACTORS)
        formats = self.config.get("pipeline.formats", DATA_FORMATS)
        
        # Add scale factors and formats if specified
        if scale_factors:
            cmd.extend(["--scale-factors"] + [str(sf) for sf in scale_factors])
        
        if formats:
            cmd.extend(["--formats"] + formats)
        
        success, stdout, stderr = run_command(cmd, "Data conversion")
        return success
    
    def _run_hdfs_upload_step(self) -> bool:
        """
        Run the HDFS upload step
        
        Returns:
            True if successful, False otherwise
        """
        hdfs_target_dir = self.config.get("pipeline.hdfs_target_dir", "/benchmark/tpcds")
        scale_factors = self.config.get("pipeline.scale_factors", DEFAULT_SCALE_FACTORS)
        formats = self.config.get("pipeline.formats", DATA_FORMATS)
        
        # Get HDFS configurations
        simple_auth_hdfs = self.config.get_hdfs_config("simple_auth")
        kerberized_hdfs = self.config.get_hdfs_config("kerberized")
        
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
            "--data-dir", self.dirs["formatted_data"],
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
    
    def _run_ddl_generation_step(self) -> bool:
        """
        Run the DDL generation step
        
        Returns:
            True if successful, False otherwise
        """
        hdfs_target_dir = self.config.get("pipeline.hdfs_target_dir", "/benchmark/tpcds")
        scale_factors = self.config.get("pipeline.scale_factors", DEFAULT_SCALE_FACTORS)
        formats = self.config.get("pipeline.formats", DATA_FORMATS)
        
        # Get Dremio configurations
        dremio_a = self.config.get_cluster_config("dremio_a")
        dremio_b = self.config.get_cluster_config("dremio_b")
        
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
            "--output-dir", os.path.join(self.dirs["logs"], "ddl_a")
        ]
        
        # Add --execute if requested
        if self.config.get("execute_ddl", False):
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
            "--output-dir", os.path.join(self.dirs["logs"], "ddl_b")
        ]
        
        # Add --execute if requested
        if self.config.get("execute_ddl", False):
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
    
    def _run_cross_cluster_setup_step(self) -> bool:
        """
        Run the cross-cluster setup step
        
        Returns:
            True if successful, False otherwise
        """
        dremio_a = self.config.get_cluster_config("dremio_a")
        dremio_b = self.config.get_cluster_config("dremio_b")
        
        cmd = [
            "python", os.path.join("cross-cluster-setup", "setup_cross_cluster.py"),
            "--source-host", dremio_a.get("host"),
            "--source-port", str(dremio_a.get("port", 9047)),
            "--source-username", dremio_a.get("username", "admin"),
            "--source-password", dremio_a.get("password", ""),
            "--target-host", dremio_b.get("host"),
            "--target-port", str(dremio_b.get("port", 9047)),
            "--target-username", dremio_b.get("username", "admin"),
            "--target-password", dremio_b.get("password", ""),
            "--output-dir", os.path.join(self.dirs["logs"], "cross_cluster")
        ]
        
        # Add --no-ssl if specified
        if not dremio_a.get("ssl", True):
            cmd.append("--source-no-ssl")
        
        if not dremio_b.get("ssl", True):
            cmd.append("--target-no-ssl")
        
        success, stdout, stderr = run_command(cmd, "Cross-cluster setup")
        return success
    
    def _run_benchmark_step(self) -> bool:
        """
        Run the benchmark step
        
        Returns:
            True if successful, False otherwise
        """
        dremio_a = self.config.get_cluster_config("dremio_a")
        dremio_b = self.config.get_cluster_config("dremio_b")
        
        query_dir = self.config.get("pipeline.query_dir")
        if not query_dir:
            logger.error("pipeline.query_dir not specified in configuration")
            return False
        
        timeout_seconds = self.config.get("pipeline.timeout_seconds", 600)
        num_iterations = self.config.get("pipeline.num_iterations", 3)
        
        cmd = [
            "python", os.path.join("test-automation", "run_benchmarks.py"),
            "--source-host", dremio_a.get("host"),
            "--source-port", str(dremio_a.get("port", 9047)),
            "--source-username", dremio_a.get("username", "admin"),
            "--source-password", dremio_a.get("password", ""),
            "--target-host", dremio_b.get("host"),
            "--target-port", str(dremio_b.get("port", 9047)),
            "--target-username", dremio_b.get("username", "admin"),
            "--target-password", dremio_b.get("password", ""),
            "--query-dir", query_dir,
            "--timeout", str(timeout_seconds),
            "--iterations", str(num_iterations),
            "--output-dir", os.path.join(self.dirs["logs"], "benchmark")
        ]
        
        # Add --no-ssl if specified
        if not dremio_a.get("ssl", True):
            cmd.append("--source-no-ssl")
        
        if not dremio_b.get("ssl", True):
            cmd.append("--target-no-ssl")
        
        success, stdout, stderr = run_command(cmd, "Benchmark execution")
        return success
    
    def _run_report_generation_step(self) -> bool:
        """
        Run the report generation step
        
        Returns:
            True if successful, False otherwise
        """
        benchmark_dir = os.path.join(self.dirs["logs"], "benchmark")
        output_dir = self.config.get("reports.output_dir", os.path.join(self.dirs["reports"], "output"))
        
        # Create reports output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        cmd = [
            "python", os.path.join("reports", "generate_report.py"),
            "--input-dir", benchmark_dir,
            "--output-dir", output_dir
        ]
        
        # Add report format if specified
        report_format = self.config.get("reports.format", "html")
        if report_format:
            cmd.extend(["--format", report_format])
        
        # Add --no-charts if charts are disabled
        if not self.config.get("reports.generate_charts", True):
            cmd.append("--no-charts")
        
        success, stdout, stderr = run_command(cmd, "Report generation")
        return success
    
    def _log_summary(self) -> None:
        """Log a summary of the pipeline execution"""
        logger.info("Pipeline execution summary:")
        for step, result in self.step_results.items():
            status = "SUCCESS" if result else "FAILURE"
            logger.info(f"  {step}: {status}")


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments
    
    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(description="Dremio Benchmark Pipeline")
    parser.add_argument(
        "--config", 
        default="config/default_config.yml", 
        help="Path to configuration file"
    )
    parser.add_argument(
        "--steps", 
        nargs="+", 
        default=["all"],
        choices=PIPELINE_STEPS + ["all"],
        help=f"Pipeline steps to run (default: all). Available steps: {', '.join(PIPELINE_STEPS)}, all"
    )
    
    return parser.parse_args()


def main() -> int:
    """
    Main function
    
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    args = parse_args()
    
    try:
        pipeline = BenchmarkPipeline(args.config)
        success = pipeline.run_pipeline(args.steps)
        
        return 0 if success else 1
    
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main()) 