import os
import json
import time
from datetime import datetime
from typing import Dict, List, Optional
from loguru import logger
import pandas as pd
from pathlib import Path

class MetricsCollector:
    def __init__(self, metrics_dir: str = "metrics"):
        """
        Initialize the metrics collector.
        
        Args:
            metrics_dir (str): Directory to store metrics
        """
        self.metrics_dir = Path(metrics_dir)
        self.metrics_dir.mkdir(exist_ok=True)
        self.current_metrics = {}

    def record_ingestion_metrics(self, 
                               file_format: str,
                               file_size: int,
                               ingestion_time: float,
                               success: bool,
                               data_lake: str = "dremio",
                               additional_metrics: Optional[Dict] = None) -> None:
        """
        Record metrics for a file ingestion.
        
        Args:
            file_format (str): Format of the ingested file
            file_size (int): Size of the file in GB
            ingestion_time (float): Time taken for ingestion in seconds
            success (bool): Whether ingestion was successful
            data_lake (str): Name of the data lake (dremio, hdfs, s3, adls)
            additional_metrics (Dict): Additional metrics to record
        """
        timestamp = datetime.now().isoformat()
        metrics = {
            "timestamp": timestamp,
            "file_format": file_format,
            "file_size_gb": file_size,
            "ingestion_time_seconds": ingestion_time,
            "success": success,
            "data_lake": data_lake,
            "metrics_version": "1.0"
        }
        
        if additional_metrics:
            metrics.update(additional_metrics)
            
        # Store metrics in JSON format
        metrics_file = self.metrics_dir / f"ingestion_metrics_{timestamp}.json"
        with open(metrics_file, 'w') as f:
            json.dump(metrics, f, indent=2)
            
        # Update current metrics for dashboard
        self.current_metrics = metrics
        
        logger.info(f"Recorded metrics for {file_format} {file_size}GB ingestion")

    def get_metrics_summary(self) -> Dict:
        """Get summary of all recorded metrics."""
        metrics_files = list(self.metrics_dir.glob("ingestion_metrics_*.json"))
        all_metrics = []
        
        for file in metrics_files:
            with open(file, 'r') as f:
                metrics = json.load(f)
                all_metrics.append(metrics)
                
        if not all_metrics:
            return {}
            
        df = pd.DataFrame(all_metrics)
        
        summary = {
            "total_ingestions": len(df),
            "successful_ingestions": len(df[df['success']]),
            "failed_ingestions": len(df[~df['success']]),
            "average_ingestion_time": df['ingestion_time_seconds'].mean(),
            "format_stats": df.groupby('file_format').agg({
                'success': 'mean',
                'ingestion_time_seconds': 'mean'
            }).to_dict(),
            "size_stats": df.groupby('file_size_gb').agg({
                'success': 'mean',
                'ingestion_time_seconds': 'mean'
            }).to_dict(),
            "data_lake_stats": df.groupby('data_lake').agg({
                'success': 'mean',
                'ingestion_time_seconds': 'mean'
            }).to_dict()
        }
        
        return summary

    def generate_dashboard_data(self) -> Dict:
        """
        Generate data for the dashboard.
        
        Returns:
            Dict: Dashboard data including metrics and comparisons
        """
        metrics_summary = self.get_metrics_summary()
        
        dashboard_data = {
            "timestamp": datetime.now().isoformat(),
            "summary": metrics_summary,
            "comparisons": {
                "format_comparison": self._generate_format_comparison(),
                "size_comparison": self._generate_size_comparison(),
                "data_lake_comparison": self._generate_data_lake_comparison()
            }
        }
        
        return dashboard_data

    def _generate_format_comparison(self) -> Dict:
        """Generate comparison data for different file formats."""
        metrics_files = list(self.metrics_dir.glob("ingestion_metrics_*.json"))
        all_metrics = []
        
        for file in metrics_files:
            with open(file, 'r') as f:
                metrics = json.load(f)
                all_metrics.append(metrics)
                
        if not all_metrics:
            return {}
            
        df = pd.DataFrame(all_metrics)
        
        return {
            "formats": df['file_format'].unique().tolist(),
            "avg_ingestion_time": df.groupby('file_format')['ingestion_time_seconds'].mean().to_dict(),
            "success_rate": df.groupby('file_format')['success'].mean().to_dict(),
            "total_ingestions": df.groupby('file_format').size().to_dict()
        }

    def _generate_size_comparison(self) -> Dict:
        """Generate comparison data for different file sizes."""
        metrics_files = list(self.metrics_dir.glob("ingestion_metrics_*.json"))
        all_metrics = []
        
        for file in metrics_files:
            with open(file, 'r') as f:
                metrics = json.load(f)
                all_metrics.append(metrics)
                
        if not all_metrics:
            return {}
            
        df = pd.DataFrame(all_metrics)
        
        return {
            "sizes": df['file_size_gb'].unique().tolist(),
            "avg_ingestion_time": df.groupby('file_size_gb')['ingestion_time_seconds'].mean().to_dict(),
            "success_rate": df.groupby('file_size_gb')['success'].mean().to_dict(),
            "total_ingestions": df.groupby('file_size_gb').size().to_dict()
        }

    def _generate_data_lake_comparison(self) -> Dict:
        """Generate comparison data for different data lakes."""
        metrics_files = list(self.metrics_dir.glob("ingestion_metrics_*.json"))
        all_metrics = []
        
        for file in metrics_files:
            with open(file, 'r') as f:
                metrics = json.load(f)
                all_metrics.append(metrics)
                
        if not all_metrics:
            return {}
            
        df = pd.DataFrame(all_metrics)
        
        return {
            "data_lakes": df['data_lake'].unique().tolist(),
            "avg_ingestion_time": df.groupby('data_lake')['ingestion_time_seconds'].mean().to_dict(),
            "success_rate": df.groupby('data_lake')['success'].mean().to_dict(),
            "total_ingestions": df.groupby('data_lake').size().to_dict()
        } 