#!/usr/bin/env python3

"""
Dremio Benchmark Report Generator
This script generates performance reports from benchmark results
"""

import argparse
import logging
import sys
import csv
import json
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import List, Dict, Optional
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("report_generation.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def load_benchmark_results(results_file: str) -> pd.DataFrame:
    """
    Load benchmark results from CSV file
    
    Args:
        results_file (str): Path to results CSV file
    
    Returns:
        pd.DataFrame: DataFrame containing benchmark results
    """
    try:
        df = pd.read_csv(results_file)
        logger.info(f"Loaded {len(df)} benchmark results from {results_file}")
        return df
    except Exception as e:
        logger.error(f"Error loading benchmark results: {str(e)}")
        sys.exit(1)

def generate_summary_statistics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate summary statistics from benchmark results
    
    Args:
        df (pd.DataFrame): DataFrame containing benchmark results
    
    Returns:
        pd.DataFrame: DataFrame containing summary statistics
    """
    # Group by query_name and calculate statistics
    summary = df.groupby('query_name').agg({
        'execution_time': ['mean', 'min', 'max', 'std', 'count'],
        'memory_used': ['mean', 'max'],
        'cpu_used': ['mean', 'max'],
        'io_used': ['mean', 'max'],
        'status': lambda x: (x == 'COMPLETED').mean() * 100  # Success rate as percentage
    })
    
    # Flatten multi-index columns
    summary.columns = ['_'.join(col).strip() for col in summary.columns.values]
    
    # Rename columns for clarity
    summary = summary.rename(columns={
        'execution_time_mean': 'avg_execution_time',
        'execution_time_min': 'min_execution_time',
        'execution_time_max': 'max_execution_time',
        'execution_time_std': 'std_execution_time',
        'execution_time_count': 'run_count',
        'memory_used_mean': 'avg_memory_used',
        'memory_used_max': 'max_memory_used',
        'cpu_used_mean': 'avg_cpu_used',
        'cpu_used_max': 'max_cpu_used',
        'io_used_mean': 'avg_io_used',
        'io_used_max': 'max_io_used',
        'status_<lambda>': 'success_rate'
    })
    
    return summary

def generate_comparison_report(df_a: pd.DataFrame, df_b: pd.DataFrame, report_name: str) -> pd.DataFrame:
    """
    Generate a comparison report between two benchmark results
    
    Args:
        df_a (pd.DataFrame): DataFrame containing benchmark results for cluster A
        df_b (pd.DataFrame): DataFrame containing benchmark results for cluster B
        report_name (str): Name of the report
    
    Returns:
        pd.DataFrame: DataFrame containing comparison results
    """
    # Generate summary statistics for each cluster
    summary_a = generate_summary_statistics(df_a)
    summary_b = generate_summary_statistics(df_b)
    
    # Calculate performance differences
    comparison = pd.DataFrame()
    comparison['query_name'] = summary_a.index
    comparison['a_avg_time'] = summary_a['avg_execution_time']
    comparison['b_avg_time'] = summary_b['avg_execution_time']
    comparison['time_diff'] = summary_b['avg_execution_time'] - summary_a['avg_execution_time']
    comparison['time_diff_pct'] = (comparison['time_diff'] / summary_a['avg_execution_time']) * 100
    
    comparison['a_avg_memory'] = summary_a['avg_memory_used']
    comparison['b_avg_memory'] = summary_b['avg_memory_used']
    comparison['memory_diff_pct'] = ((summary_b['avg_memory_used'] - summary_a['avg_memory_used']) / 
                                     summary_a['avg_memory_used']) * 100
    
    comparison['a_success_rate'] = summary_a['success_rate']
    comparison['b_success_rate'] = summary_b['success_rate']
    
    # Set index for easier access
    comparison.set_index('query_name', inplace=True)
    
    return comparison

def generate_charts(df: pd.DataFrame, output_dir: str, prefix: str = ""):
    """
    Generate charts from benchmark results
    
    Args:
        df (pd.DataFrame): DataFrame containing benchmark results
        output_dir (str): Directory to save charts
        prefix (str, optional): Prefix for chart filenames. Defaults to "".
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. Execution Time by Query
    plt.figure(figsize=(12, 6))
    chart = sns.barplot(x=df.index, y='avg_execution_time', data=df)
    chart.set_xticklabels(chart.get_xticklabels(), rotation=90)
    plt.title('Average Execution Time by Query')
    plt.ylabel('Execution Time (seconds)')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f"{prefix}execution_time.png"))
    plt.close()
    
    # 2. Memory Usage by Query
    plt.figure(figsize=(12, 6))
    chart = sns.barplot(x=df.index, y='avg_memory_used', data=df)
    chart.set_xticklabels(chart.get_xticklabels(), rotation=90)
    plt.title('Average Memory Usage by Query')
    plt.ylabel('Memory Usage')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f"{prefix}memory_usage.png"))
    plt.close()
    
    # 3. CPU Usage by Query
    plt.figure(figsize=(12, 6))
    chart = sns.barplot(x=df.index, y='avg_cpu_used', data=df)
    chart.set_xticklabels(chart.get_xticklabels(), rotation=90)
    plt.title('Average CPU Usage by Query')
    plt.ylabel('CPU Usage')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f"{prefix}cpu_usage.png"))
    plt.close()
    
    # 4. Success Rate by Query
    plt.figure(figsize=(12, 6))
    chart = sns.barplot(x=df.index, y='success_rate', data=df)
    chart.set_xticklabels(chart.get_xticklabels(), rotation=90)
    plt.title('Success Rate by Query')
    plt.ylabel('Success Rate (%)')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f"{prefix}success_rate.png"))
    plt.close()

def generate_comparison_charts(comparison_df: pd.DataFrame, output_dir: str):
    """
    Generate comparison charts between two benchmark results
    
    Args:
        comparison_df (pd.DataFrame): DataFrame containing comparison results
        output_dir (str): Directory to save charts
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. Execution Time Comparison
    plt.figure(figsize=(12, 6))
    comparison_df[['a_avg_time', 'b_avg_time']].plot(kind='bar', figsize=(12, 6))
    plt.title('Execution Time Comparison (A vs B)')
    plt.ylabel('Execution Time (seconds)')
    plt.xticks(rotation=90)
    plt.legend(['Cluster A', 'Cluster B'])
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "time_comparison.png"))
    plt.close()
    
    # 2. Performance Difference Percentage
    plt.figure(figsize=(12, 6))
    sns.barplot(x=comparison_df.index, y='time_diff_pct', data=comparison_df)
    plt.title('Performance Difference (B vs A)')
    plt.ylabel('Difference (%)')
    plt.axhline(y=0, color='r', linestyle='-')
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "performance_diff.png"))
    plt.close()
    
    # 3. Memory Usage Comparison
    plt.figure(figsize=(12, 6))
    comparison_df[['a_avg_memory', 'b_avg_memory']].plot(kind='bar', figsize=(12, 6))
    plt.title('Memory Usage Comparison (A vs B)')
    plt.ylabel('Memory Usage')
    plt.xticks(rotation=90)
    plt.legend(['Cluster A', 'Cluster B'])
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "memory_comparison.png"))
    plt.close()
    
    # 4. Success Rate Comparison
    plt.figure(figsize=(12, 6))
    comparison_df[['a_success_rate', 'b_success_rate']].plot(kind='bar', figsize=(12, 6))
    plt.title('Success Rate Comparison (A vs B)')
    plt.ylabel('Success Rate (%)')
    plt.xticks(rotation=90)
    plt.legend(['Cluster A', 'Cluster B'])
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "success_rate_comparison.png"))
    plt.close()

def generate_html_report(summary_df: pd.DataFrame, comparison_df: Optional[pd.DataFrame], 
                        output_file: str, title: str):
    """
    Generate an HTML report from benchmark results
    
    Args:
        summary_df (pd.DataFrame): DataFrame containing summary statistics
        comparison_df (Optional[pd.DataFrame]): DataFrame containing comparison results
        output_file (str): Path to output HTML file
        title (str): Report title
    """
    # Create HTML header and style
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>{title}</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            h1, h2 {{ color: #333; }}
            table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
            tr:nth-child(even) {{ background-color: #f9f9f9; }}
            .chart {{ margin: 20px 0; max-width: 100%; }}
            .summary {{ margin-bottom: 40px; }}
            .good {{ color: green; }}
            .bad {{ color: red; }}
        </style>
    </head>
    <body>
        <h1>{title}</h1>
        <p>Report generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    """
    
    # Add summary section
    html += """
        <div class="summary">
            <h2>Performance Summary</h2>
            <table>
                <tr>
                    <th>Metric</th>
                    <th>Value</th>
                </tr>
    """
    
    # Add overall statistics
    total_queries = len(summary_df)
    avg_execution_time = summary_df['avg_execution_time'].mean()
    success_rate = summary_df['success_rate'].mean()
    
    html += f"""
                <tr>
                    <td>Total Queries</td>
                    <td>{total_queries}</td>
                </tr>
                <tr>
                    <td>Average Execution Time</td>
                    <td>{avg_execution_time:.2f} seconds</td>
                </tr>
                <tr>
                    <td>Overall Success Rate</td>
                    <td>{success_rate:.2f}%</td>
                </tr>
    """
    
    html += """
            </table>
        </div>
    """
    
    # Add detailed results section
    html += """
        <div class="detailed-results">
            <h2>Detailed Query Results</h2>
            <table>
                <tr>
                    <th>Query</th>
                    <th>Avg Time (s)</th>
                    <th>Min Time (s)</th>
                    <th>Max Time (s)</th>
                    <th>Std Dev</th>
                    <th>Avg Memory</th>
                    <th>Avg CPU</th>
                    <th>Success Rate</th>
                </tr>
    """
    
    # Add a row for each query
    for query, row in summary_df.iterrows():
        html += f"""
                <tr>
                    <td>{query}</td>
                    <td>{row['avg_execution_time']:.2f}</td>
                    <td>{row['min_execution_time']:.2f}</td>
                    <td>{row['max_execution_time']:.2f}</td>
                    <td>{row['std_execution_time']:.2f}</td>
                    <td>{row['avg_memory_used']:.2f}</td>
                    <td>{row['avg_cpu_used']:.2f}</td>
                    <td>{row['success_rate']:.2f}%</td>
                </tr>
        """
    
    html += """
            </table>
        </div>
    """
    
    # Add comparison section if available
    if comparison_df is not None:
        html += """
            <div class="comparison">
                <h2>Cluster Comparison</h2>
                <table>
                    <tr>
                        <th>Query</th>
                        <th>A Avg Time (s)</th>
                        <th>B Avg Time (s)</th>
                        <th>Time Diff (s)</th>
                        <th>Time Diff (%)</th>
                        <th>A Memory</th>
                        <th>B Memory</th>
                        <th>Memory Diff (%)</th>
                    </tr>
        """
        
        # Add a row for each query comparison
        for query, row in comparison_df.iterrows():
            # Determine if B is better (negative diff) or worse (positive diff)
            time_diff_class = "good" if row['time_diff'] < 0 else "bad" if row['time_diff'] > 0 else ""
            
            html += f"""
                    <tr>
                        <td>{query}</td>
                        <td>{row['a_avg_time']:.2f}</td>
                        <td>{row['b_avg_time']:.2f}</td>
                        <td class="{time_diff_class}">{row['time_diff']:.2f}</td>
                        <td class="{time_diff_class}">{row['time_diff_pct']:.2f}%</td>
                        <td>{row['a_avg_memory']:.2f}</td>
                        <td>{row['b_avg_memory']:.2f}</td>
                        <td>{row['memory_diff_pct']:.2f}%</td>
                    </tr>
            """
        
        html += """
                </table>
            </div>
        """
    
    # Add chart section - these would be links to the generated chart images
    if comparison_df is not None:
        html += """
            <div class="charts">
                <h2>Performance Charts</h2>
                <div class="chart">
                    <h3>Execution Time Comparison</h3>
                    <img src="charts/time_comparison.png" alt="Execution Time Comparison">
                </div>
                <div class="chart">
                    <h3>Performance Difference</h3>
                    <img src="charts/performance_diff.png" alt="Performance Difference">
                </div>
                <div class="chart">
                    <h3>Memory Usage Comparison</h3>
                    <img src="charts/memory_comparison.png" alt="Memory Usage Comparison">
                </div>
                <div class="chart">
                    <h3>Success Rate Comparison</h3>
                    <img src="charts/success_rate_comparison.png" alt="Success Rate Comparison">
                </div>
            </div>
        """
    else:
        html += """
            <div class="charts">
                <h2>Performance Charts</h2>
                <div class="chart">
                    <h3>Execution Time by Query</h3>
                    <img src="charts/execution_time.png" alt="Execution Time by Query">
                </div>
                <div class="chart">
                    <h3>Memory Usage by Query</h3>
                    <img src="charts/memory_usage.png" alt="Memory Usage by Query">
                </div>
                <div class="chart">
                    <h3>CPU Usage by Query</h3>
                    <img src="charts/cpu_usage.png" alt="CPU Usage by Query">
                </div>
                <div class="chart">
                    <h3>Success Rate by Query</h3>
                    <img src="charts/success_rate.png" alt="Success Rate by Query">
                </div>
            </div>
        """
    
    # Close HTML tags
    html += """
    </body>
    </html>
    """
    
    # Write HTML to file
    with open(output_file, 'w') as f:
        f.write(html)
    
    logger.info(f"Generated HTML report: {output_file}")

def main():
    """Main function to generate performance reports"""
    parser = argparse.ArgumentParser(description="Generate performance reports from benchmark results")
    parser.add_argument("--results-a", required=True, help="Path to results CSV file for cluster A")
    parser.add_argument("--results-b", help="Path to results CSV file for cluster B (for comparison)")
    parser.add_argument("--output-dir", default="./reports", help="Output directory for reports")
    parser.add_argument("--title", default="Dremio Benchmark Report", help="Report title")
    
    args = parser.parse_args()
    
    logger.info("Starting report generation...")
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(os.path.join(args.output_dir, "charts"), exist_ok=True)
    
    # Load benchmark results for cluster A
    df_a = load_benchmark_results(args.results_a)
    
    # Generate summary statistics for cluster A
    summary_a = generate_summary_statistics(df_a)
    
    # Generate charts for cluster A
    generate_charts(summary_a, os.path.join(args.output_dir, "charts"))
    
    # Initialize comparison DataFrame
    comparison_df = None
    
    # Load benchmark results for cluster B if provided
    if args.results_b:
        df_b = load_benchmark_results(args.results_b)
        
        # Generate comparison report
        comparison_df = generate_comparison_report(df_a, df_b, f"{args.title} - Comparison")
        
        # Generate comparison charts
        generate_comparison_charts(comparison_df, os.path.join(args.output_dir, "charts"))
    
    # Generate HTML report
    generate_html_report(
        summary_df=summary_a,
        comparison_df=comparison_df,
        output_file=os.path.join(args.output_dir, "report.html"),
        title=args.title
    )
    
    # Generate CSV reports
    summary_a.to_csv(os.path.join(args.output_dir, "summary_a.csv"))
    
    if comparison_df is not None:
        comparison_df.to_csv(os.path.join(args.output_dir, "comparison.csv"))
    
    logger.info(f"Report generation completed. Reports saved to {args.output_dir}")

if __name__ == "__main__":
    main() 