import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from metrics_collector import MetricsCollector
from datetime import datetime, timedelta
import json

def setup_page():
    """Setup the Streamlit page configuration."""
    st.set_page_config(
        page_title="Data Lake Performance Dashboard",
        page_icon="📊",
        layout="wide"
    )
    st.title("Data Lake Performance Dashboard")

def load_metrics():
    """Load metrics from the collector."""
    collector = MetricsCollector()
    return collector.generate_dashboard_data()

def display_summary_metrics(metrics):
    """Display summary metrics in cards."""
    summary = metrics['summary']
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Ingestions",
            summary.get('total_ingestions', 0)
        )
    
    with col2:
        st.metric(
            "Successful Ingestions",
            summary.get('successful_ingestions', 0)
        )
    
    with col3:
        st.metric(
            "Failed Ingestions",
            summary.get('failed_ingestions', 0)
        )
    
    with col4:
        st.metric(
            "Avg Ingestion Time (s)",
            f"{summary.get('average_ingestion_time', 0):.2f}"
        )

def display_format_comparison(metrics):
    """Display file format comparison charts."""
    st.subheader("File Format Comparison")
    
    format_data = metrics['comparisons']['format_comparison']
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Ingestion time comparison
        fig = px.bar(
            x=list(format_data['avg_ingestion_time'].keys()),
            y=list(format_data['avg_ingestion_time'].values()),
            title="Average Ingestion Time by Format",
            labels={'x': 'File Format', 'y': 'Time (seconds)'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Success rate comparison
        fig = px.bar(
            x=list(format_data['success_rate'].keys()),
            y=list(format_data['success_rate'].values()),
            title="Success Rate by Format",
            labels={'x': 'File Format', 'y': 'Success Rate'}
        )
        st.plotly_chart(fig, use_container_width=True)

def display_size_comparison(metrics):
    """Display file size comparison charts."""
    st.subheader("File Size Comparison")
    
    size_data = metrics['comparisons']['size_comparison']
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Ingestion time by size
        fig = px.line(
            x=list(size_data['avg_ingestion_time'].keys()),
            y=list(size_data['avg_ingestion_time'].values()),
            title="Average Ingestion Time by File Size",
            labels={'x': 'File Size (GB)', 'y': 'Time (seconds)'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Success rate by size
        fig = px.bar(
            x=list(size_data['success_rate'].keys()),
            y=list(size_data['success_rate'].values()),
            title="Success Rate by File Size",
            labels={'x': 'File Size (GB)', 'y': 'Success Rate'}
        )
        st.plotly_chart(fig, use_container_width=True)

def display_data_lake_comparison(metrics):
    """Display data lake comparison charts."""
    st.subheader("Data Lake Comparison")
    
    lake_data = metrics['comparisons']['data_lake_comparison']
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Ingestion time comparison
        fig = px.bar(
            x=list(lake_data['avg_ingestion_time'].keys()),
            y=list(lake_data['avg_ingestion_time'].values()),
            title="Average Ingestion Time by Data Lake",
            labels={'x': 'Data Lake', 'y': 'Time (seconds)'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Success rate comparison
        fig = px.bar(
            x=list(lake_data['success_rate'].keys()),
            y=list(lake_data['success_rate'].values()),
            title="Success Rate by Data Lake",
            labels={'x': 'Data Lake', 'y': 'Success Rate'}
        )
        st.plotly_chart(fig, use_container_width=True)

def display_trend_analysis(metrics):
    """Display trend analysis over time."""
    st.subheader("Performance Trends")
    
    # Load all metrics for trend analysis
    collector = MetricsCollector()
    metrics_files = list(collector.metrics_dir.glob("ingestion_metrics_*.json"))
    
    if not metrics_files:
        st.info("No trend data available yet")
        return
        
    all_metrics = []
    for file in metrics_files:
        with open(file, 'r') as f:
            all_metrics.append(json.load(f))
            
    df = pd.DataFrame(all_metrics)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Time series of ingestion times
    fig = px.scatter(
        df,
        x='timestamp',
        y='ingestion_time_seconds',
        color='file_format',
        title="Ingestion Time Trends",
        labels={'timestamp': 'Time', 'ingestion_time_seconds': 'Time (seconds)'}
    )
    st.plotly_chart(fig, use_container_width=True)

def main():
    """Main function to run the dashboard."""
    setup_page()
    
    # Load metrics
    metrics = load_metrics()
    
    # Display metrics
    display_summary_metrics(metrics)
    
    # Display comparisons
    display_format_comparison(metrics)
    display_size_comparison(metrics)
    display_data_lake_comparison(metrics)
    
    # Display trends
    display_trend_analysis(metrics)
    
    # Add refresh button
    if st.button("Refresh Dashboard"):
        st.experimental_rerun()

if __name__ == "__main__":
    main() 