#!/bin/bash

# Configuration
SCALE_FACTORS=(1 10 100)  # Scale factors for 1GB, 10GB, and 100GB
OUTPUT_BASE_DIR="./tpcds_data"
PARALLEL_STREAMS=4  # Number of parallel streams for data generation
LOG_DIR="./logs"
LOG_FILE="$LOG_DIR/tpcds_generation.log"

# Error handling
set -e
trap 'echo "Error occurred at line $LINENO. Exit code: $?" >&2' ERR

# Create log directory
mkdir -p "$LOG_DIR"

# Logging function
log() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] $1" | tee -a "$LOG_FILE"
}

# Check if dsdgen is available
if ! command -v dsdgen &> /dev/null; then
    log "Error: dsdgen command not found. Please install the TPC-DS toolkit."
    exit 1
fi

# Function to check disk space
check_disk_space() {
    local required_space=$1
    local available_space=$(df -B1 . | awk 'NR==2 {print $4}')
    
    if [ "$available_space" -lt "$required_space" ]; then
        log "Error: Insufficient disk space. Required: $required_space bytes, Available: $available_space bytes"
        exit 1
    fi
}

# Function to generate data for a specific stream
generate_stream() {
    local scale_factor=$1
    local stream_num=$2
    local start=$3
    local end=$4
    local output_dir=$5
    
    log "Generating data for scale factor $scale_factor, stream $stream_num (tables $start to $end)"
    
    for ((i=start; i<=end; i++)); do
        if ! ./dsdgen -scale $scale_factor -child $i -parallel $PARALLEL_STREAMS -dir "$output_dir"; then
            log "Error: Failed to generate data for table $i in stream $stream_num"
            return 1
        fi
    done
}

# Function to generate data for a specific scale factor
generate_scale_factor() {
    local scale_factor=$1
    local output_dir="$OUTPUT_BASE_DIR/sf$scale_factor"
    
    log "Starting TPC-DS data generation for scale factor $scale_factor"
    log "Output directory: $output_dir"
    
    # Create output directory if it doesn't exist
    mkdir -p "$output_dir"
    
    # Calculate tables per stream
    TOTAL_TABLES=24  # Number of tables in TPC-DS
    TABLES_PER_STREAM=$((TOTAL_TABLES / PARALLEL_STREAMS))
    
    # Launch parallel streams
    local pids=()
    for ((stream=0; stream<PARALLEL_STREAMS; stream++)); do
        start=$((stream * TABLES_PER_STREAM + 1))
        end=$((start + TABLES_PER_STREAM - 1))
        if [ $stream -eq $((PARALLEL_STREAMS - 1)) ]; then
            end=$TOTAL_TABLES  # Last stream gets remaining tables
        fi
        
        generate_stream $scale_factor $stream $start $end "$output_dir" &
        pids+=($!)
    done
    
    # Wait for all background processes to complete and check their status
    for pid in "${pids[@]}"; do
        if ! wait "$pid"; then
            log "Error: One or more data generation processes failed"
            return 1
        fi
    done
    
    log "Data generation completed for scale factor $scale_factor!"
    log "Generated data is in: $output_dir"
}

# Main data generation process
log "Starting TPC-DS data generation for multiple scale factors"
log "Using $PARALLEL_STREAMS parallel streams"

# Calculate total required space (approximate)
# Note: This is a rough estimate. Adjust based on your needs
TOTAL_REQUIRED_SPACE=0
for sf in "${SCALE_FACTORS[@]}"; do
    case $sf in
        1)  TOTAL_REQUIRED_SPACE=$((TOTAL_REQUIRED_SPACE + 1024*1024*1024)) ;;  # 1GB
        10) TOTAL_REQUIRED_SPACE=$((TOTAL_REQUIRED_SPACE + 10*1024*1024*1024)) ;;  # 10GB
        100) TOTAL_REQUIRED_SPACE=$((TOTAL_REQUIRED_SPACE + 100*1024*1024*1024)) ;;  # 100GB
    esac
done

# Check disk space before starting
check_disk_space $TOTAL_REQUIRED_SPACE

# Generate data for each scale factor
for sf in "${SCALE_FACTORS[@]}"; do
    log "Processing scale factor $sf..."
    if ! generate_scale_factor $sf; then
        log "Error: Failed to generate data for scale factor $sf"
        exit 1
    fi
    log "Completed scale factor $sf"
done

log "All TPC-DS data generation completed!"
log "Data is organized in the following directories:"
for sf in "${SCALE_FACTORS[@]}"; do
    log "- $OUTPUT_BASE_DIR/sf$sf"
done

log "TPC-DS data preparation completed successfully!"

# Convert to Parquet format (placeholder - implement based on your needs)
echo "Converting data to Parquet format..."
# Add your Parquet conversion logic here

echo "TPC-DS data preparation completed!" 