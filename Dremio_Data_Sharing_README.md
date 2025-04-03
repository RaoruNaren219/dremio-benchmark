# Dremio Data Sharing API Guide

This guide provides step-by-step instructions for using the Dremio Data Sharing API collection in Postman to enable data sharing between two Dremio instances.

## Prerequisites

1. **Postman Installation**
   - Download and install [Postman](https://www.postman.com/downloads/)
   - Create a Postman account (optional but recommended)

2. **Dremio Access**
   - Access to source Dremio instance
   - Access to target Dremio instance
   - Valid credentials for both instances

3. **Network Access**
   - Ensure both Dremio instances are accessible from your network
   - Verify ports (default: 9047) are open and accessible

## Setup Instructions

### 1. Import the Collection

1. Open Postman
2. Click "Import" button
3. Select `Dremio_Data_Sharing.postman_collection.json`
4. Click "Import"

### 2. Configure Environment Variables

1. Create a new environment in Postman:
   - Click "Environments" in the sidebar
   - Click "+" to create new environment
   - Name it "Dremio Data Sharing"

2. Add the following variables:
   ```
   source_dremio_url: http://your-source-dremio:9047
   target_dremio_url: http://your-target-dremio:9047
   username: your_dremio_username
   password: your_dremio_password
   auth_token: (leave empty, will be populated after authentication)
   space_name: shared_data
   table_name: shared_table
   format: parquet
   content_type: application/x-parquet
   ```

### 3. Authentication Setup

1. Select the "Dremio Data Sharing" environment
2. Go to "Authentication" folder
3. Send "Get Auth Token" request
4. Copy the token from the response
5. Update the `auth_token` environment variable with the received token

## Data Sharing Workflow

### 1. Source Dremio Operations

1. **List Available Datasets**
   - Send "List Available Datasets" request
   - Review the response to identify the dataset to share
   - Note the dataset ID and path

2. **Get Dataset Details**
   - Update `dataset_id` variable with the selected dataset ID
   - Send "Get Dataset Details" request
   - Verify dataset structure and content

3. **Export Dataset**
   - Update `dataset_path` with the selected dataset path
   - Choose format (parquet, csv, or orc)
   - Send "Export Dataset" request
   - Save the exported file location

### 2. Target Dremio Operations

1. **Create Space**
   - Update `space_name` if needed
   - Send "Create Space" request
   - Verify space creation success

2. **Import Dataset**
   - Update `file_path` with the exported file location
   - Update `content_type` based on format:
     - Parquet: application/x-parquet
     - CSV: text/csv
     - ORC: application/x-orc
   - Send "Import Dataset" request
   - Wait for import completion

3. **Verify Dataset**
   - Send "Verify Dataset" request
   - Confirm dataset exists and is accessible

### 3. Data Sharing Operations

1. **Share Dataset**
   - Update variables:
     ```
     source_dataset_path: Path to source dataset
     target_space: Target space name
     target_name: Target table name
     format: Desired format
     ```
   - Send "Share Dataset" request
   - Save the `share_id` from response

2. **Check Sharing Status**
   - Update `share_id` with the received ID
   - Send "Get Sharing Status" request
   - Monitor until status is "COMPLETED"

## Metrics Collection

### 1. Query Performance Metrics

1. **Get Query Performance Metrics**
   - Send "Get Query Performance Metrics" request
   - Review metrics including:
     - Query execution time
     - Number of rows processed
     - Resource utilization
     - Cache hit/miss rates

2. **Analyze Performance Data**
   - Look for patterns in slow queries
   - Identify resource bottlenecks
   - Monitor cache effectiveness

### 2. Dataset Metrics

1. **Get Dataset Metrics**
   - Update `dataset_id` variable
   - Send "Get Dataset Metrics" request
   - Review metrics including:
     - Dataset size
     - Access patterns
     - Update frequency
     - Storage utilization

2. **Monitor Dataset Health**
   - Track growth patterns
   - Monitor access patterns
   - Identify optimization opportunities

### 3. Sharing Performance Metrics

1. **Get Sharing Performance Metrics**
   - Update `share_id` variable
   - Send "Get Sharing Performance Metrics" request
   - Review metrics including:
     - Transfer speed
     - Data volume
     - Network utilization
     - Success/failure rates

2. **Track Sharing Operations**
   - Monitor transfer progress
   - Identify bottlenecks
   - Optimize sharing patterns

### 4. System Resource Metrics

1. **Get System Resource Metrics**
   - Send "Get System Resource Metrics" request
   - Review metrics including:
     - CPU utilization
     - Memory usage
     - Disk I/O
     - Network bandwidth

2. **Monitor System Health**
   - Track resource utilization
   - Identify capacity issues
   - Plan for scaling

### 5. Metrics Analysis

1. **Performance Analysis**
   - Compare metrics across instances
   - Identify performance patterns
   - Set performance baselines

2. **Capacity Planning**
   - Track resource utilization trends
   - Plan for future growth
   - Optimize resource allocation

3. **Troubleshooting**
   - Use metrics to identify issues
   - Track performance degradation
   - Monitor system health

### 6. Metrics Best Practices

1. **Collection Frequency**
   - Set appropriate collection intervals
   - Balance granularity with overhead
   - Store historical data

2. **Analysis**
   - Regular review of metrics
   - Set up alerts for thresholds
   - Document performance patterns

3. **Optimization**
   - Use metrics to guide improvements
   - Track optimization effectiveness
   - Maintain performance baselines

## Troubleshooting

### Common Issues

1. **Authentication Failures**
   - Verify credentials in environment variables
   - Check if token is expired
   - Ensure proper URL format

2. **Connection Issues**
   - Verify network connectivity
   - Check firewall settings
   - Validate Dremio instance URLs

3. **Import/Export Failures**
   - Check file permissions
   - Verify sufficient disk space
   - Validate file format compatibility

### Error Messages

1. **401 Unauthorized**
   - Re-authenticate using "Get Auth Token"
   - Update `auth_token` variable

2. **404 Not Found**
   - Verify dataset path
   - Check space/table names
   - Validate Dremio URLs

3. **500 Internal Server Error**
   - Check Dremio logs
   - Verify resource availability
   - Contact Dremio support

## Best Practices

1. **Security**
   - Use environment variables for sensitive data
   - Rotate credentials regularly
   - Use HTTPS for API calls

2. **Performance**
   - Monitor memory usage
   - Use appropriate chunk sizes
   - Implement proper error handling

3. **Maintenance**
   - Regular token refresh
   - Monitor sharing status
   - Clean up temporary files

## Support

For additional support:
- Refer to [Dremio Documentation](https://docs.dremio.com/)
- Contact Dremio Support
- Check Postman documentation for API collection usage 