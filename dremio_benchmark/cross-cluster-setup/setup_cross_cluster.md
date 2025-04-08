# Cross-Cluster Setup for Dremio A and Dremio B

This document provides instructions for setting up cross-cluster access between Dremio A (Simple-auth HDFS) and Dremio B (Kerberized HDFS).

## Prerequisites

- Dremio A is connected to a Simple-auth HDFS cluster
- Dremio B is connected to a Kerberized HDFS cluster
- Network connectivity between the two clusters
- Administrative access to both Dremio instances

## Setup Procedure

### 1. Configure External Source in Dremio A (to access Dremio B)

1. Log in to Dremio A as an administrator.
2. Navigate to "Sources" in the left sidebar.
3. Click "Add Source" and select "Dremio".
4. Configure the source with the following settings:
   - Name: `DremioB`
   - Host: `<Dremio B hostname or IP>`
   - Port: `9047` (default JDBC port)
   - Authentication: `Basic Authentication`
   - Username: `<admin username for Dremio B>`
   - Password: `<admin password for Dremio B>`
   - SSL Enabled: `true` (if SSL is enabled on Dremio B)
5. Click "Save" to create the source.

### 2. Configure External Source in Dremio B (to access Dremio A)

1. Log in to Dremio B as an administrator.
2. Navigate to "Sources" in the left sidebar.
3. Click "Add Source" and select "Dremio".
4. Configure the source with the following settings:
   - Name: `DremioA`
   - Host: `<Dremio A hostname or IP>`
   - Port: `9047` (default JDBC port)
   - Authentication: `Basic Authentication`
   - Username: `<admin username for Dremio A>`
   - Password: `<admin password for Dremio A>`
   - SSL Enabled: `true` (if SSL is enabled on Dremio A)
5. Click "Save" to create the source.

### 3. Set Up Access Controls

1. In Dremio A:
   - Create a user or role for cross-cluster access.
   - Grant appropriate permissions to this user/role for accessing datasets that need to be shared.

2. In Dremio B:
   - Create a user or role for cross-cluster access.
   - Grant appropriate permissions to this user/role for accessing datasets that need to be shared.

### 4. Test Cross-Cluster Access

#### Test Access from Dremio A to Dremio B

1. In Dremio A, navigate to the "DremioB" source.
2. You should see the available spaces and datasets from Dremio B.
3. Run a simple query against a dataset in Dremio B to verify access:
   ```sql
   SELECT * FROM DremioB.hdfs.tpcds_1gb_parquet.customer LIMIT 10;
   ```

#### Test Access from Dremio B to Dremio A

1. In Dremio B, navigate to the "DremioA" source.
2. You should see the available spaces and datasets from Dremio A.
3. Run a simple query against a dataset in Dremio A to verify access:
   ```sql
   SELECT * FROM DremioA.hdfs.tpcds_1gb_parquet.customer LIMIT 10;
   ```

### 5. Set Up Virtual Datasets for Cross-Cluster Joins

1. In Dremio A, create a virtual dataset that joins data from both clusters:
   ```sql
   CREATE VDS cross_cluster_join AS
   SELECT a.customer_id, a.name, b.order_id, b.order_date
   FROM hdfs.tpcds_1gb_parquet.customer a
   JOIN DremioB.hdfs.tpcds_1gb_parquet.store_sales b
   ON a.customer_id = b.customer_id;
   ```

2. In Dremio B, create a similar virtual dataset for testing:
   ```sql
   CREATE VDS cross_cluster_join AS
   SELECT a.customer_id, a.name, b.order_id, b.order_date
   FROM hdfs.tpcds_1gb_parquet.customer a
   JOIN DremioA.hdfs.tpcds_1gb_parquet.store_sales b
   ON a.customer_id = b.customer_id;
   ```

## Troubleshooting

### Common Issues

1. **Connection Errors**: Ensure network connectivity between clusters and check firewall settings.
2. **Authentication Failures**: Verify username and password credentials.
3. **Permission Denied**: Check access control settings for the cross-cluster user.
4. **SSL/TLS Issues**: Verify SSL certificate configuration if SSL is enabled.

### Checking Connectivity

Run the following command to verify network connectivity:
```bash
telnet <remote_host> 9047
```

### Viewing Logs

Check the Dremio logs for error messages:
```bash
less /var/log/dremio/server.log
```

## Performance Considerations

- Cross-cluster queries will have higher latency compared to local queries.
- Large data transfers between clusters can impact performance.
- Consider using data replication for frequently accessed datasets.
- Use query profiles to identify performance bottlenecks.

## Security Considerations

- Use SSL/TLS for secure communication between clusters.
- Create dedicated users with minimal required permissions for cross-cluster access.
- Regularly rotate credentials used for cross-cluster authentication.
- Monitor and audit cross-cluster access. 