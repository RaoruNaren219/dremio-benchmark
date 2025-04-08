-- Cross-Cluster Join Example
-- This query joins customer data from Dremio A with sales data from Dremio B

SELECT 
    a.c_customer_id,
    a.c_first_name,
    a.c_last_name,
    a.c_email_address,
    COUNT(b.ss_item_sk) as item_count,
    SUM(b.ss_net_paid) as total_spent
FROM 
    dfs.hdfs.tpcds_1gb_parquet.customer a
JOIN 
    DremioB.hdfs.tpcds_1gb_parquet.store_sales b
ON 
    a.c_customer_sk = b.ss_customer_sk
WHERE 
    a.c_birth_year BETWEEN 1980 AND 1990
GROUP BY 
    a.c_customer_id,
    a.c_first_name,
    a.c_last_name,
    a.c_email_address
HAVING 
    SUM(b.ss_net_paid) > 500
ORDER BY 
    total_spent DESC
LIMIT 100; 