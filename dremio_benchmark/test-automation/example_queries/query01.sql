-- TPC-DS Query 1
-- Pricing Summary Report Query

SELECT 
    avg(ss_quantity),
    avg(ss_ext_sales_price),
    avg(ss_ext_wholesale_cost),
    sum(ss_ext_wholesale_cost)
FROM 
    dfs.hdfs.tpcds_1gb_parquet.store_sales
    , dfs.hdfs.tpcds_1gb_parquet.date_dim
    , dfs.hdfs.tpcds_1gb_parquet.customer_address
    , dfs.hdfs.tpcds_1gb_parquet.item
WHERE 
    ss_sold_date_sk = d_date_sk
    AND d_year = 2001
    AND ss_addr_sk = ca_address_sk
    AND ca_gmt_offset = -5
    AND i_item_sk = ss_item_sk
    AND i_manager_id = 1
GROUP BY
    i_brand,
    i_brand_id,
    i_item_id,
    i_item_desc
ORDER BY
    ext_price_sum DESC, 
    brand_id ASC
LIMIT 100; 