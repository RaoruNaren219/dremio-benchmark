-- Function to ingest data for a specific schema and table
CREATE OR REPLACE FUNCTION ingest_tpcds_data(schema_name VARCHAR, table_name VARCHAR, scale_factor INTEGER)
RETURNS void AS $$
BEGIN
    -- Construct the path to the Parquet file
    DECLARE
        parquet_path VARCHAR := format('/tpcds_parquet/sf%d/%s.parquet', scale_factor, table_name);
    BEGIN
        -- Load data using COPY INTO
        EXECUTE format('
        COPY INTO %I.%I
        FROM (SELECT * FROM parquet.`%s`)
        ', schema_name, table_name, parquet_path);
    END;
END;
$$ LANGUAGE plpgsql;

-- Function to ingest all tables for a specific scale factor
CREATE OR REPLACE FUNCTION ingest_scale_factor(schema_name VARCHAR, scale_factor INTEGER)
RETURNS void AS $$
BEGIN
    -- List of all TPC-DS tables
    PERFORM ingest_tpcds_data(schema_name, 'call_center', scale_factor);
    PERFORM ingest_tpcds_data(schema_name, 'catalog_sales', scale_factor);
    PERFORM ingest_tpcds_data(schema_name, 'customer', scale_factor);
    PERFORM ingest_tpcds_data(schema_name, 'customer_address', scale_factor);
    PERFORM ingest_tpcds_data(schema_name, 'date_dim', scale_factor);
    PERFORM ingest_tpcds_data(schema_name, 'item', scale_factor);
    PERFORM ingest_tpcds_data(schema_name, 'store_sales', scale_factor);
    PERFORM ingest_tpcds_data(schema_name, 'store', scale_factor);
    PERFORM ingest_tpcds_data(schema_name, 'time_dim', scale_factor);
    PERFORM ingest_tpcds_data(schema_name, 'warehouse', scale_factor);
END;
$$ LANGUAGE plpgsql;

-- Ingest data for each scale factor
SELECT ingest_scale_factor('tpcds_sf1', 1);
SELECT ingest_scale_factor('tpcds_sf10', 10);
SELECT ingest_scale_factor('tpcds_sf100', 100);

-- Verify data ingestion
SELECT 
    schema_name,
    table_name,
    COUNT(*) as row_count
FROM (
    SELECT 'tpcds_sf1' as schema_name, 'call_center' as table_name, COUNT(*) FROM tpcds_sf1.call_center UNION ALL
    SELECT 'tpcds_sf1', 'catalog_sales', COUNT(*) FROM tpcds_sf1.catalog_sales UNION ALL
    SELECT 'tpcds_sf1', 'customer', COUNT(*) FROM tpcds_sf1.customer UNION ALL
    SELECT 'tpcds_sf1', 'customer_address', COUNT(*) FROM tpcds_sf1.customer_address UNION ALL
    SELECT 'tpcds_sf1', 'date_dim', COUNT(*) FROM tpcds_sf1.date_dim UNION ALL
    SELECT 'tpcds_sf1', 'item', COUNT(*) FROM tpcds_sf1.item UNION ALL
    SELECT 'tpcds_sf1', 'store_sales', COUNT(*) FROM tpcds_sf1.store_sales UNION ALL
    SELECT 'tpcds_sf1', 'store', COUNT(*) FROM tpcds_sf1.store UNION ALL
    SELECT 'tpcds_sf1', 'time_dim', COUNT(*) FROM tpcds_sf1.time_dim UNION ALL
    SELECT 'tpcds_sf1', 'warehouse', COUNT(*) FROM tpcds_sf1.warehouse UNION ALL
    SELECT 'tpcds_sf10', 'call_center', COUNT(*) FROM tpcds_sf10.call_center UNION ALL
    SELECT 'tpcds_sf10', 'catalog_sales', COUNT(*) FROM tpcds_sf10.catalog_sales UNION ALL
    SELECT 'tpcds_sf10', 'customer', COUNT(*) FROM tpcds_sf10.customer UNION ALL
    SELECT 'tpcds_sf10', 'customer_address', COUNT(*) FROM tpcds_sf10.customer_address UNION ALL
    SELECT 'tpcds_sf10', 'date_dim', COUNT(*) FROM tpcds_sf10.date_dim UNION ALL
    SELECT 'tpcds_sf10', 'item', COUNT(*) FROM tpcds_sf10.item UNION ALL
    SELECT 'tpcds_sf10', 'store_sales', COUNT(*) FROM tpcds_sf10.store_sales UNION ALL
    SELECT 'tpcds_sf10', 'store', COUNT(*) FROM tpcds_sf10.store UNION ALL
    SELECT 'tpcds_sf10', 'time_dim', COUNT(*) FROM tpcds_sf10.time_dim UNION ALL
    SELECT 'tpcds_sf10', 'warehouse', COUNT(*) FROM tpcds_sf10.warehouse UNION ALL
    SELECT 'tpcds_sf100', 'call_center', COUNT(*) FROM tpcds_sf100.call_center UNION ALL
    SELECT 'tpcds_sf100', 'catalog_sales', COUNT(*) FROM tpcds_sf100.catalog_sales UNION ALL
    SELECT 'tpcds_sf100', 'customer', COUNT(*) FROM tpcds_sf100.customer UNION ALL
    SELECT 'tpcds_sf100', 'customer_address', COUNT(*) FROM tpcds_sf100.customer_address UNION ALL
    SELECT 'tpcds_sf100', 'date_dim', COUNT(*) FROM tpcds_sf100.date_dim UNION ALL
    SELECT 'tpcds_sf100', 'item', COUNT(*) FROM tpcds_sf100.item UNION ALL
    SELECT 'tpcds_sf100', 'store_sales', COUNT(*) FROM tpcds_sf100.store_sales UNION ALL
    SELECT 'tpcds_sf100', 'store', COUNT(*) FROM tpcds_sf100.store UNION ALL
    SELECT 'tpcds_sf100', 'time_dim', COUNT(*) FROM tpcds_sf100.time_dim UNION ALL
    SELECT 'tpcds_sf100', 'warehouse', COUNT(*) FROM tpcds_sf100.warehouse
) counts
ORDER BY schema_name, table_name; 