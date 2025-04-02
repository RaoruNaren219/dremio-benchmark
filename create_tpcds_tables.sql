-- Create schemas for different scale factors
CREATE SCHEMA IF NOT EXISTS tpcds_sf1;
CREATE SCHEMA IF NOT EXISTS tpcds_sf10;
CREATE SCHEMA IF NOT EXISTS tpcds_sf100;

-- Function to create tables for a specific schema
CREATE OR REPLACE FUNCTION create_tpcds_tables(schema_name VARCHAR)
RETURNS void AS $$
BEGIN
    -- Call Center table
    EXECUTE format('
    CREATE TABLE IF NOT EXISTS %I.call_center (
        cc_call_center_sk BIGINT,
        cc_call_center_id VARCHAR(16),
        cc_rec_start_date DATE,
        cc_rec_end_date DATE,
        cc_closed_date_sk BIGINT,
        cc_open_date_sk BIGINT,
        cc_name VARCHAR(50),
        cc_class VARCHAR(50),
        cc_employees INTEGER,
        cc_sq_ft INTEGER,
        cc_hours VARCHAR(20),
        cc_manager VARCHAR(40),
        cc_mkt_id INTEGER,
        cc_mkt_class VARCHAR(50),
        cc_mkt_desc VARCHAR(100),
        cc_market_manager VARCHAR(40),
        cc_division INTEGER,
        cc_division_name VARCHAR(50),
        cc_company INTEGER,
        cc_company_name VARCHAR(50),
        cc_street_number VARCHAR(10),
        cc_street_name VARCHAR(60),
        cc_street_type VARCHAR(15),
        cc_suite_number VARCHAR(10),
        cc_city VARCHAR(60),
        cc_county VARCHAR(30),
        cc_state VARCHAR(2),
        cc_zip VARCHAR(10),
        cc_country VARCHAR(20),
        cc_gmt_offset DECIMAL(5,2),
        cc_tax_percentage DECIMAL(5,2)
    )', schema_name);

    -- Catalog Sales table
    EXECUTE format('
    CREATE TABLE IF NOT EXISTS %I.catalog_sales (
        cs_sold_date_sk BIGINT,
        cs_sold_time_sk BIGINT,
        cs_ship_date_sk BIGINT,
        cs_bill_customer_sk BIGINT,
        cs_bill_cdemo_sk BIGINT,
        cs_bill_hdemo_sk BIGINT,
        cs_bill_addr_sk BIGINT,
        cs_ship_customer_sk BIGINT,
        cs_ship_cdemo_sk BIGINT,
        cs_ship_hdemo_sk BIGINT,
        cs_ship_addr_sk BIGINT,
        cs_call_center_sk BIGINT,
        cs_catalog_page_sk BIGINT,
        cs_ship_mode_sk BIGINT,
        cs_warehouse_sk BIGINT,
        cs_item_sk BIGINT,
        cs_promo_sk BIGINT,
        cs_order_number BIGINT,
        cs_quantity INTEGER,
        cs_wholesale_cost DECIMAL(7,2),
        cs_list_price DECIMAL(7,2),
        cs_sales_price DECIMAL(7,2),
        cs_ext_discount_amt DECIMAL(7,2),
        cs_ext_sales_price DECIMAL(7,2),
        cs_ext_wholesale_cost DECIMAL(7,2),
        cs_ext_list_price DECIMAL(7,2),
        cs_ext_tax DECIMAL(7,2),
        cs_coupon_amt DECIMAL(7,2),
        cs_ext_ship_cost DECIMAL(7,2),
        cs_net_paid DECIMAL(7,2),
        cs_net_paid_inc_tax DECIMAL(7,2),
        cs_net_paid_inc_ship DECIMAL(7,2),
        cs_net_paid_inc_ship_tax DECIMAL(7,2),
        cs_net_profit DECIMAL(7,2)
    )', schema_name);

    -- Customer table
    EXECUTE format('
    CREATE TABLE IF NOT EXISTS %I.customer (
        c_customer_sk BIGINT,
        c_customer_id VARCHAR(16),
        c_current_cdemo_sk BIGINT,
        c_current_hdemo_sk BIGINT,
        c_current_addr_sk BIGINT,
        c_first_shipto_date_sk BIGINT,
        c_first_sales_date_sk BIGINT,
        c_salutation VARCHAR(10),
        c_first_name VARCHAR(20),
        c_last_name VARCHAR(30),
        c_preferred_cust_flag CHAR(1),
        c_birth_day INTEGER,
        c_birth_month INTEGER,
        c_birth_year INTEGER,
        c_birth_country VARCHAR(20),
        c_login VARCHAR(13),
        c_email_address VARCHAR(50),
        c_last_review_date_sk BIGINT
    )', schema_name);

    -- Customer Address table
    EXECUTE format('
    CREATE TABLE IF NOT EXISTS %I.customer_address (
        ca_address_sk BIGINT,
        ca_address_id VARCHAR(16),
        ca_street_number VARCHAR(10),
        ca_street_name VARCHAR(60),
        ca_street_type VARCHAR(15),
        ca_suite_number VARCHAR(10),
        ca_city VARCHAR(60),
        ca_county VARCHAR(30),
        ca_state VARCHAR(2),
        ca_zip VARCHAR(10),
        ca_country VARCHAR(20),
        ca_gmt_offset DECIMAL(5,2),
        ca_location_type VARCHAR(20)
    )', schema_name);

    -- Date Dimension table
    EXECUTE format('
    CREATE TABLE IF NOT EXISTS %I.date_dim (
        d_date_sk BIGINT,
        d_date_id VARCHAR(16),
        d_date DATE,
        d_month_seq INTEGER,
        d_week_seq INTEGER,
        d_quarter_seq INTEGER,
        d_year INTEGER,
        d_dow INTEGER,
        d_moy INTEGER,
        d_dom INTEGER,
        d_qoy INTEGER,
        d_fy_year INTEGER,
        d_fy_quarter_seq INTEGER,
        d_fy_week_seq INTEGER,
        d_day_name VARCHAR(9),
        d_quarter_name VARCHAR(6),
        d_holiday CHAR(1),
        d_weekend CHAR(1),
        d_following_holiday CHAR(1),
        d_first_dom INTEGER,
        d_last_dom INTEGER,
        d_same_day_ly INTEGER,
        d_same_day_lq INTEGER,
        d_current_day CHAR(1),
        d_current_week CHAR(1),
        d_current_month CHAR(1),
        d_current_quarter CHAR(1),
        d_current_year CHAR(1)
    )', schema_name);

    -- Item table
    EXECUTE format('
    CREATE TABLE IF NOT EXISTS %I.item (
        i_item_sk BIGINT,
        i_item_id VARCHAR(16),
        i_rec_start_date DATE,
        i_rec_end_date DATE,
        i_item_desc VARCHAR(200),
        i_current_price DECIMAL(7,2),
        i_wholesale_cost DECIMAL(7,2),
        i_brand_id INTEGER,
        i_brand VARCHAR(50),
        i_class_id INTEGER,
        i_class VARCHAR(50),
        i_category_id INTEGER,
        i_category VARCHAR(50),
        i_manufact_id INTEGER,
        i_manufact VARCHAR(50),
        i_size VARCHAR(20),
        i_formulation VARCHAR(20),
        i_color VARCHAR(20),
        i_units VARCHAR(10),
        i_container VARCHAR(10),
        i_manager_id INTEGER,
        i_product_name VARCHAR(50)
    )', schema_name);

    -- Store Sales table
    EXECUTE format('
    CREATE TABLE IF NOT EXISTS %I.store_sales (
        ss_sold_date_sk BIGINT,
        ss_sold_time_sk BIGINT,
        ss_item_sk BIGINT,
        ss_customer_sk BIGINT,
        ss_cdemo_sk BIGINT,
        ss_hdemo_sk BIGINT,
        ss_addr_sk BIGINT,
        ss_store_sk BIGINT,
        ss_promo_sk BIGINT,
        ss_ticket_number BIGINT,
        ss_quantity INTEGER,
        ss_wholesale_cost DECIMAL(7,2),
        ss_list_price DECIMAL(7,2),
        ss_sales_price DECIMAL(7,2),
        ss_ext_discount_amt DECIMAL(7,2),
        ss_ext_sales_price DECIMAL(7,2),
        ss_ext_wholesale_cost DECIMAL(7,2),
        ss_ext_list_price DECIMAL(7,2),
        ss_ext_tax DECIMAL(7,2),
        ss_coupon_amt DECIMAL(7,2),
        ss_net_paid DECIMAL(7,2),
        ss_net_paid_inc_tax DECIMAL(7,2),
        ss_net_profit DECIMAL(7,2)
    )', schema_name);

    -- Store table
    EXECUTE format('
    CREATE TABLE IF NOT EXISTS %I.store (
        s_store_sk BIGINT,
        s_store_id VARCHAR(16),
        s_rec_start_date DATE,
        s_rec_end_date DATE,
        s_closed_date_sk BIGINT,
        s_store_name VARCHAR(50),
        s_number_employees INTEGER,
        s_floor_space INTEGER,
        s_hours VARCHAR(20),
        s_manager VARCHAR(40),
        s_market_id INTEGER,
        s_geography_class VARCHAR(100),
        s_market_desc VARCHAR(100),
        s_market_manager VARCHAR(40),
        s_division_id INTEGER,
        s_division_name VARCHAR(50),
        s_company_id INTEGER,
        s_company_name VARCHAR(50),
        s_street_number VARCHAR(10),
        s_street_name VARCHAR(60),
        s_street_type VARCHAR(15),
        s_suite_number VARCHAR(10),
        s_city VARCHAR(60),
        s_county VARCHAR(30),
        s_state VARCHAR(2),
        s_zip VARCHAR(10),
        s_country VARCHAR(20),
        s_gmt_offset DECIMAL(5,2),
        s_tax_precentage DECIMAL(5,2)
    )', schema_name);

    -- Time Dimension table
    EXECUTE format('
    CREATE TABLE IF NOT EXISTS %I.time_dim (
        t_time_sk BIGINT,
        t_time_id VARCHAR(16),
        t_time INTEGER,
        t_hour INTEGER,
        t_minute INTEGER,
        t_second INTEGER,
        t_am_pm VARCHAR(2),
        t_shift VARCHAR(20),
        t_sub_shift VARCHAR(20),
        t_meal_time VARCHAR(20)
    )', schema_name);

    -- Warehouse table
    EXECUTE format('
    CREATE TABLE IF NOT EXISTS %I.warehouse (
        w_warehouse_sk BIGINT,
        w_warehouse_id VARCHAR(16),
        w_warehouse_name VARCHAR(20),
        w_warehouse_sq_ft INTEGER,
        w_street_number VARCHAR(10),
        w_street_name VARCHAR(60),
        w_street_type VARCHAR(15),
        w_suite_number VARCHAR(10),
        w_city VARCHAR(60),
        w_county VARCHAR(30),
        w_state VARCHAR(2),
        w_zip VARCHAR(10),
        w_country VARCHAR(20),
        w_gmt_offset DECIMAL(5,2)
    )', schema_name);
END;
$$ LANGUAGE plpgsql;

-- Create tables for each scale factor
SELECT create_tpcds_tables('tpcds_sf1');
SELECT create_tpcds_tables('tpcds_sf10');
SELECT create_tpcds_tables('tpcds_sf100'); 