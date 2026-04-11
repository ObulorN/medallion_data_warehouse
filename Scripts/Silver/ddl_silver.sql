/*
                                 Silver Schema
------------------------------------------------------------------------------------------------
- Create silver Schema for Data Warehouse. Be extremely careful beforee running this schema, 
-  for it drop the entire schema before recreating a fresh one

-------------------------------------------------------------------------------------------------
*/


DROP SCHEMA IF EXISTS silver;
CREATE SCHEMA silver;
USE silver;
# create crm ddl
CREATE TABLE silver_crm_olist_customers_dataset(customer_id VARCHAR(250),customer_unique_id VARCHAR(250), customer_zip_code_prefix VARCHAR(50),customer_city VARCHAR(100),customer_state VARCHAR(50),dwh_create_date DATETIME DEFAULT NOW());

# create erp ddl
CREATE TABLE silver_erp_olist_geolocation_dataset(geolocation_zip_code_prefix VARCHAR(50),geolocation_lat FLOAT,geolocation_lng FLOAT,geolocation_city VARCHAR(200),geolocation_state VARCHAR(10),dwh_create_date DATETIME DEFAULT NOW());

CREATE TABLE silver_erp_olist_order_items_dataset(order_id VARCHAR(250),order_item_id INT,product_id VARCHAR(250),seller_id VARCHAR(250),shipping_limit_date DATETIME,price FLOAT,freight_value FLOAT,dwh_create_date DATETIME DEFAULT NOW());

CREATE TABLE silver_erp_olist_order_payments_dataset(order_id VARCHAR(250),payment_sequential INT,payment_type VARCHAR(50),payment_installments INT,payment_value FLOAT,dwh_create_date DATETIME DEFAULT NOW());

CREATE TABLE silver_erp_olist_orders_dataset(order_id VARCHAR(250),customer_id VARCHAR(250), order_status VARCHAR(100), order_purchase_timestamp TIMESTAMP, order_approved_at TIMESTAMP,order_delivered_carrier_date TIMESTAMP,order_delivered_customer_date TIMESTAMP, order_estimated_delivery_date TIMESTAMP,dwh_create_date DATETIME DEFAULT NOW());

CREATE TABLE silver_erp_olist_products_dataset(product_id VARCHAR(250),product_category_name VARCHAR(255),product_name_lenght INT ,product_description_lenght INT,product_photos_qty INT,product_weight_g INT,product_length_cm INT,product_height_cm INT,product_width_cm INT,dwh_create_date DATETIME DEFAULT NOW());

CREATE TABLE silver_erp_olist_sellers_dataset(seller_id VARCHAR(250),seller_zip_code_prefix VARCHAR(50),seller_city VARCHAR(100),seller_state VARCHAR(50),dwh_create_date DATETIME DEFAULT NOW());

CREATE TABLE silver_erp_product_category_name_translation(product_category_name VARCHAR(255),product_category_name_english VARCHAR(255),dwh_create_date DATETIME DEFAULT NOW());

CREATE TABLE silver_crm_olist_customers_temp(customer_unique_id VARCHAR(255),customer_name_surrogate VARCHAR(200));