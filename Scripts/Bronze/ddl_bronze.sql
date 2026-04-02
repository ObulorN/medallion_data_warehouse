/*
                                 Bronze Schema
------------------------------------------------------------------------------------------------
- Create bronze Schema for Data Warehouse. Be extremely careful beforee running this schema, 
-  for it drop the entire schema before recreating a fresh one

-------------------------------------------------------------------------------------------------
*/


DROP SCHEMA IF EXISTS bronze;
CREATE SCHEMA bronze;
USE bronze;
# create crm ddl
CREATE TABLE bronze_crm_olist_customers_dataset(customer_id VARCHAR(250),customer_unique_id VARCHAR(250), customer_zip_code_prefix VARCHAR(50),customer_city VARCHAR(100),customer_state VARCHAR(50));

# create erp ddl
CREATE TABLE bronze_erp_olist_geolocation_dataset(geolocation_zip_code_prefix VARCHAR(50),geolocation_lat FLOAT,geolocation_lng FLOAT,geolocation_city VARCHAR(200),geolocation_state VARCHAR(10));

CREATE TABLE bronze_erp_olist_order_items_dataset(order_id VARCHAR(250),order_item_id INT,product_id VARCHAR(250),seller_id VARCHAR(250),shipping_limit_date DATETIME,price FLOAT,freight_value FLOAT);

CREATE TABLE bronze_erp_olist_order_payments_dataset(order_id VARCHAR(250),payment_sequential INT,payment_type VARCHAR(50),payment_installments INT,payment_value FLOAT);

CREATE TABLE bronze_erp_olist_orders_dataset(order_id VARCHAR(250),customer_id VARCHAR(250), order_status VARCHAR(100), order_purchase_timestamp VARCHAR(100), order_approved_at VARCHAR(100),order_delivered_carrier_date VARCHAR(100),order_delivered_customer_date VARCHAR(100), order_estimated_delivery_date VARCHAR(100));


CREATE TABLE bronze_erp_olist_products_dataset(product_id VARCHAR(250),product_category_name VARCHAR(255),product_name_lenght VARCHAR(255) ,product_description_lenght VARCHAR(255),product_photos_qty VARCHAR(255),product_weight_g VARCHAR(255),product_length_cm VARCHAR(255),product_height_cm VARCHAR(255),product_width_cm VARCHAR(255));


CREATE TABLE bronze_erp_olist_sellers_dataset(seller_id VARCHAR(250),seller_zip_code_prefix VARCHAR(50),seller_city VARCHAR(100),seller_state VARCHAR(50));

CREATE TABLE bronze_erp_product_category_name_translation(product_category_name VARCHAR(255),product_category_name_english VARCHAR(255));

CREATE TABLE bronze_crm_olist_customers_temp(customer_unique_id VARCHAR(255),customer_name_surrogate VARCHAR(200));
