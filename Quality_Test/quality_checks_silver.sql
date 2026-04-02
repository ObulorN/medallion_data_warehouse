/*
===================================================================================================
* This scripts cjecks  for data quality in the silver layer.alter
* It ensures data consistency,  standazation and completeness  across the records of the silver layer.
*
* Thid includes checks for:
* Null values in colums
* Duplicates values in key columns
* Empty colunms
* String values with spaces etc
*
*
* NOTES:
* Make sure to run this script to validate data consistency. completeness and accuracy after loading  data to
* this layer.
==================================================================================================
*/



-- *********************************************************************
-- Quality check for silver_erp_olist_customers_dataset
-- ********************************************************************
-- check customers id for duplicate values
-- outcome should be No result
SELECT customer_id, COUNT(*) FROM silver_crm_olist_customers_dataset
GROUP BY customer_id
HAVING COUNT(*)>1;
-- check customers id for trailing spaces
-- outcome should be No result
SELECT customer_id FROM silver_crm_olist_customers_dataset
WHERE customer_id != TRIM(customer_id);

SELECT customer_unique_id FROM silver_crm_olist_customers_dataset
WHERE customer_unique_id != TRIM(customer_unique_id);

SELECT customer_city FROM silver_crm_olist_customers_dataset
WHERE customer_city != TRIM(customer_city);

-- *********************************************************************
-- Quality check for silver_erp_olist_geolocation_dataset
-- ********************************************************************
-- check customers id for duplicate values
-- outcome should be No result
SELECT geolocation_city FROM silver_erp_olist_geolocation_dataset
WHERE geolocation_city  != TRIM(geolocation_city);

SELECT geolocation_state FROM silver_erp_olist_geolocation_dataset
WHERE geolocation_state  != TRIM(geolocation_state);

-- *********************************************************************
-- Quality check for silver_erp_olist_order_items_dataset
-- ********************************************************************
-- check customers id for duplicate values
-- outcome should be No result
SELECT order_id, COUNT(*) FROM silver_erp_olist_order_items_dataset
GROUP BY order_id
HAVING COUNT(*) >1;

SELECT product_id FROM silver_erp_olist_order_items_dataset
WHERE product_id  != TRIM(product_id);

SELECT seller_id FROM silver_erp_olist_order_items_dataset
WHERE seller_id  != TRIM(seller_id);


-- *********************************************************************
-- Quality check for silver_erp_olist_order_payments_dataset
-- ********************************************************************
-- check customers id for duplicate values
-- outcome should be No result
SELECT order_id,COUNT(*) FROM silver_erp_olist_order_payments_dataset
GROUP BY order_id
HAVING COUNT(*) >1;


-- *********************************************************************
-- Quality check for silver_erp_olist_products_dataset
-- ********************************************************************
-- check customers id for duplicate values
-- outcome should be No result
SELECT product_id,COUNT(*) FROM silver_erp_olist_products_dataset
GROUP BY product_id
HAVING COUNT(*) >1;

SELECT product_id FROM silver_erp_olist_products_dataset
WHERE product_id !=product_id;


-- *********************************************************************
-- Quality check for silver_erp_olist_sellers_dataset
-- ********************************************************************
-- check customers id for duplicate values
-- outcome should be No result
SELECT seller_id ,COUNT(*) FROM silver_erp_olist_sellers_dataset
GROUP BY seller_id
HAVING COUNT(*)>1;

SELECT seller_id FROM silver_erp_olist_sellers_dataset
WHERE seller_id !=seller_id;

-- *********************************************************************
-- Quality check for silver_erp_product_category_name_translation
-- ********************************************************************
-- check customers id for duplicate values
-- outcome should be No result

SELECT DISTINCT UPPER(product_category_name) FROM silver_erp_product_category_name_translation;

SELECT DISTINCT UPPER(product_category_name_english) FROM silver_erp_product_category_name_translation;

SELECT customer_unique_id, count(*) FROM bronze.bronze_crm_olist_customers_temp
GROUP BY customer_unique_id
HAVING COUNT(*)>1;

-- failed test fix
SELECT * FROM bronze.bronze_crm_olist_customers_temp
WHERE customer_unique_id='b6c083700ca8c135ba9f0f132930d4e8';



