-- ===========================================================================
--  Loading data from bronze to silver
--
--
--
-- ============================================================================
USE silver;
/*
----------------------------------------------
 Loading silver_crm_olist_customer_dataset
----------------------------------------------
 */
SET  @start_time=NOW();
-- truncate the table first and do a full load.
TRUNCATE  silver_crm_olist_customers_dataset;
 -- loading full data.
 
INSERT INTO silver_crm_olist_customers_dataset
 (customer_id , 
customer_unique_id ,
customer_zip_code_prefix ,
customer_city, 
customer_state )
SELECT customer_id , 
customer_unique_id ,
customer_zip_code_prefix ,
UPPER(customer_city), 
UPPER(customer_state)
 FROM  bronze.bronze_crm_olist_customers_dataset;
 SET  @end_time=NOW();
SELECT concat(TIMEDIFF(@end_time, @start_time) ," seconds olist_customers_dataset load time ") ;
 
 /*
----------------------------------------------
 Loading silver_erp_olist_geolocation_dataset
----------------------------------------------
 */
 SET  @start_time=NOW();
-- truncate table before running a full load.
TRUNCATE silver_erp_olist_geolocation_dataset;
-- doing a full load of silver_erp_olist_geolocation_dataset
INSERT INTO silver_erp_olist_geolocation_dataset(
geolocation_zip_code_prefix, 
geolocation_lat,
geolocation_lng,
geolocation_city,
geolocation_state)
SELECT 
geolocation_zip_code_prefix, 
geolocation_lat,
geolocation_lng,
UPPER(TRIM(geolocation_city)),
UPPER(TRIM(geolocation_state) )
FROM  bronze.bronze_erp_olist_geolocation_dataset;
 SET  @end_time=NOW();
SELECT concat(TIMEDIFF(@end_time, @start_time) ," seconds olist_geolocation_dataset load time ") ;
/*
----------------------------------------------
 Loading silver_erp_olist_order_items_dataset
----------------------------------------------
 */
 SET  @start_time=NOW();
-- truncate table before running a full load.
TRUNCATE silver_erp_olist_order_items_dataset;
INSERT INTO silver_erp_olist_order_items_dataset(
order_id,
order_item_id, 
product_id, 
seller_id,
shipping_limit_date,
price,
freight_value)
SELECT
order_id,
order_item_id, 
product_id, 
seller_id,
shipping_limit_date,
price,
freight_value  
FROM(
		 SELECT
		order_id,
		order_item_id, 
		product_id, 
		seller_id,
		shipping_limit_date,
		price ,
		freight_value,
		 ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY price DESC) AS rank_flag
FROM
 bronze.bronze_erp_olist_order_items_dataset )t
WHERE rank_flag=1;
SET  @end_time=NOW();
SELECT concat(TIMEDIFF(@end_time, @start_time) ," seconds olist_order_items_dataset load time ") ;


/*
----------------------------------------------
 Loading silver_erp_olist_order_payments_dataset
----------------------------------------------
 */
 SET  @start_time=NOW();
 -- truncate before full load
 TRUNCATE silver_erp_olist_order_payments_dataset;
 INSERT INTO silver_erp_olist_order_payments_dataset(
 order_id,
payment_sequential,
payment_type,
payment_installments,
payment_value
)
WITH cte_order_payments  AS (
	SELECT
	order_id,
	payment_sequential,
	payment_type,
	payment_installments,
	payment_value
	FROM bronze.bronze_erp_olist_order_payments_dataset)
SELECT 
order_id,
payment_sequential,
payment_type,
payment_installments,
payment_value
FROM cte_order_payments;

/*
----------------------------------------------
 Loading silver_erp_olist_orders_dataset
----------------------------------------------
 */
 SET  @start_time=NOW();
 -- truncate before full load
 TRUNCATE silver_erp_olist_orders_dataset;
 INSERT INTO silver_erp_olist_orders_dataset
(
order_id,
customer_id,
order_status ,
order_purchase_timestamp ,
order_approved_at , 
order_delivered_carrier_date ,
order_delivered_customer_date ,
order_estimated_delivery_date 
)
 WITH cte_orders AS
 (
SELECT
order_id,
customer_id,
order_status ,
order_purchase_timestamp, 
order_approved_at , 
order_delivered_carrier_date ,
order_delivered_customer_date ,
order_estimated_delivery_date 
FROM bronze.bronze_erp_olist_orders_dataset 
)
SELECT 
order_id,
customer_id,
order_status ,
CASE
	WHEN order_purchase_timestamp = ' ' THEN NULL
	ELSE
	order_purchase_timestamp
END,
CASE
	WHEN
	order_approved_at ='' THEN NULL
	ELSE order_approved_at
END,
CASE
	WHEN
	order_delivered_carrier_date ='' THEN NULL
	ELSE
	order_delivered_carrier_date
END,
CASE
	WHEN
	order_delivered_customer_date ='' THEN NULL
	ELSE
	order_delivered_customer_date
END,
order_estimated_delivery_date 
FROM cte_orders;
SET  @end_time=NOW();
SELECT concat(TIMEDIFF(@end_time, @start_time) ," seconds olist_order_items_dataset load time ") ;



/*
----------------------------------------------
 Loading silver_erp_olist_products_dataset
----------------------------------------------
 */
 
 SET  @start_time=NOW();
 -- truncate before full load
 TRUNCATE silver_erp_olist_products_dataset;
 INSERT INTO silver_erp_olist_products_dataset
 (
 product_id,
product_category_name,
product_name_lenght,
product_description_lenght,
product_photos_qty, 
product_weight_g, 
product_length_cm,
product_height_cm, 
product_width_cm
)
WITH cte_load_products AS 
(
SELECT
 product_id,
product_category_name,
product_name_lenght,
product_description_lenght ,
product_photos_qty , 
product_weight_g , 
product_length_cm ,
product_height_cm , 
product_width_cm 
FROM bronze.bronze_erp_olist_products_dataset)
SELECT 
product_id,
CASE
WHEN UPPER(product_category_name) ='' THEN 'n/a'
ELSE
product_category_name

END,

CASE
WHEN product_name_lenght  ='' THEN 0
ELSE
product_name_lenght
END,

CASE
WHEN product_description_lenght  ='' THEN 0
ELSE
product_description_lenght

END,
  CASE
WHEN product_photos_qty ='' THEN 0
ELSE
product_photos_qty

END,
  CASE
WHEN product_weight_g  ='' THEN 0

ELSE
product_weight_g
END,
  
CASE
WHEN UPPER(product_length_cm) ='' THEN 0
ELSE
product_length_cm
END,
CASE
WHEN UPPER(product_height_cm) ='' THEN 0
ELSE
product_height_cm
END,
CASE
WHEN UPPER(product_width_cm) ='' THEN 0
ELSE
product_width_cm

END

FROM cte_load_products;
SET  @end_time=NOW();
SELECT concat(TIMEDIFF(@end_time, @start_time) ," seconds olist_products_dataset load time ") ;

/*
----------------------------------------------
 Loading silver_erp_olist_sellers_dataset
----------------------------------------------
 */
  SET  @start_time=NOW();
 -- truncate before full load
 TRUNCATE silver_erp_olist_sellers_dataset;
INSERT INTO silver_erp_olist_sellers_dataset
(
seller_id,
seller_zip_code_prefix,
seller_city, 
seller_state
)
WITH cte_load_sellers AS 
(
SELECT
seller_id,
seller_zip_code_prefix,
seller_city, 
seller_state
FROM bronze.bronze_erp_olist_sellers_dataset
)
SELECT
seller_id,
seller_zip_code_prefix,
UPPER(seller_city), 
UPPER(seller_state)
FROM cte_load_sellers;
SET  @end_time=NOW();
SELECT concat(TIMEDIFF(@end_time, @start_time) ," seconds olist_sellers_dataset load time ") ;


/*
----------------------------------------------
 Loading silver_erp_product_category_name_translation
----------------------------------------------
 */
   SET  @start_time=NOW();
 -- truncate before full load
 TRUNCATE silver_erp_product_category_name_translation;
 
INSERT INTO silver_erp_product_category_name_translation
(
product_category_name,
product_category_name_english
)
WITH cte_load_product_category_name AS (
SELECT
product_category_name,
product_category_name_english 
FROM
bronze.bronze_erp_product_category_name_translation
)
SELECT
UPPER(product_category_name),
UPPER(product_category_name_english) 
FROM cte_load_product_category_name;
SET  @end_time=NOW();
SELECT concat(TIMEDIFF(@end_time, @start_time) ," seconds olist_product_category_name_translation load time ") ;


/*
----------------------------------------------
 Loading silver_crm_olist_customers_temp
 ---------------------------------------------
 */
   SET  @start_time=NOW();
 -- truncate before full load
 TRUNCATE silver_crm_olist_customers_temp;

INSERT INTO silver_crm_olist_customers_temp
(
 customer_unique_id,
customer_name_surrogate)
With cte_load_customers_temp AS (
SELECT
customer_unique_id,
customer_name_surrogate
FROM bronze.bronze_crm_olist_customers_temp)
SELECT customer_unique_id,
customer_name_surrogate FROM cte_load_customers_temp;
 