/*
===========================================================================================
= This script creates and load the the Gold layer of the datawarehouse.
=
=
=
=
=
===========================================================================================																							=
*/

DROP SCHEMA IF EXISTS gold;
CREATE DATABASE gold;
USE gold;

CREATE VIEW gold.dim_customers AS (
SELECT  customer_id, 
ct.customer_unique_id,
customer_name_surrogate,
customer_zip_code_prefix,
customer_city,
customer_state
FROM silver.silver_crm_olist_customers_dataset c
LEFT JOIN silver.silver_crm_olist_customers_temp ct
ON c.customer_unique_id=ct.customer_unique_id );


CREATE VIEW gold.dim_products AS (
SELECT 
product_id,
product_category_name,
product_name_lenght,
product_description_lenght,
product_photos_qty,
product_weight_g, 
product_length_cm,
product_height_cm,
product_width_cm
FROM silver.silver_erp_olist_products_dataset
);

CREATE VIEW gold.fact_orders AS (
SELECT 
o.order_id,
o.customer_id,
order_item_id,
oi.product_id,
oi.seller_id,
oi.price,
oi.freight_value,
o.order_status,
o.order_purchase_timestamp,
o.order_approved_at,
o.order_delivered_carrier_date,
o.order_delivered_customer_date, 
o.order_estimated_delivery_date,
oi.shipping_limit_date
FROM silver.silver_erp_olist_orders_dataset o
LEFT JOIN silver.silver_erp_olist_order_items_dataset oi
ON o.order_id=oi.order_id
 
);

CREATE VIEW gold.fact_payment AS 
(
SELECT
order_id, 
payment_sequential,
payment_type,
(payment_installments),
payment_value,rn
 FROM
	(
    SELECT 
	order_id, 
	payment_sequential,
	payment_type,
	payment_installments,
	payment_value,
	ROW_NUMBER() OVER(PARTITION BY order_id, payment_sequential ORDER BY payment_value DESC) AS rn
	FROM silver.silver_erp_olist_order_payments_dataset
	)t
WHERE rn =1
 );

-- CREATE VIEW gold.dim_payment AS 
-- (
-- SELECT


-- );
