-- check columns for duplicate key and null values
-- check columns for trailing spaces

-- check for duplicate in customer_id
USE bronze;
SELECT customer_id,COUNT(*) FROM bronze.bronze_crm_olist_customers_dataset 
GROUP BY customer_id
HAVING COUNT(*)>1;

-- check for duplicate in customer_unique_id
USE bronze;
SELECT customer_unique_id,COUNT(*) FROM bronze.bronze_crm_olist_customers_dataset 
GROUP BY customer_unique_id
HAVING COUNT(*)>1;

-- check for null in customer_unique_id
USE bronze;
SELECT customer_unique_id FROM bronze.bronze_crm_olist_customers_dataset 
where customer_unique_id is null;

-- check for null in customer_zip_code_prefix
USE bronze;
SELECT customer_zip_code_prefix FROM bronze.bronze_crm_olist_customers_dataset 
where customer_zip_code_prefix is null;

-- check for null in customer_city
USE bronze;
SELECT customer_city FROM bronze.bronze_crm_olist_customers_dataset 
where customer_city is null;

-- check for null in customer_state
USE bronze;
SELECT customer_state FROM bronze.bronze_crm_olist_customers_dataset 
where customer_state is null;

-- checking for trailing spaces in customer_city
select customer_city from  bronze.bronze_crm_olist_customers_dataset 
where  customer_city!= TRIM(customer_city);

-- checking for trailing spaces in customer_state
select customer_state from  bronze.bronze_crm_olist_customers_dataset 
where  customer_state!= TRIM(customer_state);

-- ====================================================================
-- checking for duplicates in bronze_erp_olist_order_items
USE bronze;
SELECT order_id, count(*) from bronze.bronze_erp_olist_order_items_dataset
group by order_id
having count(*)>1;


 
select *  from (
 select order_id,order_item_id,seller_id,shipping_limit_date,price,freight_value,row_number() over(partition by order_id order by(price) desc) as t_flag
from bronze.bronze_erp_olist_order_items_dataset )t
where t_flag=1; 

-- checking for insert error 1292
select * from bronze.bronze_erp_olist_order_items_dataset where shipping_limit_date like '%00%';


-- check for  nulls in bronze.bronze_erp_olist_order_items_dataset
select * from bronze.bronze_erp_olist_order_items_dataset where seller_id is null;

select * from bronze.bronze_erp_olist_order_items_dataset where shipping_limit_date is null;
select * from bronze.bronze_erp_olist_order_items_dataset where price is null;










