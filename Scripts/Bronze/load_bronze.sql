/*
							BRONZE LAYER
----------------------------------------------------------------------------
|	This an data Ingestion script that loads data from the source 'as is' to the 
|	to the bronze layer of the data warehouse.															
|  WARNING!!
|  The script first truncates the table before running a full Load, so you are 
|    advice to run the script with caution.
----------------------------------------------------------------------------
*/
# initialize the bronze_schema
USE bronze;

/*
==============================================
Loading bronze_erp_olist_order_items_dataset  
==============================================
*/
SET  @start_time=NOW();

SELECT ">> truncating order_items...";
TRUNCATE TABLE bronze_erp_olist_order_items_dataset; 
SELECT ">> truncating done...";
SELECT ">> laoding data to table...";
LOAD DATA INFILE 'D:/GitPrj/medallion_data_warehouse/DataSource/erp/olist_order_items_dataset.csv'
INTO TABLE  bronze_erp_olist_order_items_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
SET  @end_time=NOW();
SELECT concat(TIMEDIFF(@end_time, @start_time) ," seconds olist_order_items_dataset load time ") ;

/*
==============================================
Loading bronze_crm_olist_customers_dataset  
==============================================
*/
# truncate table 
SELECT ">> truncating table...";
TRUNCATE TABLE bronze_crm_olist_customers_dataset; 
SELECT ">> truncating done...";
SELECT ">> laoding data to table...";
LOAD DATA INFILE 'D:/GitPrj/medallion_data_warehouse/DataSource/crm/olist_customers_dataset.csv'
INTO TABLE  bronze_crm_olist_customers_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
SET  @end_time=NOW();
SELECT concat(TIMEDIFF(@end_time, @start_time) ," seconds olist_customers_dataset load time ") ;


/*
==============================================
Loading bronze_erp_olist_order_payments_dataset  
==============================================
*/
SELECT ">> truncating order_items...";
TRUNCATE TABLE bronze_erp_olist_order_payments_dataset; 
SELECT ">> truncating done...";
SELECT ">> laoding data to table...";
LOAD DATA INFILE 'D:/GitPrj/medallion_data_warehouse/DataSource/erp/olist_order_payments_dataset.csv'
INTO TABLE  bronze_erp_olist_order_payments_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
SET  @end_time=NOW();
SELECT concat(TIMEDIFF(@end_time, @start_time) ," seconds olist_order_payments_dataset load time ") ;

/*
==============================================
Loading bronze_erp_olist_orders_dataset  
==============================================
*/
SELECT ">> truncating order_items...";
TRUNCATE TABLE bronze_erp_olist_orders_dataset; 
SELECT ">> truncating done...";
SELECT ">> laoding data to table...";
LOAD DATA INFILE 'D:/GitPrj/medallion_data_warehouse/DataSource/erp/olist_orders_dataset.csv'
INTO TABLE  bronze_erp_olist_orders_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
SET  @end_time=NOW();
SELECT concat(TIMEDIFF(@end_time, @start_time) ," seconds olist_orders_dataset load time ") ;

/*
==============================================
Loading bronze_erp_product_category_name_translation
==============================================
*/
SELECT ">> truncating order_items...";
TRUNCATE TABLE bronze_erp_product_category_name_translation; 
SELECT ">> truncating done...";
SELECT ">> laoding data to table...";
LOAD DATA INFILE 'D:/GitPrj/medallion_data_warehouse/DataSource/erp/product_category_name_translation.csv'
INTO TABLE  bronze_erp_product_category_name_translation
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
SET  @end_time=NOW();
SELECT concat(TIMEDIFF(@end_time, @start_time) ," seconds product_category_name_translation load time ") ;

/*
==============================================
Loading bronze_erp_olist_products_dataset  
==============================================
*/
SELECT ">> truncating order_items...";
TRUNCATE TABLE bronze_erp_olist_products_dataset; 
SELECT ">> truncating done...";
SELECT ">> laoding data to table...";
LOAD DATA INFILE 'D:/GitPrj/medallion_data_warehouse/DataSource/erp/olist_products_dataset.csv'
INTO TABLE  bronze_erp_olist_products_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
SET  @end_time=NOW();
SELECT concat(TIMEDIFF(@end_time, @start_time) ," seconds olist_products_dataset load time ") ;



/*
==============================================
Loading bronze_erp_olist_sellers_dataset  
==============================================
*/
SELECT ">> truncating order_items...";
TRUNCATE TABLE bronze_erp_olist_sellers_dataset; 
SELECT ">> truncating done...";
SELECT ">> laoding data to table...";
LOAD DATA INFILE 'D:/GitPrj/medallion_data_warehouse/DataSource/erp/olist_sellers_dataset.csv'
INTO TABLE  bronze_erp_olist_sellers_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
SET  @end_time=NOW();
SELECT concat(TIMEDIFF(@end_time, @start_time) ," seconds olist_sellers_dataset load time ") ;


/*
==============================================
Loading bronze_erp_olist_geolocation_dataset  
==============================================
*/

SELECT ">> truncating geolocation...";
TRUNCATE TABLE bronze_erp_olist_geolocation_dataset; 
SELECT ">> truncating done...";
SELECT ">> laoding data to table...";
LOAD DATA INFILE 'D:/GitPrj/medallion_data_warehouse/DataSource/erp/olist_geolocation_dataset.csv'
INTO TABLE  bronze_erp_olist_geolocation_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
SET  @end_time=NOW();
SELECT concat(TIMEDIFF(@end_time, @start_time) ," seconds olist_geolocation_dataset load time ") ;

/*
==============================================
Loading bronze_crm_olist_customers_temp  
==============================================
*/
# truncate table 
SELECT ">> truncating table...";
TRUNCATE TABLE bronze_crm_olist_customers_temp ; 
SELECT ">> truncating done...";
SELECT ">> laoding data to table...";
LOAD DATA INFILE 'D:/GitPrj/medallion_data_warehouse/DataSource/crm/olist_customers_temp.csv'
INTO TABLE  bronze_crm_olist_customers_temp 
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
SET  @end_time=NOW();
SELECT concat(TIMEDIFF(@end_time, @start_time) ," seconds bronze_crm_olist_customers_temp  load time ") ;




