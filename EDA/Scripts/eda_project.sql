
/*
================================================================================
=                             EDA Project									   =
=	Note: this project contain scripts for the various eda analysis																	   =	
= 																			   =
= 																			   =
=																		       =
================================================================================
*/
USE gold;
/*
================================================================================
= DIMENSION Analysis										                   =
=	This section explores the structure of dimension tables				        =
=================================================================================
*/

-- what is our total count of customers?
WITH all_customers_count AS (
SELECT DISTINCT COUNT(customer_id) AS  total_customers_count
FROM dim_customers)
SELECT total_customers_count FROM all_customers_count;

-- what is the total count of customers who has made orders?

SELECT  COUNT(DISTINCT customer_id) AS  total_customers_WITH_orders
FROM fact_orders;



-- get a unique list of customers city?
SELECT DISTINCT customer_city
 FROM dim_customers
 ORDER BY customer_city 
 ;
   
-- what is the total count of customers WITH NO orders?

SELECT 
   COUNT( customer_id) count_of_customer_without_orders
FROM
    (SELECT 
        f.customer_id
    FROM
        fact_orders f
    LEFT JOIN dim_customers c ON f.customer_id = c.customer_id
    WHERE
        c.customer_id NOT IN (SELECT 
                customer_id
            FROM
                fact_orders)) t
;
    
-- five least customers
WITH least_five_customers AS (
SELECT c.customer_id, customer_name_surrogate,
sum(profit) OVER(PARTITION BY customer_id ORDER BY profit ASC) AS profit,
 DENSE_RANK() OVER(ORDER BY profit) AS rn
 FROM gold.fact_orders f 
 LEFT JOIN dim_customers c
ON 
 f.customer_id=c.customer_id)
 
 SELECT customer_name_surrogate,round(profit,2) AS least_customers
 FROM least_five_customers
 ORDER BY rn ASC
 LIMIT 5
;
 -- Identify customers WITH increasinging purchases
WITH cust_with_increase_purchase AS
 (  
	SELECT c.customer_id, customer_name_surrogate, sum(profit) AS total_profit,
	LEAD(sum(profit)) OVER(PARTITION BY customer_id  ORDER BY sum(profit)) AS LEAD_profit
	FROM fact_orders f
	INNER JOIN dim_customers c
	on
	f.customer_id=c.customer_id
	GROUP BY c.customer_id,customer_name_surrogate
)

SELECT customer_name_surrogate,LEAD_profit,total_profit
FROM cust_with_increase_purchase
ORDER BY total_profit DESC;

 -- get a unique list of product category?
SELECT DISTINCT product_category_name 
FROM dim_productS
ORDER BY product_category_name
;

SELECT DISTINCT count(*) AS review_count 
FROM dim_review
;

-- what is total catalog value in the Olist shop?

SELECT COUNT(DISTINCT product_id) as total_catalog_value
FROM 	dim_products
;


-- products with review_score from 3 ?
SELECT o.product_id,product_category_name ,review_score FROM fact_orders o 
LEFT JOIN dim_review r
ON 
o.order_id=r.order_id
INNER JOIN dim_products p 
ON
p.product_id=o.product_id
WHERE review_score >=3
GROUP BY product_id,product_category_name ,review_score
ORDER BY review_score DESC
;



/*
================================================================================
= MEASURES Analysis		(Key Metrics)   				    	               =
=													    		    	        =
=================================================================================
*/

-- what is total/ min/average/max profit for the Olist shop?                                                                                                                                                                             
SELECT  ROUND(sum(profit),2) as total_profit, ROUND(MIN(profit),2) AS min_profit,ROUND(AVG(profit),2) AS avg_profit, ROUND(MAX(profit),2) as max_profit
FROM fact_orders 
;

-- what is min/average/max product price in the Olist shop?
SELECT ROUND(MIN(price),2) AS min_price,ROUND(AVG(price),2) AS avg_price, ROUND(MAX(price),2) as max_price
FROM fact_orders 
;


/*
================================================================================
= PART To WHOLE analysis										                       =
=																		       =
=================================================================================
*/
-- what are the order_status pct distribution?
SELECT order_status,round((count/total)*100,2) AS pct
FROM(
	SELECT order_status,count(*) OVER()AS total,
	 count(*) OVER (PARTITION BY order_status ) AS count
	  FROM 
	fact_orders)t
GROUP BY order_status
ORDER BY pct DESC
;

-- what is the profit pct distribution of product category?

SELECT *,ROUND((category_profit/total_profit)*100,2) as   pct_profit
FROM
	(
	SELECT  product_category_name,
	ROUND(sum(profit)over(),2) as total_profit,
	ROUND(sum(profit) over(PARTITION BY product_category_name),2) as category_profit
	FROM fact_orders o
	LEFT JOIN dim_products p 
	ON
	o.product_id =p.product_id)t
GROUP BY product_category_name,category_profit,total_profit
ORDER BY category_profit DESC
LIMIT 10
;

/*
================================================================================
= CUMULATIVE analysis										                       =
=																		       =
=================================================================================
*/
-- calculate the yearly total and average sales?
SELECT order_date,
FORMAT(sum(total_profit) over(order by order_date),'c') as running_profit,
ROUND (avg(avg_profit) over(order by order_date),2) as moving_avg
FROM 
		(
		SELECT year(order_purchase_timestamp) as order_date, SUM(profit)as total_profit,
		avg(profit) as avg_profit
		FROM fact_orders
		GROUP BY year(order_purchase_timestamp))t
     order by order_date desc   
;
 -- what is the profit month on month?
  SELECT MONTH(order_purchase_timestamp)AS OrderMONTH, 
 round(sum(profit),2) AS current_profit,
 round( Lag(sum(profit)) OVER(ORDER BY MONTH(order_purchase_timestamp)),2) AS lag_profit,
 round(sum(profit) - Lag(sum(profit)) OVER(ORDER BY MONTH(order_purchase_timestamp)),2)  AS profit_differnce
  FROM
  gold.fact_orders
  GROUP BY
  MONTH(order_purchase_timestamp)
    ;
    
    -- find the next transaction profit per customer
   
    SELECT 
      customer_id, 
      MONTH(order_purchase_timestamp) AS purchase_month,
      sum(profit) AS total_profit,
       LEAD(sum(profit)) OVER(PARTITION BY customer_id ORDER BY  MONTH(order_purchase_timestamp)) AS next_profit
    FROM fact_orders
    GROUP BY
    MONTH(order_purchase_timestamp),
     customer_id
     ;
-- Quick Check: Run this simple count to see if your customers even have repeat business:
SELECT order_id, COUNT(DISTINCT MONTH(order_purchase_timestamp)) 
FROM fact_orders 
GROUP BY order_id 
HAVING COUNT(DISTINCT MONTH(order_purchase_timestamp))  > 1;
      
    
--
    
-- calculate day difference between consecutive orders

SELECT current_day,next_day, datediff(next_day,current_day)
FROM(
	SELECT order_purchase_timestamp AS current_day,
	LEAD(order_purchase_timestamp) OVER(ORDER BY order_purchase_timestamp) AS next_day
	FROM fact_orders
)t;

/*
================================================================================
= MADNITUDE analysis										                   =
=																		       =
=================================================================================
*/

 -- Best ten category based on profit
 
USE gold;
SELECT 
  product_category_name,
 round(sum(profit),2) as total_profit
 FROM gold.fact_orders o
 LEFT JOIN dim_products pd
ON
 o.product_id=pd.product_id
  GROUP BY product_category_name
 ORDER BY total_profit desc , product_category_name ASc
 limit 10
  ;
 
  -- what is the Least ten category based on profit? (Note: there seems to be some profit outlier in the output
 
 USE gold;
 WITH least_category AS (
	SELECT  product_category_name, sum(profit) AS total_profit 
    FROM gold.fact_orders o
	  LEFT JOIN dim_products pd
	ON
	 o.product_id=pd.product_id
    	  GROUP BY 
      pd.product_category_name
	  )
      
SELECT product_category_name,ROUND(total_profit,2)
FROM least_category
 ORDER BY total_profit ASc 
 LIMIT 10
  ;
  
 
 /*
================================================================================
= TIME INTELLIGENCE Analysis										                               =
=	This section focuses on analysing the dataset for location insight.        =
=================================================================================
*/
-- whats the min/max/ average purchase to customer delivare value and 

  USE gold;
 select  min(customer_days) as min_customer_delivery  ,max(customer_days) as max_customer_delivery,ROUND(avg(customer_days),2) as avg_customer_delivery
 FROM(
 SELECT 
order_id,
order_purchase_timestamp ,
datediff(order_approved_at,order_purchase_timestamp) AS approval_days,
datediff(order_delivered_carrier_date,order_purchase_timestamp) AS carrier_days,
datediff(order_delivered_customer_date,order_purchase_timestamp) AS customer_days,
datediff(order_estimated_delivery_date,order_purchase_timestamp) AS estimated_days

FROM fact_orders)t;

 -- FF
 SELECT 
order_id,
order_purchase_timestamp,
order_delivered_customer_date,
datediff(order_delivered_customer_date,order_purchase_timestamp) AS customer_days
FROM fact_orders
 GROUP BY order_id,order_delivered_customer_date,order_purchase_timestamp
 Having
 datediff(order_delivered_customer_date,order_purchase_timestamp)>30

 ;
 
 
 
/*
================================================================================
= Location	Analysis									                       =
=	This section focuses on analysing the dataset for location insight.        =
=================================================================================
*/

USE gold;
-- city segmentation
WITH make_seg AS (
SELECT f.customer_id, customer_city,
 SUM(price) OVER(PARTITION BY customer_id ORDER BY price DESC) AS total_price,
 NTILE(3) OVER (ORDER BY price DESC) nt
 
 FROM gold.fact_orders f 
 Left Join dim_customers c
 ON f.customer_id = c.customer_id)
 SELECT distinct customer_city,
 CASE 
 WHEN nt= 1 THEN "Top Rank"
 WHEN nt= 2  THEN "Mid Rank"
 ELSE
 "Regular"
  END  AS city_ranks
  FROM make_seg
  
 ;

 -- What is the best ten selling state?
 SELECT distinct customer_state, ROUND(sum(profit),2) AS total_profit
 FROM fact_orders o 
 LEFT JOIN  dim_customers c 
 ON o.customer_id=c.customer_id
 group by customer_state
 ORDER BY sum(profit) DESC
 LIMIT 10
 ;
 
 