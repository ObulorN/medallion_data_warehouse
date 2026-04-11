with last_five_customers as (
SELECT c.customer_id, customer_name_surrogate,
sum(profit) over(partition by customer_id order by profit asc) as profit,
 dense_rank() over(order by profit) as rn
 FROM gold.fact_orders f 
 left join dim_customers c
 on 
 f.customer_id=c.customer_id)
 
 select customer_name_surrogate,round(profit,2) as worst_customers from last_five_customers
 order by rn asc
 LIMIT 5
;
-- segmentation
with make_seg as (
SELECT customer_id,
 sum(price) over(partition by customer_id order by price desc) as total_price,
 ntile(3) over (order by price desc) nt,
 max(price) over() as max_price,
 min(price) over() as min_price
 
 FROM gold.fact_orders)
 select nt,total_price,
 case 
 when nt= 1 then "Top Rank"
 when nt= 2  then "Mid Rank"
 else
 "Regular"
  end
  from make_seg
 ;
 
 -- Best ten category
 
 use gold;
SELECT o.order_id,
 o.product_id, 
 price, payment_value,
 product_category_name,
 round( sum(payment_value-price),2) as profit 
 FROM gold.fact_orders o
left join
 gold.fact_payment pm
 on o.order_id=pm.order_id
 left join dim_products pd
 on
 o.product_id=pd.product_id
  group by o.order_id, o.product_id, payment_value,price,product_category_name
 order by profit desc , product_category_name asc
 limit 10
  ;
 
  -- Least five category (Note: there seems to be some profit outlier in the output
 
 use gold;
 with least_category as (
	SELECT o.order_id, o.product_id, sum(price) as price,product_category_name, sum(profit)as profit  FROM gold.fact_orders o
	  left join dim_products pd
	 on
	 o.product_id=pd.product_id
	  group by o.order_id, o.product_id,price,product_category_name,profit
	  order by profit asc , product_category_name asc)
      
select order_id,product_id,product_category_name,price,profit
from least_category
limit 10
  ;
  
  -- profit mom
  select Month(order_purchase_timestamp)as OrderMonth, sum(profit) as current_profit,
  Lag(sum(profit)) over(order by Month(order_purchase_timestamp)) as lag_profit,
  sum(profit)-Lag(sum(profit)) over(order by Month(order_purchase_timestamp))  as profit_differnce
  from
  gold.fact_orders
  group by 
  Month(order_purchase_timestamp)
    ;
    
    -- find the next transaction amount per customer
     use gold;
    select 
      customer_id, month(order_purchase_timestamp) as om,sum(profit) as total_p,
       lead(sum(profit)) over(partition by customer_id order by  month(order_purchase_timestamp)) as next_buy
     
    from fact_orders
    group by 
    month(order_purchase_timestamp),
     customer_id
     ;
     
     
   -- ==================  
   select customer_id,
   current_profit,
   yr, mt,
   lead(sum(current_profit)) over (partition by customer_id order by yr, mt) as next_profit
	  FROM ( select customer_id, 
	   year(order_purchase_timestamp) as yr,
	   month(order_purchase_timestamp) as mt,
	   sum(profit) as current_profit from fact_orders
	   group by customer_id, year(order_purchase_timestamp), month(order_purchase_timestamp)
	   order by    year(order_purchase_timestamp) desc, month(order_purchase_timestamp), customer_id)t
	   
   group by yr, mt,current_profit,customer_id
   ;
   
   -- Quick Check: Run this simple count to see if your customers even have repeat business:
SELECT order_id, COUNT(DISTINCT month(order_purchase_timestamp)) 
FROM fact_orders 
GROUP BY order_id 
HAVING COUNT(DISTINCT month(order_purchase_timestamp))  > 1;

-- calculate day difference between consecutive orders

select current_day,next_day, datediff(next_day,current_day)
from(
select order_purchase_timestamp as current_day,
lead(order_purchase_timestamp) over(order by order_purchase_timestamp) as next_day
from fact_orders)t;


 -- Identify customers with increasing purchases