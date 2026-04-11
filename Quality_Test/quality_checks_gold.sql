/*
===================================================================================================
* This scripts checks  for data quality in the gold layer.alter
* It ensures data consistency,  standazation and completeness  across the records of the silver layer.
*
* 
* NOTES:
* Make sure to run this script to validate data consistency. completeness and accuracy after loading  data to
* this layer.
==================================================================================================
*/




-- Gold Layer Test

-- Test that there is no nagative profit
-- Expected out: is Nothing

use gold;
SELECT order_id, profit, count(profit) FROM gold.fact_orders
where profit <0
group by order_id, profit;



