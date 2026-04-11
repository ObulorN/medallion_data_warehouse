>Data Catalog Overview:

The data catalog is an inventory of metadata that helps organization, find, organize and govern their data assets.In data warehouse, it clearly shows the data type of each field/entity in the tables of the data warehouse.

>Gold Layer Data Catalog:

- gold.dim_customers:

```
___________________________________________
               
   Entity_Name    | Data Type  | Description   
____________________________________________
                   
1   customer_id   |  varchar(255)      | key for dim_customer

2   customer_unique_id   |  varchar(255)      | unique key for dim_customer

3   customer_name_surrogate   |  varchar(255)      | generateed names for dim_customer

4   customer_zip_code_prefix   |  varchar(255)      | zip code prefix

5   customer_city   |  varchar(255)      | customers city

6   customer_state   |  varchar(255)      | customer state 

```

- gold.dim_payment:

```
___________________________________________
               
   Entity_Name    | Data Type  | Description   
____________________________________________
                   
1   order_id   |  varchar(255)      | payment order id

2   payment_value   |  float      | unique key for dim_customer

3   number_of_payments   |  int    | generateed names for dim_customer

4   max_install   |  int      | zip code prefix

```

- gold.dim_products:

```
________________________________________________
               
   Entity_Name    | Data Type  | Description   
________________________________________________
                   
1   product_id   |  varchar(255)      | product id

2   product_category_name   |  varchar(255)      | category name

3   product_name_lenght   |  int      | length of product name

4   product_description_lenght   |  int      | zip code prefix

5   product_photos_qty   |  int      | product photo quantity

6   product_weight_g_state   |  int      | weight of product

7   product_length_cm   |  int      | length of product

8   product_height_cm   |  int      | height of product

9   product_width_cm   |  int      | width of product


```




- gold.fact_orders:

```
___________________________________________
               
   Entity_Name    | Data Type  | Description   
____________________________________________
                   
1   order_id   |  varchar(255)      | key for fact_order

2   customer_id   |  varchar(255)      | customer id

3   order_item_id   |  varchar(255)      | order item id

4   product_id  |  varchar(255)      | product id

5   seller_id   |  varchar(255)      | seller id

6   price   |  float      | product price

7   freight_value   |  float      | freight value 

8   order_status   |  varchar(100)      | order status entries 

9   order_purchase_timestamp   |  timestamp      | datetime of purchas

10   order_approved_at   |  timestamp      | datetime of approval

11   order_delivered_carrier_date   |  timestamp      | carrier collected datetime 

12   order_delivered_customer_date   |  timestamp      | datetime custommer order collection 

13   order_estimated_delivery_date   |  timestamp      | estimated datetime delivery 

14   shipping_limit_date   |  timestamp      |  

```
 
