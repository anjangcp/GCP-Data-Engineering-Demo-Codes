/*
Author : @ Anjan GCP Data Engineering
Created SQLs to demo Biqguery Datalineage
*/

-- Create conslolidated sales table bi joining customer and items tables
create or replace table data_eng_demos.cust_item_sales_dtls as
SELECT
    customer.fname||''||customer.lname as customer_name,
    items.itm_name,
    sales.qty,
    sales.price,
    sales.ord_date
  FROM
    `gcp-dataeng-demo-431907.data_eng_demos.customer` AS customer
    INNER JOIN `gcp-dataeng-demo-431907.data_eng_demos.sales` AS sales ON customer.cust_id = sales.cust_id
    INNER JOIN `gcp-dataeng-demo-431907.data_eng_demos.items` AS items ON sales.item_id = items.item_id;

-- Create Aggregate table based on customer name
create or replace table data_eng_demos.customer_agg_sales as
SELECT
  customer_name,
  SUM(qty) AS tot_qty,
  SUM(price) AS tot_price
FROM
  data_eng_demos.cust_item_sales_dtls
GROUP BY
  1;

-- Create Aggregate table based on item name
create or replace table data_eng_demos.item_agg_sales as
SELECT
  itm_name,
  SUM(qty) AS tot_qty,
  SUM(price) AS tot_price
FROM
  data_eng_demos.cust_item_sales_dtls
GROUP BY
  1;

