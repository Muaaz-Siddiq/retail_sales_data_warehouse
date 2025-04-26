SELECT * FROM retail_sales_dwh_bronze.retail_sales_bronze

SELECT MAX("Sales") AS "m_sales", MIN("Sales") AS "min_sales", "Category", "Sub-Category", "Product Name", "City"
FROM retail_sales_dwh_bronze.retail_sales_bronze
GROUP BY ("Category", "Sub-Category", "Product Name", "City") ORDER BY ("Product Name", "City")

SELECT 
"Category", 
"Sub-Category", 
"Product Name", 
"City",
MAX("Sales") OVER (PARTITION BY "Category", "Sub-Category") AS "max_sales",
MIN("Sales") OVER (PARTITION BY "Category", "Sub-Category") AS "min_sales",
("Sales" - MIN("Sales") OVER (
PARTITION BY "Category", "Sub-Category"
)) AS "delievery_charges"
FROM retail_sales_dwh_bronze.retail_sales_bronze


SELECT * FROM(
SELECT 
"Category", 
"Sub-Category", 
"Product Name", 
"City",
"Sales",
MAX("Sales") OVER (PARTITION BY "Category", "Sub-Category") AS "max_sales",
MIN("Sales") OVER (PARTITION BY "Category", "Sub-Category") AS "min_sales",
("Sales" - MIN("Sales") OVER (
PARTITION BY "Category", "Sub-Category"
)) AS "delievery_charges"
FROM retail_sales_dwh_bronze.retail_sales_bronze) AS q
WHERE q.delievery_charges = 0



SELECT 
"Category", 
"Sub-Category", 
"Product Name", 
"City",
MAX("Sales") OVER (PARTITION BY "Category", "Sub-Category") AS "max_sales",
MIN("Sales") OVER (PARTITION BY "Category", "Sub-Category") AS "min_sales",
ROUND(
("Sales" / (MIN("Sales") OVER (
PARTITION BY "Category", "Sub-Category")))::numeric, 0
) AS "quantity"
FROM retail_sales_dwh_bronze.retail_sales_bronze