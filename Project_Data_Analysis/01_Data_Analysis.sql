-- DATA ANALYSIS
-- Step 2 for grabbing things from Cleaned Data
-- EDA ()

select * from gold.dim_customers
select * from gold.dim_products
select * from gold.fact_sales

-- whether the column is Dimesnion or Measure

-- >> Is it Numeric and Is it Sense have an Aggreagation then ✅ Measure Column
-- >> Otherwise ✅ Dimension

-- FOr Grouping we need Dimneiosns

select * from INFORMATION_SCHEMA.TABLES

select * from INFORMATION_SCHEMA.COLUMNS

-- ♦️🎯 DImensions Exploration 
-- Checking distinct or not

select * from gold.dim_customers

select DISTINCT(marital_status) from gold.dim_customers

-- ♦️🎯 Date Exploration 
-- Checking Boundaries

SELECT 
    MAX(CASE WHEN ranku = 1 THEN order_date END) AS last_order_date,
    MAX(CASE WHEN ranki = 1 THEN order_date END) AS first_order_date
FROM (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY order_date ASC) AS ranki,
        ROW_NUMBER() OVER (ORDER BY order_date DESC) AS ranku,
        order_date
    FROM gold.fact_sales
    WHERE product_key = (SELECT MIN(product_key) FROM gold.fact_sales)
) AS sumi
WHERE ranku = 1 OR ranki = 1;

select MIN(order_date) as first_order_date, 
	   MAX(order_date) as last_order_date 
from gold.fact_sales

-- HOW MANY YEARS ARE AVAILABLE

select 
	DATEDIFF(YEAR,first_order_date,last_order_date) as years_available 
from (
	select MIN(order_date) as first_order_date, 
		   MAX(order_date) as last_order_date 
	from gold.fact_sales
) as sumi


-- Find the Youngest and Oldest customer
select 
	*
from gold.dim_customers
where birthdate = (select MIN(birthdate) from gold.dim_customers) or
	  birthdate= (select MAX(birthdate) from gold.dim_customers

-- ♦️🎯 MEASURES EXPLORATION --

-- Find the total sales

select
	sum(sales_amount) as total_sales_amount,
	sum(quantity) as total_quantitty_sales,
	count(order_number) as total_orders
from gold.fact_sales



select COUNT(DISTINCT(product_name)) as total_producsts from gold.dim_products

select COUNT(DISTINCT(customer_number)) as total_number_customers from gold.dim_customers


select
	count(DISTINCT(c.customer_key)) as orders_placed 
from gold.dim_customers c
inner join  gold.fact_sales s
on s.customer_key=c.customer_key

-- GENERTING TOTAL REPORT

select 'Total Sales' as Measure , sum(sales_amount) as total_sales_amount from gold.fact_sales
UNION ALL
select 'Total Quantity Sales' , sum(quantity) from gold.fact_sales
UNION ALL
select 'Total Orders', count(order_number) as total_orders from gold.fact_sales

-- ♦️🎯 MAgintude Analysis

-- Find Total Customers by Countries
select 
	count(DISTINCT(customer_key)) as customers_count,
	country
from gold.dim_customers
group by country

-- gender
select 
	count(DISTINCT(customer_key)) as customers_count,
	gender
from gold.dim_customers
group by gender

-- Category

select 
	COUNT(DISTINCT(product_name)) as total_products,
	category
from gold.dim_products
group by category

-- Avergae PRice by category

select 
	(AVG(cost)) as avg_cost,
	category
from gold.dim_products
group by category

-- Total revenue by Category

select 
	(SUM(cost)) as total_cost,
	category
from gold.dim_products
group by category

-- Toatl Revenue by Each customer


select 
	(s.price * s.quantity) as per_customer,
	s.price,
	s.quantity,
	c.customer_key ,
	ROW_NUMBER() OVER(PARTITION BY c.customer_key ORDER BY c.customer_key) as ranki
from gold.dim_customers c
inner join gold.fact_sales s
on s.customer_key=c.customer_key

-- Across the Countries

select 
	DENSE_RANK() OVER(PARTITION BY s.product_key ORDER BY s.sales_amount) as densi,
	s.sales_amount,
	p.product_name
from gold.dim_products p
inner join gold.fact_sales s
on p.product_key=s.product_key



