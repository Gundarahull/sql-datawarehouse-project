-- SomeWhat Advanced Data Analysis....


select * from INFORMATION_SCHEMA.TABLES

--  ♦️🎯 CHANGE OVER TIME >> Analytics...

-- analyze sales over time

select 
	SUM(sales_amount) as products_count,
	DATETRUNC(YEAR,due_date)
from gold.fact_sales
group by DATETRUNC(YEAR,due_date)
order by DATETRUNC(YEAR,due_date)

-- Higher Aggregation like Month and year



--  ♦️🎯 CUMMALTIVE >> Analytics...
--  Calucalte the total sales per month and the running total over time

select
	*,
	SUM(sales) OVER(PARTITION BY (sales_year) ORDER BY (sales_year)) as running_toatl
from (
	SELECT 
		DATETRUNC(MONTH,order_date) AS sales_year,
		SUM(sales_amount) as sales
		FROM gold.fact_sales
	WHERE DATETRUNC(MONTH,order_date) IS NOT NULL
	GROUP BY DATETRUNC(MONTH,order_date)
) as sumi

-- --  ♦️🎯 COMPARE OR PERFORMNACE >> Analytics...




-- 43419,7118507





select 
	avg(sales_amount)
from gold.fact_sales
where YEAR(order_date) = YEAR(DATEADD(YEAR, -1 ,GETDATE()))
group by product_key 

select YEAR(DATEADD(YEAR, -1 ,GETDATE()))




select 
	product_key,
	AVG(sales_amount) as avg_amount,
	SUM(sales_amount) as total
from gold.fact_sales
where order_date is nOT NULL
GROUP BY product_key
ORDER BY product_key






-- PROPOTIONAL ANMALYSIS

-- MEASURE/TOTAL *100 

-- Its almost PIE Chart

-- which categories contribute the most to overall sales

select * from gold.dim_products

select * from gold.fact_sales

select category from (
	select 
		*,
		ROW_NUMBER() OVER(order by amount_categ DESC) as Rani
	from (
		select

			sum(sales_amount) as amount_categ,
			category
		from gold.fact_sales fs
		left join gold.dim_products p
		on fs.product_key=p.product_key
		group by p.category
	) as sumi
) as sumiy
where Rani=1


-- DATA SEGMENATATION

-- 0 to 723 is low
-- 724 to 1447 >> Medi
-- 1448 to 2171 >> High

select 
	price_segment, COUNT(product_name) as coumti
from (
	select
		product_key,
		product_name,
		CASE 
			WHEN cost  BETWEEN 0 and 723 THEN 'Low'
			WHEN cost  BETWEEN 724 and 1447 THEN 'Moderate'
			WHEN cost  BETWEEN 1448 and 2171 THEN 'High'
		END as price_segment
	from gold.dim_products
	) as sumi
group by price_segment


SELECT 
    customer_segments,
    COUNT(*) AS total_customers
FROM (
    SELECT
        s.customer_key,
        SUM(sales_amount) AS sales_per,
        CASE 
            WHEN COUNT(DISTINCT order_date) > 12 AND SUM(sales_amount) > 5000 THEN 'VIP'
            WHEN COUNT(DISTINCT order_date) > 12 AND SUM(sales_amount) <= 5000 THEN 'Regular'
            ELSE 'New'
        END AS customer_segments
    FROM gold.dim_customers c
    LEFT JOIN gold.fact_sales s
        ON c.customer_key = s.customer_key
    GROUP BY s.customer_key
) AS sumi
GROUP BY customer_segments;








-- BUILD CUSTOMER REPORT

-- STEPS
-- 1. Build details
-- 2. Do aggreagtions

WITH CTE_cust_detail AS
(
	select
		CONCAT(c.first_name,' ',c.last_name) as full_name,
		DATEDIFF(YEAR,c.birthdate,GETDATE()) as age,
		c.customer_number,
		c.customer_key,
		s.order_number,
		s.sales_amount,
		s.quantity,
		s.order_date,
		s.price,
		s.product_key
	from  gold.fact_sales s
	left join gold.dim_customers c
	on c.customer_key=s.customer_key
)
select 
	customer_key,
	SUM(sales_amount) as total_Sales ,
	SUM(product_key) as total_products,
	SUM(quantity) as total_orders,
from CTE_cust_detail
group by customer_key








