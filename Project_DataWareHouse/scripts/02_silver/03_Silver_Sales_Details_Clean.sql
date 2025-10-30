-- DATA CLEANING FOR SALES_DETAILS

select * from bronze.crm_cust_info
select * from bronze.crm_prd_info
select * from bronze.crm_sales_details
select * from bronze.erp_loc_a101
select * from bronze.erp_cust_az12
select * from bronze.erp_px_cat_g1v2


-- Sales_Details
select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_at,
	sls_sales,
	sls_quantity,
	sls_price from bronze.crm_sales_details

-- Check Any UNWANTED Spaces
-- No unwanted Spaces

select
	sls_ord_num 
from bronze.crm_sales_details
where 
	LEN(sls_ord_num)!=LEN(TRIM(sls_ord_num))

-- CHECK any uneanted  prd_key or customer_id
-- No Escaped or Extra keys and id

select
	sls_cust_id
from bronze.crm_sales_details
where sls_cust_id NOT IN (select cst_id from silver.crm_cust_info )

-- DATE CHECK

select
	sls_order_dt
from bronze.crm_sales_details
where 
	sls_order_dt = 0 or LEN(sls_order_dt)!=8 
	-- sls_order_dt IS NULL or
-- diretcly we cant cast integer to Date , first varChar then DAte


select * from (select
	CASE
		WHEN sls_order_dt=0 OR LEN(sls_order_dt)!=8 THEN NULL
		ELSE CAST(CAST(sls_order_dt as varchar)as date)
	END as sls_order_dt,
	CASE
		WHEN sls_ship_dt=0 OR LEN(sls_ship_dt)!=8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt as varchar)as date)
	END as sls_ship_dt,
	CASE
		WHEN sls_due_at=0 OR LEN(sls_due_at)!=8 THEN NULL
		ELSE CAST(CAST(sls_due_at as varchar)as date)
	END as sls_due_at
from bronze.crm_sales_details
) as sumi

-- ORDER Chek between Order date, shipping date and delivery date

-- NO disOrder


select * from (select
	CASE
		WHEN sls_order_dt=0 OR LEN(sls_order_dt)!=8 THEN NULL
		ELSE CAST(CAST(sls_order_dt as varchar)as date)
	END as sls_order_dt,
	CASE
		WHEN sls_ship_dt=0 OR LEN(sls_ship_dt)!=8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt as varchar)as date)
	END as sls_ship_dt,
	CASE
		WHEN sls_due_at=0 OR LEN(sls_due_at)!=8 THEN NULL
		ELSE CAST(CAST(sls_due_at as varchar)as date)
	END as sls_due_at
from bronze.crm_sales_details
) as sumi
where sls_order_dt>=sls_ship_dt and sls_ship_dt>=sls_due_at

-- Order Date <= Shipping Date <=Delivery date

-- SALES AND SLS_PRICES CHECK

select 
	sls_sales,
	sls_quantity,
	sls_price 
from bronze.crm_sales_details
where  sls_price != sls_quantity * sls_sales
-- (sls_sales <= 0 OR sls_sales IS NULL) OR  (sls_price <= 0 OR sls_price IS NULL)


-- SALES
select 
	CASE 
		WHEN sls_sales IS NULL OR sls_sales <= 0 THEN (sls_price/sls_quantity)
		ELSE sls_sales
	END as sls_sales_modi,
	sls_sales,
	sls_quantity,
	sls_price 
from bronze.crm_sales_details
where (sls_sales <= 0 OR sls_sales IS NULL)


-- PRICE
select 
	CASE
	 WHEN sls_price IS NULL THEN (sls_sales*sls_quantity)
	 WHEN sls_price <0 THEN ABS(sls_price)
	 ELSE sls_price
	END as sls_price_modi,
	sls_sales,
	sls_quantity,
	sls_price 
from bronze.crm_sales_details
where (sls_price <= 0 OR sls_price IS NULL)

-- QUNATITY 
SELECT * FROM (
	SELECT 
		-- Fix sls_sales
		CASE 
			WHEN sls_sales IS NULL OR sls_sales <= 0 THEN sls_price / NULLIF(sls_quantity, 0)
			ELSE sls_sales
		END AS sls_sales,

		sls_quantity,

		-- Fix sls_price
		CASE
			WHEN sls_price IS NULL THEN sls_sales * sls_quantity
			WHEN sls_price < 0 THEN ABS(sls_price)
			WHEN sls_price=0 THEN  sls_sales * sls_quantity
			WHEN sls_price != (sls_sales * sls_quantity) THEN sls_sales * sls_quantity
			ELSE sls_price
		END AS sls_price_modi

	FROM bronze.crm_sales_details
	) AS sumi
WHERE sls_price_modi IS NULL








select * from silver.crm_sales_details

INSERT INTO silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_at,
	sls_sales,
	sls_quantity,
	sls_price
)
SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE 
		WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	CASE 
		WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	CASE 
		WHEN sls_due_at = 0 OR LEN(sls_due_at) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_at AS VARCHAR) AS DATE)
	END AS sls_due_at,
	CASE 
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
	sls_quantity,
	CASE 
		WHEN sls_price IS NULL OR sls_price <= 0 
		THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price  -- Derive price if original value is invalid
	END AS sls_price
FROM bronze.crm_sales_details;

