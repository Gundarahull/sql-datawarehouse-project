-- SLIVER LAYER

-- Understanding the table, closly looking into the table and analyzing the joins || relations betweeen the layer

-- STEP 1 DDL 
CREATE TABLE silver.crm_cust_info(
	cst_id INTEGER,
	cst_key VARCHAR(100),
	cts_firstname VARCHAR(100),
	cst_lastname VARCHAR(100),
	cst_marital_status VARCHAR(10),
	cst_gndr VARCHAR(10),
	cst_create_date DATE,
	dwh_create_date DATETIME DEFAULT GETDATE()
)

CREATE TABLE silver.crm_prd_info(
	prd_id INTEGER,
	prd_key VARCHAR(100),
	prd_nm VARCHAR(100),
	prd_cost INTEGER,
	prd_line VARCHAR(100),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date DATETIME DEFAULT GETDATE()
)

CREATE TABLE silver.crm_sales_details(
	sls_ord_num VARCHAR(100),
	sls_prd_key VARCHAR(100),
	sls_cust_id INTEGER,
	sls_order_dt VARCHAR(100),
	sls_ship_dt VARCHAR(100),
	sls_due_at VARCHAR(100),
	sls_sales INTEGER,
	sls_quantity INTEGER,
	sls_price INTEGER,
	dwh_create_date DATETIME DEFAULT GETDATE()
)


CREATE TABLE silver.erp_cust_az12(
	cid VARCHAR(100),
	bdate DATE,
	gen VARCHAR(100),
	dwh_create_date DATETIME DEFAULT GETDATE()
)

CREATE TABLE silver.erp_loc_a101(
	cid VARCHAR(100),
	cntry VARCHAR(100),
	dwh_create_date DATETIME DEFAULT GETDATE()
)

CREATE TABLE silver.erp_px_cat_g1v2(
	id VARCHAR(100),
	cat VARCHAR(100),
	subcat VARCHAR(100),
	maintenance VARCHAR(100),
	dwh_create_date DATETIME DEFAULT GETDATE()
)


-- STEP 2 : Check
-- 1. Find any Duplicates
select COUNT(DISTINCT(cst_id)) as unique_primary_key from bronze.crm_cust_info

select 
	cst_id as duplicate ,
	(select COUNT(cst_id) from silver.crm_cust_info) as all_primary_key,
	(select COUNT(DISTINCT(cst_id))  from silver.crm_cust_info) as unique_primary_key
from silver.crm_cust_info
GROUP BY cst_id
having COUNT(cst_id)>1 or cst_id  IS NULL


-- If any duplicate , have to choose the latest one
-- 29449,29473,29433,NULL,29483,29466

select  cst_id,
		cst_key,
		TRIM(cts_firstname),
		TRIM(cst_lastname),
		CASE
			WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
			WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
			ELSE 'n/a'
		END as cst_gndr,
		CASE
			WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
			WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
			ELSE 'n/a'
		END as cst_marital_status,
		cst_create_date from (
	select 
		RANK() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as ranking,
		*
	from bronze.crm_cust_info
	) as sumi
where ranking=1 and cst_id=29466

-- Check for Unwanted Spaces

select
	*, 
	LEN(cts_firstname), 
	LEN(TRIM(cts_firstname)) as trimmed 
from bronze.crm_cust_info
where cts_firstname LIKE ' %' -- or cst_lastname LIKE ' %'

-- DATA Constitnecy and Standratization

select cst_id,
		cst_key,
		TRIM(cts_firstname),
		TRIM(cst_lastname),
		CASE
			WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
			WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
			ELSE 'n/a'
		END as cst_gndr,
		CASE
			WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
			WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
			ELSE 'n/a'
		END as cst_marital_status,
		cst_create_date
from bronze.crm_cust_info

select DISTINCT(cst_marital_status) from bronze.crm_cust_info

-- ==================================================================================
-- INSERTING INTO THE SILVER (Clean DATA)

INSERT INTO silver.crm_cust_info(
	cst_id,
	cst_key,
	cts_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
	)
select  cst_id,
		cst_key,
		TRIM(cts_firstname),
		TRIM(cst_lastname),
		--===============================
		CASE
			WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
			WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
			ELSE 'n/a'
		END as cst_marital_status,
		--=============================
		CASE
			WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
			WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
			ELSE 'n/a'
		END as cst_gndr,
		
		cst_create_date from (
	select 
		RANK() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as ranking,
		*
	from bronze.crm_cust_info
	) as sumi
where ranking=1 and cst_id is NOT NULL;

-- ===============================================================================================================

select count(cst_id) from silver.crm_cust_info

select count(*),cst_id from silver.crm_cust_info group by cst_id having count(*)>1 or cst_id is NULL

select * from silver.crm_cust_info where cst_id IS NOT NULL










