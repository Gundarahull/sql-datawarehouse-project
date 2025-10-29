-- DATA CLEANING FOR CRM_PROD_INFO


select * from bronze.crm_cust_info
select * from bronze.crm_prd_info
select * from bronze.crm_sales_details
select * from bronze.erp_loc_a101
select * from bronze.erp_cust_az12
select * from bronze.erp_px_cat_g1v2

-- Check no duplicate id 
-- Check duplicate prd_key


select 
		SUBSTRING(prd_key,7,LEN(prd_key)),
		count(SUBSTRING(prd_key,7,LEN(prd_key))) as counter
from bronze.crm_prd_info
GROUP BY SUBSTRING(prd_key,7,LEN(prd_key)),prd_key
HAVING count(SUBSTRING(prd_key,7,LEN(prd_key)))>1

select * from bronze.crm_prd_info where prd_cost<0

-- YES THERE ARE SOME DUPLICATE VALUES
-- BI-MB-BK-M68B-38


-- NO NEED TO TRIM

-- Check NUll or Negative numbers in prd_cost
 select
	COALESCE(prd_cost,0) as prd_cost 
from bronze.crm_prd_info where prd_cost<0


select 
	LEFT(prd_key,5),
	REPLACE(LEFT(prd_key,5),'-','_') as cat_id ,
	SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
	COALESCE(prd_cost,0) as prd_cost
from bronze.crm_prd_info where prd_cost<0

-- DATA NORMALIZATion and Consistency
select DISTINCT(prd_line) from bronze.crm_prd_info

select
	CASE
		WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
		WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
		WHEN UPPER(TRIM(prd_line))='S' THEN 'other Sales'
		WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
		ELSE 'n/a'
	END as prd_line
from bronze.crm_prd_info

SELECT 
    prd_id,
    prd_key,
    prd_start_dt,
    DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
FROM bronze.crm_prd_info

INSERT INTO silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)SELECT 
	prd_id,
	REPLACE(LEFT(prd_key,5),'-','_') as cat_id,
	SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
	prd_nm,
	COALESCE(prd_cost,0) as prd_cost,
	CASE
		WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
		WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
		WHEN UPPER(TRIM(prd_line))='S' THEN 'other Sales'
		WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
		ELSE 'n/a'
	END as prd_line,
	prd_start_dt,
    DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
from bronze.crm_prd_info




