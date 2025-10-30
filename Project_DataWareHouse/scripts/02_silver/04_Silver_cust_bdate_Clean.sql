-- DATA CLEANING fro ERP Customer 


select * from bronze.crm_cust_info
select * from bronze.crm_prd_info
select * from bronze.crm_sales_details
select * from bronze.erp_cust_az12
select * from bronze.erp_loc_a101

select * from bronze.erp_px_cat_g1v2



-- Remove the Extra Characters
-- AW00011000
-- NASAW00011003

select 
	SUBSTRING(cid,4,LEN(cid)) as cid_modi,*
from bronze.erp_cust_az12
-- Check Bdate

select 
	CASE WHEN bdate>GETDATE() THEN NULL
	ELSE bdate
	END as bdate
from bronze.erp_cust_az12

select * from (
	select 
		CASE
			WHEN gen is NULL or gen LIKE ' %' THEN 'n/a'
			WHEN UPPER(gen)='F' THEN 'Female'
			WHEN UPPER(gen)='M' THEN 'Male'
			ELSE gen
		 END as gen
	from bronze.erp_cust_az12
) as sumi

select * from silver.erp_cust_az12 

INSERT INTO silver.erp_cust_az12(
	cid,
	bdate,
	gen
)
SELECT 
	SUBSTRING(cid,4,LEN(cid)) as cid,
	CASE
		WHEN bdate>GETDATE() THEN NULL
		ELSE bdate
	END as bdate,
	CASE
		WHEN gen is NULL or gen LIKE ' %' THEN 'n/a'
		WHEN UPPER(gen)='F' THEN 'Female'
		WHEN UPPER(gen)='M' THEN 'Male'
		ELSE gen
	END as gen
from bronze.erp_cust_az12

