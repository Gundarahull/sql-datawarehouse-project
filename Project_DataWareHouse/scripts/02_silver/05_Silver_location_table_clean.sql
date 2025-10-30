-- DATA CLEANING fro ERP Location


select * from bronze.crm_cust_info
select * from bronze.crm_prd_info
select * from bronze.crm_sales_details
select * from bronze.erp_loc_a101

select * from bronze.erp_cust_az12
select * from bronze.erp_px_cat_g1v2

-- AW-00011000
-- AW00011000
select 'AW-00011000', SUBSTRING('AW-00011000',4,LEN('AW-00011000')) 

select 'AW-00011000',		
		CONCAT(LEFT('AW-00011000',2),SUBSTRING('AW-00011000',4,LEN('AW-00011000')))

-- Removed
select
		cid,		
		CONCAT(LEFT(cid,2),SUBSTRING(cid,4,LEN(cid))) as cid_2,
		REPLACE(cid,'-','') as repces
from bronze.erp_loc_a101

select distinct(cntry) from (
	select
	CASE
		WHEN UPPER(cntry) = 'US' OR UPPER(cntry) = 'USA' THEN 'United States'
		WHEN UPPER(cntry) = 'DE' THEN 'Germany'
		WHEN cntry is NULL or cntry LIKE ' %' THEN 'n/a'
		ELSE cntry
	END as cntry
	from bronze.erp_loc_a101
) as sumi

select * from silver.erp_loc_a101 

INSERT INTO silver.erp_loc_a101(
	cid,
	cntry
)select 
	REPLACE(cid,'-','') as cid,
	CASE
		WHEN UPPER(cntry) = 'US' OR UPPER(cntry) = 'USA' THEN 'United States'
		WHEN UPPER(cntry) = 'DE' THEN 'Germany'
		WHEN cntry is NULL or cntry LIKE ' %' THEN 'n/a'
		ELSE cntry
	END as cntry
from bronze.erp_loc_a101




