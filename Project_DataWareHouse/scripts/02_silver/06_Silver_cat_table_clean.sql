-- DATA Cleaning for Category

select * from bronze.crm_cust_info
select * from silver.crm_prd_info
select * from bronze.crm_sales_details
select * from bronze.erp_px_cat_g1v2

-- ID check
select 
	DISTINCT(maintenance)
from bronze.erp_px_cat_g1v2

INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
select id,cat,subcat,maintenance from bronze.erp_px_cat_g1v2
