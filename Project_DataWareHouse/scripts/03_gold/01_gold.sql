-- GOLD LAYER

-- HOW CAN WE BUILD THIS? LETS SEE
-- DATA MODELING 
-- Conceptual, Logocal and Physical model

-- STAR and SNOWFLAKE Schema

-- Dimension TABLE >> Describes WHO WHERE WHAT , which shows about or description of the table
-- FACT TABLE >> how many, quantitty , numbers, dates , which shows mathematics 

-- CREATING VIEWS for gold LAYER

--- ALWAYS LEFY JOIN not to loose the Source Details


-- Check Duplicates

-- CUSTOMER DIMNESION

select * from silver.crm_cust_info

select * from silver.erp_cust_az12

select * from silver.erp_loc_a101

CREATE VIEW gold.dim_customers AS (
	select 
		ROW_NUMBER() OVER(ORDER BY ci.cst_id) as customer_key,
		ci.cst_id as customer_id,
		ci.cst_key as customer_number, 
		ci.cts_firstname as first_name,
		ci.cst_lastname as last_name, 
		cl.cntry as country,
		ci.cst_marital_status as marital_status,
		CASE
			WHEN ci.cst_gndr!='n/a' then ci.cst_gndr
			ELSE COALESCE(cd.gen,'n/a')
		END as gender,
		cd.bdate as birthdate,
		ci.cst_create_date as create_date
	from silver.crm_cust_info ci --18484
	LEFT JOIN silver.erp_cust_az12 cd
	ON ci.cst_key=cd.cid
	LEFT JOIN silver.erp_loc_a101 cl
	on ci.cst_key=cl.cid
)


-- SUrrogaet KEy (LIKE A PK)


-- PRODUCT DIMENSION


select 
	ROW_NUMBER() OVER(ORDER BY pd.prd_start_dt, pd.prd_key ) as product_key,
	pd.prd_id as product_id,
	pd.prd_key as product_number,
	pd.prd_nm as product_name,

	px.cat as category,
	pd.cat_id as category_id,
	px.subcat as subcategory,
	px.maintenance as maintenance,
	
	pd.prd_cost as cost,
	pd.prd_line as product_line,
	pd.prd_start_dt as start_date	
from silver.crm_prd_info pd
LEFT JOIN silver.erp_px_cat_g1v2 px
ON pd.cat_id = px.id
where pd.prd_end_dt IS NULL



select * from silver.crm_prd_info

select * from silver.erp_px_cat_g1v2

select * from silver.erp_loc_a101




