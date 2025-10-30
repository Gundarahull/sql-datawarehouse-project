-- BRONZE LAYER
-- DATA INGESTION (Bridge)

-- CREATING TABLES for the Source


CREATE TABLE bronze.crm_cust_info(
	cst_id INTEGER,
	cst_key VARCHAR(100),
	cts_firstname VARCHAR(100),
	cst_lastname VARCHAR(100),
	cst_marital_status VARCHAR(10),
	cst_gndr VARCHAR(10),
	cst_create_date DATE
)

CREATE TABLE bronze.crm_prd_info(
	prd_id INTEGER,
	prd_key VARCHAR(100),
	prd_nm VARCHAR(100),
	prd_cost INTEGER,
	prd_line VARCHAR(100),
	prd_start_dt DATE,
	prd_end_dt DATE
)

CREATE TABLE bronze.crm_sales_details(
	sls_ord_num VARCHAR(100),
	sls_prd_key VARCHAR(100),
	sls_cust_id INTEGER,
	sls_order_dt VARCHAR(100),
	sls_ship_dt VARCHAR(100),
	sls_due_at VARCHAR(100),
	sls_sales INTEGER,
	sls_quantity INTEGER,
	sls_price INTEGER
)


CREATE TABLE bronze.erp_cust_az12(
	cid VARCHAR(100),
	bdate VARCHAR(100),
	gen INTEGER
)

CREATE TABLE bronze.erp_loc_a101(
	cid VARCHAR(100),
	cntry VARCHAR(100)
)

CREATE TABLE bronze.erp_px_cat_g1v2(
	id VARCHAR(100),
	cat VARCHAR(100),
	subcat VARCHAR(100),
	maintenance VARCHAR(100)
)

-- // FIRST CHECK EXITS THEN >>>
IF OBJECT_ID('bronze.erp_px_cat_g1v2') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2
CREATE TABLE bronze.erp_px_cat_g1v2(
	id VARCHAR(100),
	cat VARCHAR(100),
	subcat VARCHAR(100),
	maintenance VARCHAR(100)
)

