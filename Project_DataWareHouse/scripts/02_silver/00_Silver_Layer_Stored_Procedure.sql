-- STORED PROCEDURE FOR SILVER LAYER

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	BEGIN TRY
		DECLARE @start_time DATETIME, 
				 @end_time DATETIME,
				 @batch_start_time DATETIME,
				 @batch_end_time DATETIME

		PRINT 'LOADING THE SILVER LAYER'

		PRINT '====================================='
		PRINT 'LOADING CRM LAYER'
		PRINT '====================================='

		PRINT '====================================='
		PRINT 'TRUNCATING and INSERTING into : silver.crm_cust_info'
		PRINT '====================================='
		
		SET @batch_start_time=GETDATE()
		TRUNCATE TABLE silver.crm_cust_info
		SET @start_time=GETDATE()
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

		SET @end_time=GETDATE();
		PRINT 'TIME TAKEN TO COMPLETE: ' + 
			   CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';	


		PRINT '====================================='
		PRINT 'TRUNCATING and INSERTING into : silver.crm_prd_info'
		PRINT '====================================='
		SET @start_time=GETDATE()
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
		SET @end_time=GETDATE();
		PRINT 'TIME TAKEN TO COMPLETE: ' + 
			   CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';	


		PRINT '====================================='
		PRINT 'TRUNCATING and INSERTING into : silver.sales_details'
		PRINT '====================================='

		SET @start_time=GETDATE()
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
		SET @end_time=GETDATE();
		PRINT 'TIME TAKEN TO COMPLETE: ' + 
			   CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';	


		PRINT '====================================='
		PRINT 'LOADING ERP LAYER'
		PRINT '====================================='

		PRINT '====================================='
		PRINT 'TRUNCATING and INSERTING into : bronze.erp_cust_az12'
		PRINT '====================================='

		SET @start_time=GETDATE()
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
		SET @end_time=GETDATE();
		PRINT 'TIME TAKEN TO COMPLETE: ' + 
			   CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';	


		PRINT '====================================='
		PRINT 'TRUNCATING and INSERTING into : silver.erp_loc_a101'
		PRINT '====================================='

		SET @start_time=GETDATE()
		SET @end_time=GETDATE();
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
		PRINT 'TIME TAKEN TO COMPLETE: ' + 
			   CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';	

		PRINT '====================================='
		PRINT 'TRUNCATING and INSERTING into : silver.erp_px_cat_g1v2'
		PRINT '====================================='
		
		SET @start_time=GETDATE()
		INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
		select id,cat,subcat,maintenance from bronze.erp_px_cat_g1v2
		SET @end_time=GETDATE();

		PRINT 'TIME TAKEN TO COMPLETE: ' + 
			   CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';	
		 SET @batch_end_time=GETDATE()
		 PRINT 'TIME TAKEN TO Complete Load Bronze Layer' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time)as NVARCHAR(20)) + ' Seconds' 

		
	END TRY
	BEGIN CATCH
		PRINT('Error Occurred: ' + ERROR_MESSAGE());
        PRINT('Error Line: ' + CAST(ERROR_LINE() AS NVARCHAR(10)));
        PRINT('Error Procedure: ' + ISNULL(ERROR_PROCEDURE(), 'N/A'));
	END CATCH
END

EXEC silver.load_silver