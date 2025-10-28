-- Step 2 >> Create Stored Procedure
-- SP starts with the load prefix
-- Track how much its taking to insert

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	BEGIN TRY
	DECLARE @start_time DATETIME ,@end_time DATETIME, @start_bronze_time DATETIME, @end_bronze_time DATETIME
	PRINT 'LOADING THE BRONZE LAYER'

		PRINT '====================================='
		PRINT 'TRUNCATING and INSERTING into : bronze.crm_cust_info'
		PRINT '====================================='
		
		SET @start_bronze_time=GETDATE()
		TRUNCATE TABLE bronze.crm_cust_info
		SET @start_time=GETDATE()
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\fplet\OneDrive\Documents\Something\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR=',',
			TABLOCK
		)
		SET @end_time=GETDATE();

PRINT 'TIME TAKEN TO COMPLETE: ' + 
      CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';	



		TRUNCATE TABLE bronze.crm_prd_info
		SET @start_time=GETDATE()
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\fplet\OneDrive\Documents\Something\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
				SET @end_time=GETDATE();
PRINT 'TIME TAKEN TO COMPLETE: ' + 
      CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';




		TRUNCATE TABLE bronze.crm_sales_details
		SET @start_time=GETDATE()
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\fplet\OneDrive\Documents\Something\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR=',',
			TABLOCK
		)
				SET @end_time=GETDATE();
PRINT 'TIME TAKEN TO COMPLETE: ' + 
      CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';



		TRUNCATE TABLE bronze.erp_cust_az12
		SET @start_time=GETDATE()
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\fplet\OneDrive\Documents\Something\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR=',',
			TABLOCK
		)
				SET @end_time=GETDATE();
PRINT 'TIME TAKEN TO COMPLETE: ' + 
      CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';

		TRUNCATE TABLE bronze.erp_loc_a101
		SET @start_time=GETDATE()
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\fplet\OneDrive\Documents\Something\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR=',',
			TABLOCK
		)
				SET @end_time=GETDATE();
PRINT 'TIME TAKEN TO COMPLETE: ' + 
      CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';





		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		SET @start_time=GETDATE()
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\fplet\OneDrive\Documents\Something\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR=',',
			TABLOCK
		)
		SET @end_time=GETDATE();
		PRINT 'TIME TAKEN TO COMPLETE: ' + 
      CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';

	  SET @end_bronze_time=GETDATE()
	  PRINT 'TIME TAKEN TO Complete Load Bronze Layer' + CAST(DATEDIFF(SECOND,@start_bronze_time,@end_bronze_time)as NVARCHAR(20)) + ' Seconds' 

	END TRY
	BEGIN CATCH
		PRINT('Error Occurred: ' + ERROR_MESSAGE());
        PRINT('Error Line: ' + CAST(ERROR_LINE() AS NVARCHAR(10)));
        PRINT('Error Procedure: ' + ISNULL(ERROR_PROCEDURE(), 'N/A'));
	END CATCH
END

EXEC bronze.load_bronze
