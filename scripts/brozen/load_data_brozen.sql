/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROCEDURE  brozen.load_brozen
as
begin

DECLARE @start_time datetime, @end_time datetime, @batch_start_time datetime,@batch_end_time datetime;
BEGIN TRY
SET @batch_start_time = GETDATE();
print '==========================='
print 'LOADING BROZEN LAYER'
PRINT '==========================='

set @start_time = getdate();
print 'TRUNCATING THE TABLE brozen.crm_cust_info';
truncate table brozen.crm_cust_info;
print 'INSERTING THE DATA INTO brozen.crm_cust_info';

bulk insert brozen.crm_cust_info
from 'C:\Users\chait\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
with(
firstrow = 2,
fieldterminator = ',',
tablock
);
set @end_time =getdate() 
print 'LOAD DURATION' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
print '.......................................................................................'

SET @batch_start_time = GETDATE();
print '==========================='
print 'LOADING BROZEN LAYER'
PRINT '==========================='
set @start_time = getdate();
print 'TRUNCATING THE TABLE brozen.crm_prd_info';
truncate table brozen.crm_prd_info;
print 'INSERTING THE DATA INTO brozen.crm_prd_info';
bulk insert brozen.crm_prd_info
from 'C:\prd_info.csv'
with
(
firstrow = 2,
fieldterminator = ',',
tablock
);
set @end_time =getdate() 
print 'LOAD DURATION' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
print '.......................................................................................'




SET @batch_start_time = GETDATE();
print '==========================='
print 'LOADING BROZEN LAYER'
PRINT '==========================='
set @start_time = getdate();
print 'TRUNCATING THE TABLE brozen.crm_sales_details';
truncate table brozen.crm_sales_details;
print 'INSERTING THE DATA INTO brozen.crm_sales_details';
bulk insert brozen.crm_sales_details
from 'C:\Users\chait\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
with
(
firstrow = 2,
fieldterminator = ',',
tablock
);
set @end_time =getdate() 
print 'LOAD DURATION' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
print '.......................................................................................'





SET @batch_start_time = GETDATE();
print '==========================='
print 'LOADING BROZEN LAYER'
PRINT '==========================='
set @start_time = getdate();
print 'TRUNCATING THE TABLE brozen.erp_cust_az12';
truncate table brozen.erp_cust_az12;
print 'INSERTING THE DATA INTO brozen.erp_cust_az12';
bulk insert brozen.erp_cust_az12
from 'C:\Users\chait\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
with(
firstrow = 2,
fieldterminator = ',',
tablock
);
set @end_time =getdate() 
print 'LOAD DURATION' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
print '.......................................................................................'




SET @batch_start_time = GETDATE();
print '==========================='
print 'LOADING BROZEN LAYER'
PRINT '==========================='
set @start_time = getdate();
print 'TRUNCATING THE TABLE brozen.erp_loc_a101';
truncate table brozen.erp_loc_a101
print 'INSERTING THE DATA INTO brozen.erp_loc_a101';
bulk insert brozen.erp_loc_a101
from 'C:\Users\chait\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
with(
firstrow = 2,
fieldterminator = ',',
tablock
);
set @end_time =getdate() 
print 'LOAD DURATION' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
print '.......................................................................................'



SET @batch_start_time = GETDATE();
print '==========================='
print 'LOADING BROZEN LAYER'
PRINT '==========================='
set @start_time = getdate();
print 'TRUNCATING THE TABLE  brozen.erp_px_cat_g1v2';
truncate table  brozen.erp_px_cat_g1v2
print 'INSERTING THE DATA INTO  brozen.erp_px_cat_g1v2';
bulk insert brozen.erp_px_cat_g1v2
from 'C:\Users\chait\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
with(
firstrow = 2,
fieldterminator = ',',
tablock
);
set @end_time =getdate() 
print 'LOAD DURATION' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
print '.......................................................................................'

SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
END TRY

BEGIN CATCH
PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
END CATCH

end


exec brozen.load_brozen
