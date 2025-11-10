/* 
=============================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=============================================================
Script Purpose:
    Loads CSV files into 'bronze' tables using BULK INSERT.
    - Truncates target tables before load
    - Prints per-table and total durations

Prereqs:
    - SQL Server with access to BULK files
    - Login has bulkadmin or ADMINISTER BULK OPERATIONS
    - CSVs present under @base_path
Usage:
    EXEC bronze.load_bronze;
=============================================================
*/

-- Ensure schema exists (safe for repeated runs)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');

GO

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE 
        @start_time DATETIME2(0),
        @end_time   DATETIME2(0),
        @total_start DATETIME2(0),
        @total_end   DATETIME2(0),
        @base_path NVARCHAR(260) = N'C:\path\to\sql-data-warehouse-project\datasets';  -- TODO: change for your machine
    DECLARE 
        @crm_cust   NVARCHAR(400) = @base_path + N'\source_crm\cust_info.csv',
        @crm_prd    NVARCHAR(400) = @base_path + N'\source_crm\prd_info.csv',
        @crm_sales  NVARCHAR(400) = @base_path + N'\source_crm\sales_details.csv',
        @erp_loc    NVARCHAR(400) = @base_path + N'\source_erp\LOC_A101.csv',
        @erp_cust   NVARCHAR(400) = @base_path + N'\source_erp\CUST_AZ12.csv',
        @erp_px     NVARCHAR(400) = @base_path + N'\source_erp\PX_CAT_G1V2.csv';

    SET @total_start = SYSDATETIME();

    BEGIN TRY
        PRINT '=============================';
        PRINT 'Loading Bronze Layer';
        PRINT '=============================';

        PRINT '-----------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '-----------------------------';

        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting Data Into: bronze.crm_cust_info';
        SET @start_time = SYSDATETIME();
        BULK INSERT bronze.crm_cust_info FROM @crm_cust
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = SYSDATETIME();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';

        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting Data Into: bronze.crm_prd_info';
        SET @start_time = SYSDATETIME();
        BULK INSERT bronze.crm_prd_info FROM @crm_prd
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = SYSDATETIME();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';

        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting Data Into: bronze.crm_sales_details';
        SET @start_time = SYSDATETIME();
        BULK INSERT bronze.crm_sales_details FROM @crm_sales
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = SYSDATETIME();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';

        PRINT '-----------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '-----------------------------';

        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
        SET @start_time = SYSDATETIME();
        BULK INSERT bronze.erp_loc_a101 FROM @erp_loc
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = SYSDATETIME();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';

        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
        SET @start_time = SYSDATETIME();
        BULK INSERT bronze.erp_cust_az12 FROM @erp_cust
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = SYSDATETIME();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';

        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
        SET @start_time = SYSDATETIME();
        BULK INSERT bronze.erp_px_cat_g1v2 FROM @erp_px
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = SYSDATETIME();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';

        SET @total_end = SYSDATETIME();
        PRINT '=============================';
        PRINT 'Bronze Layer Load Completed';
        PRINT 'Total Duration: ' + CAST(DATEDIFF(second, @total_start, @total_end) AS NVARCHAR(20)) + ' seconds';
        PRINT '=============================';
    END TRY
    BEGIN CATCH
        PRINT '==========================================';
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR(10));
        PRINT '==========================================';
    END CATCH;
END;
GO

-- Example run (leave commented in repo)
-- EXEC bronze.load_bronze;



