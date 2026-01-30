/*
===============================================================================

Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================

Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from CSV files into bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;

===============================================================================
*/












EXEC bronze.load_bronze
    CREATE OR ALTER PROCEDURE bronze.load_bronze
    AS
    BEGIN
        DECLARE @start_time DATETIME, @end_time DATETIME;

        BEGIN TRY

            PRINT '==========================================';
            PRINT 'Load Bronze Layer';
            PRINT '==========================================';

            PRINT '------------------------------------------';
            PRINT 'Loading CRM Tables';
            PRINT '------------------------------------------';

            -- ================= CRM CUST INFO =================
            SET @start_time = GETDATE();
            PRINT '>> Truncating Table: bronze.crm_cust_info';
            TRUNCATE TABLE bronze.crm_cust_info;

            PRINT '>> Inserting Data Into: bronze.crm_cust_info';
            BULK INSERT bronze.crm_cust_info
            FROM 'C:\Users\IranelGonzalez\SQL Projects and Course\Warehouse-Project\datasets\source_crm\cust_info.csv'
            WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
            );

            SET @end_time = GETDATE();
            PRINT 'Load Duration: ' 
                + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
            PRINT '-----------------------------------------------';

            -- ================= CRM PRD INFO =================
            SET @start_time = GETDATE();
            PRINT '>> Truncating Table: bronze.crm_prd_info';
            TRUNCATE TABLE bronze.crm_prd_info;

            PRINT '>> Inserting Data Into: bronze.crm_prd_info';
            BULK INSERT bronze.crm_prd_info
            FROM 'C:\Users\IranelGonzalez\SQL Projects and Course\Warehouse-Project\datasets\source_crm\prd_info.csv'
            WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
            );

            SET @end_time = GETDATE();
            PRINT 'Load Duration: ' 
                + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
            PRINT '-----------------------------------------------';

            -- ================= CRM SALES DETAILS =================
            SET @start_time = GETDATE();
            PRINT '>> Truncating Table: bronze.crm_sales_details';
            TRUNCATE TABLE bronze.crm_sales_details;

            PRINT '>> Inserting Data Into: bronze.crm_sales_details';
            BULK INSERT bronze.crm_sales_details
            FROM 'C:\Users\IranelGonzalez\SQL Projects and Course\Warehouse-Project\datasets\source_crm\sales_details.csv'
            WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
            );

            SET @end_time = GETDATE();
            PRINT 'Load Duration: ' 
                + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
            PRINT '-----------------------------------------------';

            PRINT '------------------------------------------';
            PRINT 'Loading ERP Tables';
            PRINT '------------------------------------------';

            -- ================= ERP CUST =================
            SET @start_time = GETDATE();
            TRUNCATE TABLE bronze.erp_cust_az12;

            BULK INSERT bronze.erp_cust_az12
            FROM 'C:\Users\IranelGonzalez\SQL Projects and Course\Warehouse-Project\datasets\source_erp\CUST_AZ12.csv'
            WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
            );

            SET @end_time = GETDATE();
            PRINT 'Load Duration: ' 
                + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

            -- ================= ERP LOC =================
            SET @start_time = GETDATE();
            TRUNCATE TABLE bronze.erp_loc_a101;

            BULK INSERT bronze.erp_loc_a101
            FROM 'C:\Users\IranelGonzalez\SQL Projects and Course\Warehouse-Project\datasets\source_erp\LOC_A101.csv'
            WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
            );

            SET @end_time = GETDATE();
            PRINT 'Load Duration: ' 
                + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

            -- ================= ERP PX =================
            SET @start_time = GETDATE();
            TRUNCATE TABLE bronze.erp_px_cat_g1v2;

            BULK INSERT bronze.erp_px_cat_g1v2
            FROM 'C:\Users\IranelGonzalez\SQL Projects and Course\Warehouse-Project\datasets\source_erp\PX_CAT_G1V2.csv'
            WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
            );

            SET @end_time = GETDATE();
            PRINT 'Load Duration: ' 
                + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        END TRY
        BEGIN CATCH
            PRINT '=====================================';
            PRINT 'ERROR DURING BRONZE LOAD';
            PRINT ERROR_MESSAGE();
            PRINT ERROR_NUMBER();
            PRINT ERROR_STATE();
            PRINT '=====================================';
        END CATCH
    END;

