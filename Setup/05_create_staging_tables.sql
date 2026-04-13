-- ============================================================
-- Script: 05_create_staging_tables.sql
-- Purpose: Create staging tables for CDC data processing
-- Author: Data Warehouse Team
-- Date: 2024
-- ============================================================

/*
INSTRUCTIONS:
1. Run this script on Azure Synapse Dedicated SQL Pool
2. Staging tables are used for temporary storage of CDC changes
3. These tables are truncated after each ETL run
*/

-- ============================================================
-- STEP 1: Create Staging Schema
-- ============================================================
PRINT '========================================';
PRINT 'STEP 1: Creating Staging Schema';
PRINT '========================================';

CREATE SCHEMA IF NOT EXISTS staging;
GO

-- ============================================================
-- STEP 2: Create Product Staging Table
-- ============================================================
PRINT '========================================';
PRINT 'STEP 2: Creating Product Staging Table';
PRINT '========================================';

CREATE TABLE staging.Product_CDC (
    ProductID INT,
    Name NVARCHAR(50),
    ProductNumber NVARCHAR(25),
    Color NVARCHAR(15),
    StandardCost MONEY,
    ListPrice MONEY,
    ProductSubcategoryID INT,
    ProductModelID INT,
    SellStartDate DATE,
    SellEndDate DATE,
    DiscontinuedDate DATE,
    __$start_lsn BINARY(10),
    __$operation INT,
    operation_type VARCHAR(20),
    _row_hash VARCHAR(64),
    _processing_date DATETIME2,
    _source_file VARCHAR(500)
)
WITH (
    DISTRIBUTION = ROUND_ROBIN,
    HEAP
);
GO

-- ============================================================
-- STEP 3: Create Customer Staging Table
-- ============================================================
PRINT '========================================';
PRINT 'STEP 3: Creating Customer Staging Table';
PRINT '========================================';

CREATE TABLE staging.Customer_CDC (
    CustomerID INT,
    PersonID INT,
    StoreID INT,
    TerritoryID INT,
    AccountNumber VARCHAR(10),
    CustomerType CHAR(1),
    __$start_lsn BINARY(10),
    __$operation INT,
    operation_type VARCHAR(20),
    _row_hash VARCHAR(64),
    _processing_date DATETIME2,
    _source_file VARCHAR(500)
)
WITH (
    DISTRIBUTION = ROUND_ROBIN,
    HEAP
);
GO

-- ============================================================
-- STEP 4: Create SalesOrderDetail Staging Table
-- ============================================================
PRINT '========================================';
PRINT 'STEP 4: Creating SalesOrderDetail Staging Table';
PRINT '========================================';

CREATE TABLE staging.SalesOrderDetail_CDC (
    SalesOrderID INT,
    SalesOrderDetailID INT,
    OrderQty SMALLINT,
    ProductID INT,
    UnitPrice MONEY,
    UnitPriceDiscount MONEY,
    LineTotal MONEY,
    __$start_lsn BINARY(10),
    __$operation INT,
    operation_type VARCHAR(20),
    _processing_date DATETIME2,
    _source_file VARCHAR(500)
)
WITH (
    DISTRIBUTION = ROUND_ROBIN,
    HEAP
);
GO

-- ============================================================
-- STEP 5: Create SalesOrderHeader Staging Table
-- ============================================================
PRINT '========================================';
PRINT 'STEP 5: Creating SalesOrderHeader Staging Table';
PRINT '========================================';

CREATE TABLE staging.SalesOrderHeader_CDC (
    SalesOrderID INT,
    RevisionNumber TINYINT,
    OrderDate DATE,
    DueDate DATE,
    ShipDate DATE,
    Status TINYINT,
    OnlineOrderFlag BIT,
    CustomerID INT,
    TerritoryID INT,
    SubTotal MONEY,
    TaxAmt MONEY,
    Freight MONEY,
    TotalDue MONEY,
    __$start_lsn BINARY(10),
    __$operation INT,
    operation_type VARCHAR(20),
    _processing_date DATETIME2,
    _source_file VARCHAR(500)
)
WITH (
    DISTRIBUTION = ROUND_ROBIN,
    HEAP
);
GO

-- ============================================================
-- STEP 6: Create Stored Procedure to Truncate Staging Tables
-- ============================================================
PRINT '========================================';
PRINT 'STEP 6: Creating Truncate Staging Procedure';
PRINT '========================================';

CREATE OR ALTER PROCEDURE staging.usp_Truncate_Staging_Tables
AS
BEGIN
    SET NOCOUNT ON;
    
    TRUNCATE TABLE staging.Product_CDC;
    TRUNCATE TABLE staging.Customer_CDC;
    TRUNCATE TABLE staging.SalesOrderDetail_CDC;
    TRUNCATE TABLE staging.SalesOrderHeader_CDC;
    
    PRINT 'All staging tables truncated successfully';
END;
GO

-- ============================================================
-- STEP 7: Create Stored Procedure to Get Unprocessed CDC Data
-- ============================================================
PRINT '========================================';
PRINT 'STEP 7: Creating Get Unprocessed CDC Procedure';
PRINT '========================================';

CREATE OR ALTER PROCEDURE staging.usp_GetUnprocessedCDC
    @TableName NVARCHAR(100),
    @LastLSN BINARY(10) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @SQL NVARCHAR(MAX);
    
    IF @TableName = 'Product'
    BEGIN
        SELECT * FROM staging.Product_CDC
        WHERE __$start_lsn > ISNULL(@LastLSN, 0x00000000000000000000)
        ORDER BY __$start_lsn;
    END
    ELSE IF @TableName = 'Customer'
    BEGIN
        SELECT * FROM staging.Customer_CDC
        WHERE __$start_lsn > ISNULL(@LastLSN, 0x00000000000000000000)
        ORDER BY __$start_lsn;
    END
    ELSE IF @TableName = 'SalesOrderDetail'
    BEGIN
        SELECT * FROM staging.SalesOrderDetail_CDC
        WHERE __$start_lsn > ISNULL(@LastLSN, 0x00000000000000000000)
        ORDER BY __$start_lsn;
    END
    ELSE IF @TableName = 'SalesOrderHeader'
    BEGIN
        SELECT * FROM staging.SalesOrderHeader_CDC
        WHERE __$start_lsn > ISNULL(@LastLSN, 0x00000000000000000000)
        ORDER BY __$start_lsn;
    END
END;
GO

-- ============================================================
-- SUMMARY
-- ============================================================
PRINT '========================================';
PRINT 'STAGING TABLES CREATION COMPLETE';
PRINT '========================================';
PRINT 'Created Tables:';
PRINT '- staging.Product_CDC';
PRINT '- staging.Customer_CDC';
PRINT '- staging.SalesOrderDetail_CDC';
PRINT '- staging.SalesOrderHeader_CDC';
PRINT '';
PRINT 'Created Procedures:';
PRINT '- staging.usp_Truncate_Staging_Tables';
PRINT '- staging.usp_GetUnprocessedCDC';
PRINT '';
PRINT 'Next Steps:';
PRINT '1. Deploy ADF pipelines from /adf-pipelines folder';
PRINT '2. Deploy stored procedures from /stored-procedures folder';
PRINT '========================================';
GO