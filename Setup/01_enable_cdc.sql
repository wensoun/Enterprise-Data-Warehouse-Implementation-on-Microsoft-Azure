-- ============================================================
-- Script: 01_enable_cdc.sql
-- Purpose: Enable Change Data Capture on SQL Server database
-- ============================================================

USE AdventureWorks2022;
GO

-- Step 1: Check if CDC is already enabled
SELECT name, is_cdc_enabled 
FROM sys.databases 
WHERE name = 'AdventureWorks2022';
GO

-- Step 2: Enable CDC at database level
EXEC sys.sp_cdc_enable_db;
GO

-- Step 3: Verify CDC is enabled
SELECT name, is_cdc_enabled 
FROM sys.databases 
WHERE name = 'AdventureWorks2022';
-- Expected: is_cdc_enabled = 1
GO

-- Step 4: Enable CDC on specific tables

-- SalesOrderHeader
EXEC sys.sp_cdc_enable_table
    @source_schema = 'Sales',
    @source_name = 'SalesOrderHeader',
    @role_name = NULL,
    @filegroup_name = 'PRIMARY',
    @capture_instance = 'Sales_SalesOrderHeader';
GO

-- SalesOrderDetail
EXEC sys.sp_cdc_enable_table
    @source_schema = 'Sales',
    @source_name = 'SalesOrderDetail',
    @role_name = NULL,
    @filegroup_name = 'PRIMARY',
    @capture_instance = 'Sales_SalesOrderDetail';
GO

-- Product
EXEC sys.sp_cdc_enable_table
    @source_schema = 'Production',
    @source_name = 'Product',
    @role_name = NULL,
    @filegroup_name = 'PRIMARY',
    @capture_instance = 'Production_Product';
GO

-- Customer
EXEC sys.sp_cdc_enable_table
    @source_schema = 'Sales',
    @source_name = 'Customer',
    @role_name = NULL,
    @filegroup_name = 'PRIMARY',
    @capture_instance = 'Sales_Customer';
GO

-- Step 5: Verify CDC tables were created
SELECT 
    s.name AS schema_name,
    t.name AS table_name,
    t.is_tracked_by_cdc
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE t.is_tracked_by_cdc = 1;
GO

-- Step 6: View CDC capture instances
SELECT 
    capture_instance,
    source_schema,
    source_name,
    start_lsn
FROM cdc.change_tables;
GO

PRINT 'CDC has been successfully enabled on AdventureWorks2022 database';