-- Execute scripts in order
USE AdventureWorks2022;
EXEC sys.sp_cdc_enable_db;):


--------------------------------------------------------------
-- Enable base on table need to capture data

EXEC sys.sp_cdc_enable_table
@source_schema = 'Sales',
@source_name = 'SalesOrderHeader',
@role_name = NULL; -- Allows all users to access change data
