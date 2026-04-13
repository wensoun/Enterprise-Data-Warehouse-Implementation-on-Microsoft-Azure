-- ============================================================
-- Script: 02_create_control_tables.sql
-- Purpose: Create watermark and audit tables for CDC tracking
-- ============================================================

-- Step 1: Create control schema
CREATE SCHEMA control;
GO

-- Step 2: Create watermark table for LSN tracking
CREATE TABLE control.CDCWatermark (
    TableName NVARCHAR(100) NOT NULL,
    CaptureInstance NVARCHAR(100) NOT NULL,
    LastLSN BINARY(10) NULL,
    LastProcessedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT PK_CDCWatermark PRIMARY KEY (TableName, CaptureInstance)
);
GO

-- Step 3: Insert initial watermark records
INSERT INTO control.CDCWatermark (TableName, CaptureInstance, LastLSN, LastProcessedDate)
VALUES 
    ('SalesOrderHeader', 'Sales_SalesOrderHeader', NULL, GETUTCDATE()),
    ('SalesOrderDetail', 'Sales_SalesOrderDetail', NULL, GETUTCDATE()),
    ('Product', 'Production_Product', NULL, GETUTCDATE()),
    ('Customer', 'Sales_Customer', NULL, GETUTCDATE());
GO

-- Step 4: Create ETL Audit table
CREATE TABLE control.ETLAudit (
    AuditID INT IDENTITY(1,1) PRIMARY KEY,
    BatchID INT NOT NULL,
    ProcessName NVARCHAR(200) NOT NULL,
    TableName NVARCHAR(200) NULL,
    RowsInserted INT DEFAULT 0,
    RowsUpdated INT DEFAULT 0,
    RowsDeleted INT DEFAULT 0,
    RowsClosed INT DEFAULT 0,
    StartTime DATETIME2 NOT NULL,
    EndTime DATETIME2 NULL,
    Status NVARCHAR(50) NOT NULL,
    ErrorMessage NVARCHAR(MAX) NULL
);
GO

-- Step 5: Create ETL Error Log table
CREATE TABLE control.ETLErrorLog (
    ErrorID INT IDENTITY(1,1) PRIMARY KEY,
    BatchID INT NOT NULL,
    ProcessName NVARCHAR(200) NOT NULL,
    ErrorNumber INT,
    ErrorMessage NVARCHAR(MAX),
    ErrorDateTime DATETIME2 NOT NULL
);
GO

-- Step 6: Create stored procedure to update watermark
CREATE PROCEDURE control.usp_UpdateWatermark
    @TableName NVARCHAR(100),
    @CaptureInstance NVARCHAR(100),
    @LastLSN BINARY(10)
AS
BEGIN
    SET NOCOUNT ON;
    
    UPDATE control.CDCWatermark
    SET LastLSN = @LastLSN,
        LastProcessedDate = GETUTCDATE()
    WHERE TableName = @TableName 
      AND CaptureInstance = @CaptureInstance;
    
    IF @@ROWCOUNT = 0
    BEGIN
        INSERT INTO control.CDCWatermark (TableName, CaptureInstance, LastLSN, LastProcessedDate)
        VALUES (@TableName, @CaptureInstance, @LastLSN, GETUTCDATE());
    END
END;
GO

PRINT 'Control tables and procedures created successfully';