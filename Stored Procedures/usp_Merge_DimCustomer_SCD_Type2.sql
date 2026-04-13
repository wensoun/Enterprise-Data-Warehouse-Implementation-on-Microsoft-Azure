-- ============================================================
-- Stored Procedure: silver.usp_Merge_DimCustomer_SCD_Type2
-- Purpose: Implements SCD Type 2 logic for Customer dimension
-- Author: Data Warehouse Team
-- Date: 2024
-- ============================================================

CREATE OR ALTER PROCEDURE silver.usp_Merge_DimCustomer_SCD_Type2
    @BatchID INT,
    @ProcessDate DATETIME2 = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    
    IF @ProcessDate IS NULL
        SET @ProcessDate = GETDATE();
    
    DECLARE @StartTime DATETIME2 = GETDATE();
    DECLARE @RowsInserted INT = 0;
    DECLARE @RowsClosed INT = 0;
    DECLARE @RowsDeleted INT = 0;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- ========================================================
        -- STEP 1: Handle DELETES (Operation Code 1)
        -- ========================================================
        UPDATE t
        SET 
            t.ValidTo = @ProcessDate,
            t.IsCurrent = 0,
            t.BatchID = @BatchID
        FROM silver.DimCustomer t
        INNER JOIN staging.Customer_CDC s 
            ON t.CustomerID = s.CustomerID
        WHERE 
            t.IsCurrent = 1
            AND s.__$operation = 1;
            
        SET @RowsClosed = @RowsClosed + @@ROWCOUNT;
        SET @RowsDeleted = @RowsDeleted + @@ROWCOUNT;
        
        -- ========================================================
        -- STEP 2: Handle INSERTS (Operation Code 2)
        -- ========================================================
        INSERT INTO silver.DimCustomer (
            CustomerID, PersonID, StoreID, TerritoryID, AccountNumber, CustomerType,
            ValidFrom, ValidTo, IsCurrent, LoadLSN, LoadDateTime, BatchID
        )
        SELECT 
            s.CustomerID, s.PersonID, s.StoreID, s.TerritoryID, s.AccountNumber, s.CustomerType,
            @ProcessDate, '9999-12-31', 1, s.__$start_lsn, GETDATE(), @BatchID
        FROM staging.Customer_CDC s
        WHERE 
            s.__$operation = 2
            AND NOT EXISTS (
                SELECT 1 FROM silver.DimCustomer t
                WHERE t.CustomerID = s.CustomerID
            );
            
        SET @RowsInserted = @RowsInserted + @@ROWCOUNT;
        
        -- ========================================================
        -- STEP 3: Handle UPDATES (Operation Code 4)
        -- ========================================================
        
        -- Identify customers with actual changes
        WITH ChangedCustomers AS (
            SELECT DISTINCT s.CustomerID
            FROM staging.Customer_CDC s
            INNER JOIN silver.DimCustomer t 
                ON s.CustomerID = t.CustomerID
            WHERE 
                s.__$operation = 4
                AND t.IsCurrent = 1
                AND (
                    ISNULL(s.TerritoryID, -1) != ISNULL(t.TerritoryID, -1)
                    OR ISNULL(s.CustomerType, '') != ISNULL(t.CustomerType, '')
                    OR ISNULL(s.AccountNumber, '') != ISNULL(t.AccountNumber, '')
                )
        )
        
        -- Close current versions
        UPDATE t
        SET 
            t.ValidTo = @ProcessDate,
            t.IsCurrent = 0,
            t.BatchID = @BatchID
        FROM silver.DimCustomer t
        INNER JOIN ChangedCustomers c ON t.CustomerID = c.CustomerID
        WHERE t.IsCurrent = 1;
        
        SET @RowsClosed = @RowsClosed + @@ROWCOUNT;
        
        -- Insert new versions
        INSERT INTO silver.DimCustomer (
            CustomerID, PersonID, StoreID, TerritoryID, AccountNumber, CustomerType,
            ValidFrom, ValidTo, IsCurrent, LoadLSN, LoadDateTime, BatchID
        )
        SELECT 
            s.CustomerID, s.PersonID, s.StoreID, s.TerritoryID, s.AccountNumber, s.CustomerType,
            @ProcessDate, '9999-12-31', 1, s.__$start_lsn, GETDATE(), @BatchID
        FROM staging.Customer_CDC s
        WHERE 
            s.__$operation = 4
            AND EXISTS (
                SELECT 1 FROM silver.DimCustomer t
                WHERE t.CustomerID = s.CustomerID
                AND t.ValidTo = @ProcessDate
            );
            
        SET @RowsInserted = @RowsInserted + @@ROWCOUNT;
        
        -- ========================================================
        -- STEP 4: Log audit information
        -- ========================================================
        EXEC control.usp_LogAudit
            @BatchID = @BatchID,
            @ProcessName = 'silver.usp_Merge_DimCustomer_SCD_Type2',
            @TableName = 'silver.DimCustomer',
            @RowsInserted = @RowsInserted,
            @RowsDeleted = @RowsDeleted,
            @RowsClosed = @RowsClosed,
            @StartTime = @StartTime,
            @Status = 'SUCCESS';
        
        COMMIT TRANSACTION;
        
        SELECT 
            @RowsInserted AS RowsInserted,
            @RowsDeleted AS RowsDeleted,
            @RowsClosed AS RowsClosed;
            
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        
        EXEC control.usp_LogError
            @BatchID = @BatchID,
            @ProcessName = 'silver.usp_Merge_DimCustomer_SCD_Type2',
            @TableName = 'silver.DimCustomer',
            @ErrorMessage = ERROR_MESSAGE();
        
        THROW;
    END CATCH
END;
GO