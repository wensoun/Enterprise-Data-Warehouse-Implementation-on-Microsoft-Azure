-- ============================================================
-- Stored Procedure: silver.usp_Merge_DimProduct_SCD_Type2
-- Purpose: Implements SCD Type 2 logic for Product dimension
-- Author: Data Warehouse Team
-- Date: 2024
-- ============================================================

CREATE OR ALTER PROCEDURE silver.usp_Merge_DimProduct_SCD_Type2
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
    DECLARE @RowsUpdated INT = 0;
    DECLARE @RowsClosed INT = 0;
    DECLARE @RowsDeleted INT = 0;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- ========================================================
        -- STEP 1: Handle DELETES (Operation Code 1)
        -- Close the current version of deleted products
        -- ========================================================
        UPDATE t
        SET 
            t.ValidTo = @ProcessDate,
            t.IsCurrent = 0,
            t.BatchID = @BatchID
        FROM silver.DimProduct t
        INNER JOIN staging.Product_CDC s 
            ON t.ProductID = s.ProductID
        WHERE 
            t.IsCurrent = 1
            AND s.__$operation = 1;
            
        SET @RowsClosed = @RowsClosed + @@ROWCOUNT;
        SET @RowsDeleted = @RowsDeleted + @@ROWCOUNT;
        
        -- ========================================================
        -- STEP 2: Handle INSERTS (Operation Code 2)
        -- Insert new products that don't already exist
        -- ========================================================
        INSERT INTO silver.DimProduct (
            ProductID, Name, ProductNumber, Color, StandardCost, ListPrice,
            ProductSubcategoryID, ProductModelID, SellStartDate, SellEndDate, DiscontinuedDate,
            ValidFrom, ValidTo, IsCurrent, LoadLSN, LoadDateTime, BatchID
        )
        SELECT 
            s.ProductID, s.Name, s.ProductNumber, s.Color, s.StandardCost, s.ListPrice,
            s.ProductSubcategoryID, s.ProductModelID, s.SellStartDate, s.SellEndDate, s.DiscontinuedDate,
            @ProcessDate, '9999-12-31', 1, s.__$start_lsn, GETDATE(), @BatchID
        FROM staging.Product_CDC s
        WHERE 
            s.__$operation = 2
            AND NOT EXISTS (
                SELECT 1 FROM silver.DimProduct t
                WHERE t.ProductID = s.ProductID
            );
            
        SET @RowsInserted = @RowsInserted + @@ROWCOUNT;
        
        -- ========================================================
        -- STEP 3: Handle UPDATES (Operation Code 4)
        -- SCD Type 2: Close old record, insert new version
        -- ========================================================
        
        -- Identify products with actual changes
        WITH ChangedProducts AS (
            SELECT DISTINCT s.ProductID
            FROM staging.Product_CDC s
            INNER JOIN silver.DimProduct t 
                ON s.ProductID = t.ProductID
            WHERE 
                s.__$operation = 4
                AND t.IsCurrent = 1
                AND (
                    ISNULL(s.Name, '') != ISNULL(t.Name, '')
                    OR ISNULL(s.ListPrice, 0) != ISNULL(t.ListPrice, 0)
                    OR ISNULL(s.StandardCost, 0) != ISNULL(t.StandardCost, 0)
                    OR ISNULL(s.Color, '') != ISNULL(t.Color, '')
                    OR ISNULL(s.ProductSubcategoryID, -1) != ISNULL(t.ProductSubcategoryID, -1)
                )
        )
        
        -- Close current versions
        UPDATE t
        SET 
            t.ValidTo = @ProcessDate,
            t.IsCurrent = 0,
            t.BatchID = @BatchID
        FROM silver.DimProduct t
        INNER JOIN ChangedProducts c ON t.ProductID = c.ProductID
        WHERE t.IsCurrent = 1;
        
        SET @RowsClosed = @RowsClosed + @@ROWCOUNT;
        
        -- Insert new versions
        INSERT INTO silver.DimProduct (
            ProductID, Name, ProductNumber, Color, StandardCost, ListPrice,
            ProductSubcategoryID, ProductModelID, SellStartDate, SellEndDate, DiscontinuedDate,
            ValidFrom, ValidTo, IsCurrent, LoadLSN, LoadDateTime, BatchID
        )
        SELECT 
            s.ProductID, s.Name, s.ProductNumber, s.Color, s.StandardCost, s.ListPrice,
            s.ProductSubcategoryID, s.ProductModelID, s.SellStartDate, s.SellEndDate, s.DiscontinuedDate,
            @ProcessDate, '9999-12-31', 1, s.__$start_lsn, GETDATE(), @BatchID
        FROM staging.Product_CDC s
        WHERE 
            s.__$operation = 4
            AND EXISTS (
                SELECT 1 FROM silver.DimProduct t
                WHERE t.ProductID = s.ProductID
                AND t.ValidTo = @ProcessDate
            );
            
        SET @RowsInserted = @RowsInserted + @@ROWCOUNT;
        
        -- ========================================================
        -- STEP 4: Log audit information
        -- ========================================================
        EXEC control.usp_LogAudit
            @BatchID = @BatchID,
            @ProcessName = 'silver.usp_Merge_DimProduct_SCD_Type2',
            @TableName = 'silver.DimProduct',
            @RowsInserted = @RowsInserted,
            @RowsUpdated = @RowsUpdated,
            @RowsDeleted = @RowsDeleted,
            @RowsClosed = @RowsClosed,
            @StartTime = @StartTime,
            @Status = 'SUCCESS';
        
        COMMIT TRANSACTION;
        
        -- Return summary
        SELECT 
            @RowsInserted AS RowsInserted,
            @RowsUpdated AS RowsUpdated,
            @RowsDeleted AS RowsDeleted,
            @RowsClosed AS RowsClosed;
            
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        
        EXEC control.usp_LogError
            @BatchID = @BatchID,
            @ProcessName = 'silver.usp_Merge_DimProduct_SCD_Type2',
            @TableName = 'silver.DimProduct',
            @ErrorMessage = ERROR_MESSAGE();
        
        THROW;
    END CATCH
END;
GO