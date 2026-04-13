-- ============================================================
-- Stored Procedure: silver.usp_Load_FactSalesOrder
-- Purpose: Incremental load for fact table from CDC data
-- Author: Data Warehouse Team
-- Date: 2024
-- ============================================================

CREATE OR ALTER PROCEDURE silver.usp_Load_FactSalesOrder
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
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- ========================================================
        -- STEP 1: Handle INSERTS (Operation Code 2)
        -- Append new sales order details
        -- ========================================================
        INSERT INTO silver.FactSalesOrder (
            SalesOrderID, SalesOrderDetailID, CustomerID, ProductID,
            OrderDate, ShipDate, DueDate, OrderQty, UnitPrice,
            UnitPriceDiscount, LineTotal, TerritoryID,
            LoadLSN, LoadDateTime, BatchID, CDC_Operation
        )
        SELECT 
            s.SalesOrderID,
            s.SalesOrderDetailID,
            h.CustomerID,
            s.ProductID,
            h.OrderDate,
            h.ShipDate,
            h.DueDate,
            s.OrderQty,
            s.UnitPrice,
            s.UnitPriceDiscount,
            s.LineTotal,
            h.TerritoryID,
            s.__$start_lsn AS LoadLSN,
            @ProcessDate AS LoadDateTime,
            @BatchID AS BatchID,
            s.__$operation AS CDC_Operation
        FROM staging.SalesOrderDetail_CDC s
        INNER JOIN staging.SalesOrderHeader_CDC h 
            ON s.SalesOrderID = h.SalesOrderID
        WHERE 
            s.__$operation = 2  -- INSERT only
            AND NOT EXISTS (
                SELECT 1 FROM silver.FactSalesOrder f
                WHERE f.SalesOrderDetailID = s.SalesOrderDetailID
            );
            
        SET @RowsInserted = @@ROWCOUNT;
        
        -- ========================================================
        -- STEP 2: Handle UPDATES (Operation Code 4)
        -- Update existing records (for corrections)
        -- ========================================================
        
        -- Only allow updates within 7 days of order date
        UPDATE f
        SET 
            f.OrderQty = s.OrderQty,
            f.UnitPrice = s.UnitPrice,
            f.UnitPriceDiscount = s.UnitPriceDiscount,
            f.LineTotal = s.LineTotal,
            f.LoadLSN = s.__$start_lsn,
            f.LoadDateTime = @ProcessDate,
            f.BatchID = @BatchID
        FROM silver.FactSalesOrder f
        INNER JOIN staging.SalesOrderDetail_CDC s 
            ON f.SalesOrderDetailID = s.SalesOrderDetailID
        WHERE 
            s.__$operation = 4  -- UPDATE AFTER
            AND f.OrderDate >= DATEADD(DAY, -7, GETDATE())
            AND (
                f.OrderQty != s.OrderQty
                OR f.UnitPrice != s.UnitPrice
                OR f.UnitPriceDiscount != s.UnitPriceDiscount
            );
            
        SET @RowsUpdated = @@ROWCOUNT;
        
        -- ========================================================
        -- STEP 3: Log audit information
        -- ========================================================
        EXEC control.usp_LogAudit
            @BatchID = @BatchID,
            @ProcessName = 'silver.usp_Load_FactSalesOrder',
            @TableName = 'silver.FactSalesOrder',
            @RowsInserted = @RowsInserted,
            @RowsUpdated = @RowsUpdated,
            @StartTime = @StartTime,
            @Status = 'SUCCESS';
        
        COMMIT TRANSACTION;
        
        SELECT @RowsInserted AS RowsInserted, @RowsUpdated AS RowsUpdated;
            
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        
        EXEC control.usp_LogError
            @BatchID = @BatchID,
            @ProcessName = 'silver.usp_Load_FactSalesOrder',
            @TableName = 'silver.FactSalesOrder',
            @ErrorMessage = ERROR_MESSAGE();
        
        THROW;
    END CATCH
END;
GO