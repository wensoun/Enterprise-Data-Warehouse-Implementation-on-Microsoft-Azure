-- ============================================================
-- usp_Populate_DimProduct
-- ============================================================
CREATE OR ALTER PROCEDURE gold.usp_Populate_DimProduct
    @BatchID INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartTime DATETIME2 = GETDATE();
    DECLARE @RowCount INT = 0;
    
    BEGIN TRY
        -- Truncate and reload
        TRUNCATE TABLE gold.DimProduct;
        
        INSERT INTO gold.DimProduct (
            ProductID, ProductName, ProductNumber, Color,
            StandardCost, ListPrice, SubcategoryName, CategoryName,
            ProductCategoryID, ProductSubcategoryID, IsCurrent,
            ValidFrom, ValidTo, LoadDateTime
        )
        SELECT 
            p.ProductID,
            p.Name AS ProductName,
            p.ProductNumber,
            p.Color,
            p.StandardCost,
            p.ListPrice,
            ps.Name AS SubcategoryName,
            pc.Name AS CategoryName,
            ps.ProductCategoryID,
            p.ProductSubcategoryID,
            p.IsCurrent,
            p.ValidFrom,
            p.ValidTo,
            GETDATE() AS LoadDateTime
        FROM silver.DimProduct p
        LEFT JOIN Production.ProductSubcategory ps 
            ON p.ProductSubcategoryID = ps.ProductSubcategoryID
        LEFT JOIN Production.ProductCategory pc 
            ON ps.ProductCategoryID = pc.ProductCategoryID
        WHERE p.IsCurrent = 1;
        
        SET @RowCount = @@ROWCOUNT;
        
        -- Log audit
        EXEC control.usp_LogAudit
            @BatchID = @BatchID,
            @ProcessName = 'gold.usp_Populate_DimProduct',
            @TableName = 'gold.DimProduct',
            @RowsInserted = @RowCount,
            @StartTime = @StartTime,
            @Status = 'SUCCESS';
        
        RETURN @RowCount;
    END TRY
    BEGIN CATCH
        EXEC control.usp_LogError
            @BatchID = @BatchID,
            @ProcessName = 'gold.usp_Populate_DimProduct',
            @TableName = 'gold.DimProduct',
            @ErrorMessage = ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO

-- ============================================================
-- usp_Populate_DimCustomer
-- ============================================================
CREATE OR ALTER PROCEDURE gold.usp_Populate_DimCustomer
    @BatchID INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartTime DATETIME2 = GETDATE();
    DECLARE @RowCount INT = 0;
    
    BEGIN TRY
        TRUNCATE TABLE gold.DimCustomer;
        
        INSERT INTO gold.DimCustomer (
            CustomerID, FullName, AccountNumber, CustomerType,
            City, StateProvinceName, CountryRegionName,
            TerritoryID, TerritoryName, IsCurrent,
            ValidFrom, ValidTo, LoadDateTime
        )
        SELECT 
            c.CustomerID,
            CONCAT(p.FirstName, ' ', p.LastName) AS FullName,
            c.AccountNumber,
            CASE c.CustomerType WHEN 'I' THEN 'Individual' ELSE 'Store' END AS CustomerType,
            a.City,
            sp.Name AS StateProvinceName,
            cr.Name AS CountryRegionName,
            c.TerritoryID,
            t.Name AS TerritoryName,
            c.IsCurrent,
            c.ValidFrom,
            c.ValidTo,
            GETDATE() AS LoadDateTime
        FROM silver.DimCustomer c
        LEFT JOIN Person.Person p ON c.PersonID = p.BusinessEntityID
        LEFT JOIN Sales.CustomerAddress ca ON c.CustomerID = ca.CustomerID AND ca.AddressTypeID = 2
        LEFT JOIN Person.Address a ON ca.AddressID = a.AddressID
        LEFT JOIN Person.StateProvince sp ON a.StateProvinceID = sp.StateProvinceID
        LEFT JOIN Person.CountryRegion cr ON sp.CountryRegionCode = cr.CountryRegionCode
        LEFT JOIN Sales.SalesTerritory t ON c.TerritoryID = t.TerritoryID
        WHERE c.IsCurrent = 1;
        
        SET @RowCount = @@ROWCOUNT;
        
        EXEC control.usp_LogAudit
            @BatchID = @BatchID,
            @ProcessName = 'gold.usp_Populate_DimCustomer',
            @TableName = 'gold.DimCustomer',
            @RowsInserted = @RowCount,
            @StartTime = @StartTime,
            @Status = 'SUCCESS';
        
        RETURN @RowCount;
    END TRY
    BEGIN CATCH
        EXEC control.usp_LogError
            @BatchID = @BatchID,
            @ProcessName = 'gold.usp_Populate_DimCustomer',
            @TableName = 'gold.DimCustomer',
            @ErrorMessage = ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO

-- ============================================================
-- usp_Populate_DimTerritory
-- ============================================================
CREATE OR ALTER PROCEDURE gold.usp_Populate_DimTerritory
    @BatchID INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartTime DATETIME2 = GETDATE();
    DECLARE @RowCount INT = 0;
    
    BEGIN TRY
        TRUNCATE TABLE gold.DimTerritory;
        
        INSERT INTO gold.DimTerritory (
            TerritoryID, TerritoryName, CountryRegionCode, GroupName, LoadDateTime
        )
        SELECT 
            TerritoryID,
            Name AS TerritoryName,
            CountryRegionCode,
            [Group] AS GroupName,
            GETDATE() AS LoadDateTime
        FROM Sales.SalesTerritory;
        
        SET @RowCount = @@ROWCOUNT;
        
        EXEC control.usp_LogAudit
            @BatchID = @BatchID,
            @ProcessName = 'gold.usp_Populate_DimTerritory',
            @TableName = 'gold.DimTerritory',
            @RowsInserted = @RowCount,
            @StartTime = @StartTime,
            @Status = 'SUCCESS';
        
        RETURN @RowCount;
    END TRY
    BEGIN CATCH
        EXEC control.usp_LogError
            @BatchID = @BatchID,
            @ProcessName = 'gold.usp_Populate_DimTerritory',
            @TableName = 'gold.DimTerritory',
            @ErrorMessage = ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO

-- ============================================================
-- usp_Populate_FactSales
-- ============================================================
CREATE OR ALTER PROCEDURE gold.usp_Populate_FactSales
    @BatchID INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartTime DATETIME2 = GETDATE();
    DECLARE @RowCount INT = 0;
    
    BEGIN TRY
        TRUNCATE TABLE gold.FactSales;
        
        INSERT INTO gold.FactSales (
            SalesOrderID, SalesOrderDetailID,
            ProductKey, CustomerKey, OrderDateKey, ShipDateKey, TerritoryKey,
            OrderQty, UnitPrice, UnitPriceDiscount, LineTotal, LoadDateTime
        )
        SELECT 
            f.SalesOrderID,
            f.SalesOrderDetailID,
            dp.ProductKey,
            dc.CustomerKey,
            dd.DateKey AS OrderDateKey,
            ds.DateKey AS ShipDateKey,
            dt.TerritoryKey,
            f.OrderQty,
            f.UnitPrice,
            f.UnitPriceDiscount,
            f.LineTotal,
            GETDATE() AS LoadDateTime
        FROM silver.FactSalesOrder f
        INNER JOIN gold.DimProduct dp ON f.ProductID = dp.ProductID AND dp.IsCurrent = 1
        INNER JOIN gold.DimCustomer dc ON f.CustomerID = dc.CustomerID AND dc.IsCurrent = 1
        INNER JOIN gold.DimDate dd ON f.OrderDate = dd.Date
        LEFT JOIN gold.DimDate ds ON f.ShipDate = ds.Date
        LEFT JOIN gold.DimTerritory dt ON f.TerritoryID = dt.TerritoryID;
        
        SET @RowCount = @@ROWCOUNT;
        
        EXEC control.usp_LogAudit
            @BatchID = @BatchID,
            @ProcessName = 'gold.usp_Populate_FactSales',
            @TableName = 'gold.FactSales',
            @RowsInserted = @RowCount,
            @StartTime = @StartTime,
            @Status = 'SUCCESS';
        
        RETURN @RowCount;
    END TRY
    BEGIN CATCH
        EXEC control.usp_LogError
            @BatchID = @BatchID,
            @ProcessName = 'gold.usp_Populate_FactSales',
            @TableName = 'gold.FactSales',
            @ErrorMessage = ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO

PRINT 'Gold layer stored procedures created successfully';