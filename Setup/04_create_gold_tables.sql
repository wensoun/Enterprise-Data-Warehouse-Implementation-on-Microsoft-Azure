-- ============================================================
-- Script: 04_create_gold_tables.sql
-- Purpose: Create Gold layer (Kimball star schema) tables
-- Author: Data Warehouse Team
-- Date: 2024
-- ============================================================

/*
INSTRUCTIONS:
1. Run this script on Azure Synapse Dedicated SQL Pool
2. Creates denormalized star schema tables for Power BI
3. Gold layer is optimized for query performance
*/

-- ============================================================
-- STEP 1: Create Gold Schema
-- ============================================================
PRINT '========================================';
PRINT 'STEP 1: Creating Gold Schema';
PRINT '========================================';

CREATE SCHEMA IF NOT EXISTS gold;
GO

-- ============================================================
-- STEP 2: Create DimProduct (Denormalized with Category Hierarchy)
-- ============================================================
PRINT '========================================';
PRINT 'STEP 2: Creating DimProduct (Denormalized)';
PRINT '========================================';

CREATE TABLE gold.DimProduct (
    ProductKey INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
    ProductID INT NOT NULL,
    ProductName NVARCHAR(50) NOT NULL,
    ProductNumber NVARCHAR(25) NOT NULL,
    Color NVARCHAR(15) NULL,
    StandardCost MONEY NOT NULL,
    ListPrice MONEY NOT NULL,
    SubcategoryName NVARCHAR(50) NULL,
    CategoryName NVARCHAR(50) NULL,
    ProductCategoryID INT NULL,
    ProductSubcategoryID INT NULL,
    IsCurrent BIT NOT NULL DEFAULT 1,
    ValidFrom DATETIME2 NOT NULL,
    ValidTo DATETIME2 NOT NULL,
    LoadDateTime DATETIME2 NOT NULL DEFAULT GETDATE()
)
WITH (
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);
GO

CREATE INDEX IX_DimProduct_ProductID ON gold.DimProduct (ProductID);
CREATE INDEX IX_DimProduct_Category ON gold.DimProduct (CategoryName);
GO

-- ============================================================
-- STEP 3: Create DimCustomer (Denormalized with Geographic Hierarchy)
-- ============================================================
PRINT '========================================';
PRINT 'STEP 3: Creating DimCustomer (Denormalized)';
PRINT '========================================';

CREATE TABLE gold.DimCustomer (
    CustomerKey INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
    CustomerID INT NOT NULL,
    FullName NVARCHAR(200) NULL,
    AccountNumber VARCHAR(10) NOT NULL,
    CustomerType VARCHAR(20) NOT NULL,
    City NVARCHAR(30) NULL,
    StateProvinceName NVARCHAR(50) NULL,
    CountryRegionName NVARCHAR(50) NULL,
    TerritoryID INT NULL,
    TerritoryName NVARCHAR(50) NULL,
    IsCurrent BIT NOT NULL DEFAULT 1,
    ValidFrom DATETIME2 NOT NULL,
    ValidTo DATETIME2 NOT NULL,
    LoadDateTime DATETIME2 NOT NULL DEFAULT GETDATE()
)
WITH (
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);
GO

CREATE INDEX IX_DimCustomer_CustomerID ON gold.DimCustomer (CustomerID);
CREATE INDEX IX_DimCustomer_Territory ON gold.DimCustomer (TerritoryName);
GO

-- ============================================================
-- STEP 4: Create DimDate (Star Schema Date Dimension)
-- ============================================================
PRINT '========================================';
PRINT 'STEP 4: Creating DimDate (Star Schema)';
PRINT '========================================';

CREATE TABLE gold.DimDate (
    DateKey INT NOT NULL PRIMARY KEY,
    Date DATE NOT NULL,
    Year INT NOT NULL,
    Quarter INT NOT NULL,
    QuarterName VARCHAR(10) NOT NULL,
    Month INT NOT NULL,
    MonthName VARCHAR(20) NOT NULL,
    DayOfWeek INT NOT NULL,
    DayName VARCHAR(20) NOT NULL,
    IsWeekend BIT NOT NULL,
    IsHoliday BIT NOT NULL
)
WITH (
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- Populate DimDate from Silver
INSERT INTO gold.DimDate
SELECT * FROM silver.DimDate;
GO

-- ============================================================
-- STEP 5: Create DimTerritory (Star Schema Territory)
-- ============================================================
PRINT '========================================';
PRINT 'STEP 5: Creating DimTerritory';
PRINT '========================================';

CREATE TABLE gold.DimTerritory (
    TerritoryKey INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
    TerritoryID INT NOT NULL,
    TerritoryName NVARCHAR(50) NOT NULL,
    CountryRegionCode NVARCHAR(3) NOT NULL,
    GroupName NVARCHAR(50) NOT NULL,
    LoadDateTime DATETIME2 NOT NULL DEFAULT GETDATE()
)
WITH (
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- ============================================================
-- STEP 6: Create FactSales (Star Schema Fact Table)
-- ============================================================
PRINT '========================================';
PRINT 'STEP 6: Creating FactSales (Star Schema)';
PRINT '========================================';

CREATE TABLE gold.FactSales (
    FactKey BIGINT NOT NULL IDENTITY(1,1) PRIMARY KEY,
    SalesOrderID INT NOT NULL,
    SalesOrderDetailID INT NOT NULL,
    
    -- Dimension Foreign Keys
    ProductKey INT NOT NULL,
    CustomerKey INT NOT NULL,
    OrderDateKey INT NOT NULL,
    ShipDateKey INT NULL,
    TerritoryKey INT NULL,
    
    -- Measures
    OrderQty INT NOT NULL,
    UnitPrice DECIMAL(19,4) NOT NULL,
    UnitPriceDiscount DECIMAL(19,4) NOT NULL,
    LineTotal DECIMAL(19,4) NOT NULL,
    
    -- Audit
    LoadDateTime DATETIME2 NOT NULL DEFAULT GETDATE()
)
WITH (
    DISTRIBUTION = HASH([ProductKey]),
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- Create foreign key relationships (logical, not enforced in Synapse)
CREATE INDEX IX_FactSales_ProductKey ON gold.FactSales (ProductKey);
CREATE INDEX IX_FactSales_CustomerKey ON gold.FactSales (CustomerKey);
CREATE INDEX IX_FactSales_OrderDateKey ON gold.FactSales (OrderDateKey);
CREATE INDEX IX_FactSales_TerritoryKey ON gold.FactSales (TerritoryKey);
GO

-- ============================================================
-- STEP 7: Create Stored Procedure to Populate Gold Layer
-- ============================================================
PRINT '========================================';
PRINT 'STEP 7: Creating Populate Gold Layer Procedure';
PRINT '========================================';

CREATE OR ALTER PROCEDURE gold.usp_Populate_Gold_Layer
    @BatchID INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartTime DATETIME2 = GETDATE();
    
    BEGIN TRY
        
        -- Populate DimProduct
        TRUNCATE TABLE gold.DimProduct;
        
        INSERT INTO gold.DimProduct (
            ProductID, ProductName, ProductNumber, Color,
            StandardCost, ListPrice, SubcategoryName, CategoryName,
            ProductCategoryID, ProductSubcategoryID, IsCurrent,
            ValidFrom, ValidTo
        )
        SELECT 
            p.ProductID,
            p.Name,
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
            p.ValidTo
        FROM silver.DimProduct p
        LEFT JOIN Production.ProductSubcategory ps ON p.ProductSubcategoryID = ps.ProductSubcategoryID
        LEFT JOIN Production.ProductCategory pc ON ps.ProductCategoryID = pc.ProductCategoryID;
        
        -- Populate DimCustomer
        TRUNCATE TABLE gold.DimCustomer;
        
        INSERT INTO gold.DimCustomer (
            CustomerID, FullName, AccountNumber, CustomerType,
            City, StateProvinceName, CountryRegionName,
            TerritoryID, TerritoryName, IsCurrent, ValidFrom, ValidTo
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
            c.ValidTo
        FROM silver.DimCustomer c
        LEFT JOIN Person.Person p ON c.PersonID = p.BusinessEntityID
        LEFT JOIN Sales.CustomerAddress ca ON c.CustomerID = ca.CustomerID
        LEFT JOIN Person.Address a ON ca.AddressID = a.AddressID
        LEFT JOIN Person.StateProvince sp ON a.StateProvinceID = sp.StateProvinceID
        LEFT JOIN Person.CountryRegion cr ON sp.CountryRegionCode = cr.CountryRegionCode
        LEFT JOIN Sales.SalesTerritory t ON c.TerritoryID = t.TerritoryID
        WHERE ca.AddressTypeID = 2;  -- Main office address
        
        -- Log success
        EXEC control.usp_LogAudit
            @BatchID = @BatchID,
            @ProcessName = 'gold.usp_Populate_Gold_Layer',
            @TableName = 'gold.DimProduct, gold.DimCustomer, gold.FactSales',
            @StartTime = @StartTime,
            @Status = 'SUCCESS';
            
    END TRY
    BEGIN CATCH
        EXEC control.usp_LogError
            @BatchID = @BatchID,
            @ProcessName = 'gold.usp_Populate_Gold_Layer',
            @ErrorMessage = ERROR_MESSAGE();
        
        THROW;
    END CATCH
END;
GO

-- ============================================================
-- SUMMARY
-- ============================================================
PRINT '========================================';
PRINT 'GOLD LAYER TABLES CREATION COMPLETE';
PRINT '========================================';
PRINT 'Created Tables:';
PRINT '- gold.DimProduct (Denormalized)';
PRINT '- gold.DimCustomer (Denormalized)';
PRINT '- gold.DimDate (Star Schema)';
PRINT '- gold.DimTerritory (Star Schema)';
PRINT '- gold.FactSales (Star Schema)';
PRINT '';
PRINT 'Created Procedures:';
PRINT '- gold.usp_Populate_Gold_Layer';
PRINT '';
PRINT 'Next Steps:';
PRINT '1. Run 05_create_staging_tables.sql';
PRINT '2. Deploy stored procedures from /stored-procedures folder';
PRINT '========================================';
GO