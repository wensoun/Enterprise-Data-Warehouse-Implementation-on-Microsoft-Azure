-- ============================================================
-- Script: 03_create_silver_tables.sql
-- Purpose: Create Silver layer (Inmon normalized warehouse) tables
-- ============================================================

-- Step 1: Create silver schema
CREATE SCHEMA silver;
GO

-- Step 2: DimProduct (SCD Type 2 with history tracking)
CREATE TABLE silver.DimProduct (
    ProductID INT NOT NULL,
    Name NVARCHAR(50) NOT NULL,
    ProductNumber NVARCHAR(25) NOT NULL,
    Color NVARCHAR(15) NULL,
    StandardCost MONEY NOT NULL,
    ListPrice MONEY NOT NULL,
    ProductSubcategoryID INT NULL,
    ProductModelID INT NULL,
    SellStartDate DATE NOT NULL,
    SellEndDate DATE NULL,
    DiscontinuedDate DATE NULL,
    
    -- SCD Type 2 columns
    ValidFrom DATETIME2(2) NOT NULL,
    ValidTo DATETIME2(2) NOT NULL,
    IsCurrent BIT NOT NULL,
    
    -- Audit columns
    LoadLSN BINARY(10) NOT NULL,
    LoadDateTime DATETIME2 NOT NULL DEFAULT GETDATE(),
    BatchID INT
)
WITH (
    DISTRIBUTION = HASH([ProductID]),
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- Step 3: DimCustomer (SCD Type 2)
CREATE TABLE silver.DimCustomer (
    CustomerID INT NOT NULL,
    PersonID INT NULL,
    StoreID INT NULL,
    TerritoryID INT NULL,
    AccountNumber VARCHAR(10) NOT NULL,
    CustomerType CHAR(1) NOT NULL,
    
    -- SCD Type 2 columns
    ValidFrom DATETIME2(2) NOT NULL,
    ValidTo DATETIME2(2) NOT NULL,
    IsCurrent BIT NOT NULL,
    
    -- Audit columns
    LoadLSN BINARY(10) NOT NULL,
    LoadDateTime DATETIME2 NOT NULL DEFAULT GETDATE(),
    BatchID INT
)
WITH (
    DISTRIBUTION = HASH([CustomerID]),
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- Step 4: DimDate (Static, pre-populated)
CREATE TABLE silver.DimDate (
    DateKey INT NOT NULL,
    Date DATE NOT NULL,
    Year INT NOT NULL,
    Quarter INT NOT NULL,
    Month INT NOT NULL,
    MonthName VARCHAR(20) NOT NULL,
    DayOfWeek INT NOT NULL,
    DayName VARCHAR(20) NOT NULL,
    IsWeekend BIT NOT NULL,
    IsHoliday BIT NOT NULL DEFAULT 0
)
WITH (
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- Step 5: FactSalesOrder (Append-only)
CREATE TABLE silver.FactSalesOrder (
    SalesOrderID INT NOT NULL,
    SalesOrderDetailID INT NOT NULL,
    CustomerID INT NOT NULL,
    ProductID INT NOT NULL,
    OrderDate DATE NOT NULL,
    ShipDate DATE NULL,
    DueDate DATE NOT NULL,
    OrderQty SMALLINT NOT NULL,
    UnitPrice MONEY NOT NULL,
    UnitPriceDiscount MONEY NOT NULL,
    LineTotal MONEY NOT NULL,
    TerritoryID INT NULL,
    
    -- Audit columns
    LoadLSN BINARY(10) NOT NULL,
    LoadDateTime DATETIME2 NOT NULL DEFAULT GETDATE(),
    BatchID INT
)
WITH (
    DISTRIBUTION = HASH([ProductID]),
    PARTITION ([OrderDate] RANGE RIGHT FOR VALUES ('2022-01-01', '2023-01-01', '2024-01-01')),
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- Step 6: Create indexes for performance
CREATE INDEX IX_DimProduct_IsCurrent ON silver.DimProduct (IsCurrent);
CREATE INDEX IX_DimProduct_ValidFrom_ValidTo ON silver.DimProduct (ValidFrom, ValidTo);
CREATE INDEX IX_DimCustomer_IsCurrent ON silver.DimCustomer (IsCurrent);
CREATE INDEX IX_FactSalesOrder_OrderDate ON silver.FactSalesOrder (OrderDate);
GO

PRINT 'Silver layer tables created successfully';