/*

CREATED BY: Sakhekile Mtyingizane
DATE CREATED: 2026-10-14
LAST UPDATED: 2026-11-15 -- scope extended from 13 to 24 tables, then to 27
              (history tables of Customers, StockItems, Suppliers for DR-02)
OBJECTIVE:
    This script generates a baseline report for the WideWorldImporters database.
    It counts the number of rows in key tables and retrieves the minimum and maximum dates
    for relevant date columns, as well as the latest last edited timestamp.

SCOPE:
    27 tables covering every Business Requirement except BR-06 (cold chain),
    which is out of scope. System-versioned (temporal) tables use ValidFrom as
    the watermark; non-temporal tables use LastEditedWhen; the three history
    (_Archive) tables use ValidTo.

NOTE:
    Run this against a freshly restored source. Running it after the simulator
    has executed produces a moved baseline, not a starting one.
*/


USE WideWorldImporters;

WITH baseline AS (
    SELECT 'Sales.Orders' AS table_name, COUNT_BIG(*) AS row_count,
           MIN(OrderDate) AS min_date, MAX(OrderDate) AS max_date,
           MAX(LastEditedWhen) AS max_last_edited
    FROM Sales.Orders
    UNION ALL
    SELECT 'Sales.OrderLines', COUNT_BIG(*), NULL, NULL, MAX(LastEditedWhen)
    FROM Sales.OrderLines
    UNION ALL
    SELECT 'Sales.Invoices', COUNT_BIG(*), MIN(InvoiceDate), MAX(InvoiceDate), MAX(LastEditedWhen)
    FROM Sales.Invoices
    UNION ALL
    SELECT 'Sales.InvoiceLines', COUNT_BIG(*), NULL, NULL, MAX(LastEditedWhen)
    FROM Sales.InvoiceLines
    UNION ALL
    SELECT 'Sales.CustomerTransactions', COUNT_BIG(*), MIN(TransactionDate), MAX(TransactionDate), MAX(LastEditedWhen)
    FROM Sales.CustomerTransactions
    UNION ALL
    SELECT 'Purchasing.PurchaseOrders', COUNT_BIG(*), MIN(OrderDate), MAX(OrderDate), MAX(LastEditedWhen)
    FROM Purchasing.PurchaseOrders
    UNION ALL
    SELECT 'Purchasing.PurchaseOrderLines', COUNT_BIG(*), NULL, NULL, MAX(LastEditedWhen)
    FROM Purchasing.PurchaseOrderLines
    UNION ALL
    SELECT 'Purchasing.SupplierTransactions', COUNT_BIG(*), MIN(TransactionDate), MAX(TransactionDate), MAX(LastEditedWhen)
    FROM Purchasing.SupplierTransactions
    UNION ALL
    SELECT 'Warehouse.StockItemTransactions', COUNT_BIG(*),
           CAST(MIN(TransactionOccurredWhen) AS DATE), CAST(MAX(TransactionOccurredWhen) AS DATE),
           MAX(LastEditedWhen)
    FROM Warehouse.StockItemTransactions
    UNION ALL
    SELECT 'Sales.Customers', COUNT_BIG(*), NULL, NULL, MAX(ValidFrom)
    FROM Sales.Customers
    UNION ALL
    SELECT 'Purchasing.Suppliers', COUNT_BIG(*), NULL, NULL, MAX(ValidFrom)
    FROM Purchasing.Suppliers
    UNION ALL
    SELECT 'Warehouse.StockItems', COUNT_BIG(*), NULL, NULL, MAX(ValidFrom)
    FROM Warehouse.StockItems
    UNION ALL
    SELECT 'Application.People', COUNT_BIG(*), NULL, NULL, MAX(ValidFrom)
    FROM Application.People
    UNION ALL
    SELECT 'Application.Cities', COUNT_BIG(*), NULL, NULL, MAX(ValidFrom)
    FROM Application.Cities
    UNION ALL
    SELECT 'Application.StateProvinces', COUNT_BIG(*), NULL, NULL, MAX(ValidFrom)
    FROM Application.StateProvinces
    UNION ALL
    SELECT 'Application.Countries', COUNT_BIG(*), NULL, NULL, MAX(ValidFrom)
    FROM Application.Countries
    UNION ALL
    SELECT 'Application.DeliveryMethods', COUNT_BIG(*), NULL, NULL, MAX(ValidFrom)
    FROM Application.DeliveryMethods
    UNION ALL
    SELECT 'Application.TransactionTypes', COUNT_BIG(*), NULL, NULL, MAX(ValidFrom)
    FROM Application.TransactionTypes
    UNION ALL
    SELECT 'Sales.CustomerCategories', COUNT_BIG(*), NULL, NULL, MAX(ValidFrom)
    FROM Sales.CustomerCategories
    UNION ALL
    SELECT 'Sales.BuyingGroups', COUNT_BIG(*), NULL, NULL, MAX(ValidFrom)
    FROM Sales.BuyingGroups
    UNION ALL
    SELECT 'Purchasing.SupplierCategories', COUNT_BIG(*), NULL, NULL, MAX(ValidFrom)
    FROM Purchasing.SupplierCategories
    UNION ALL
    SELECT 'Warehouse.StockGroups', COUNT_BIG(*), NULL, NULL, MAX(ValidFrom)
    FROM Warehouse.StockGroups
    UNION ALL
    SELECT 'Warehouse.StockItemStockGroups', COUNT_BIG(*), NULL, NULL, MAX(LastEditedWhen)
    FROM Warehouse.StockItemStockGroups
    UNION ALL
    SELECT 'Warehouse.StockItemHoldings', COUNT_BIG(*), NULL, NULL, MAX(LastEditedWhen)
    FROM Warehouse.StockItemHoldings
    -- History tables of the SCD2 dimensions (DR-02). Watermark is ValidTo:
    -- an archived version keeps its old ValidFrom, ValidTo is when it ended.
    UNION ALL
    SELECT 'Sales.Customers_Archive', COUNT_BIG(*), NULL, NULL, MAX(ValidTo)
    FROM Sales.Customers_Archive
    UNION ALL
    SELECT 'Purchasing.Suppliers_Archive', COUNT_BIG(*), NULL, NULL, MAX(ValidTo)
    FROM Purchasing.Suppliers_Archive
    UNION ALL
    SELECT 'Warehouse.StockItems_Archive', COUNT_BIG(*), NULL, NULL, MAX(ValidTo)
    FROM Warehouse.StockItems_Archive
)
SELECT
    table_name,
    row_count,
    min_date,
    max_date,
    max_last_edited
FROM baseline
ORDER BY table_name;
