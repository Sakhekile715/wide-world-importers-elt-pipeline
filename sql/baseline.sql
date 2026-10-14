/*

CREATED BY: Sakhekile Mtyingizane
DATE CREATED: 2026-10-14
OBJECTIVE:
    This script generates a baseline report for the WideWorldImporters database.
    It counts the number of rows in key tables and retrieves the minimum and maximum dates
    for relevant date columns, as well as the latest last edited timestamp.
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
)
SELECT
    table_name,
    row_count,
    min_date,
    max_date,
    max_last_edited
FROM baseline
ORDER BY table_name;