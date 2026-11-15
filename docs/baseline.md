# Source baseline: WideWorldImporters

Captured 15 Nov 2026 after restoring `WideWorldImporters-Full.bak` (v1.0)
into SQL Server 2022 (Docker, `mcr.microsoft.com/mssql/server:2022-latest`).
Supersedes the 13-table capture of 14 Oct 2026; the 13 original rows are
unchanged, confirming the restore returned the source to its pre-simulation
state.

- Query: [`sql/baseline.sql`](../sql/baseline.sql)
- Results: [`baseline.csv`](baseline.csv)

`max_last_edited` is the starting watermark for incremental extraction.
Row counts are the reconciliation target for the first full load.

## Scope

27 tables, covering every Business Requirement except **BR-06 Cold-chain
compliance**, which was dropped from scope on 15 Nov 2026. The last three
(the `_Archive` history tables) were added the same day for DR-02; see
[ADR 0002](adr/0002-scd2-history-from-temporal-archives.md). The temperature
tables (`Warehouse.ColdRoomTemperatures`, its archive, and
`Warehouse.VehicleTemperatures`) are therefore not extracted.

The watermark column follows the table's temporal type: system-versioned
tables use `ValidFrom`, non-temporal tables use `LastEditedWhen`. Confirmed
against `sys.tables.temporal_type_desc` on 15 Nov 2026.

| Table | Watermark | Needed for |
|---|---|---|
| Application.Cities | ValidFrom | BR-01 |
| Application.Countries | ValidFrom | BR-01 |
| Application.DeliveryMethods | ValidFrom | BR-02 |
| Application.People | ValidFrom | BR-01 |
| Application.StateProvinces | ValidFrom | BR-01 |
| Application.TransactionTypes | ValidFrom | BR-03 |
| Purchasing.PurchaseOrderLines | LastEditedWhen | BR-04 |
| Purchasing.PurchaseOrders | LastEditedWhen | BR-04 |
| Purchasing.SupplierCategories | ValidFrom | BR-04 |
| Purchasing.Suppliers | ValidFrom | BR-04, DR-02 |
| Purchasing.SupplierTransactions | LastEditedWhen | BR-04 |
| Sales.BuyingGroups | ValidFrom | BR-01 |
| Sales.CustomerCategories | ValidFrom | BR-01 |
| Sales.Customers | ValidFrom | BR-01, BR-05, DR-02 |
| Sales.CustomerTransactions | LastEditedWhen | BR-05 |
| Sales.InvoiceLines | LastEditedWhen | BR-01 |
| Sales.Invoices | LastEditedWhen | BR-01, BR-02 |
| Sales.OrderLines | LastEditedWhen | BR-02 |
| Sales.Orders | LastEditedWhen | BR-02 |
| Warehouse.StockGroups | ValidFrom | BR-01 |
| Warehouse.StockItemHoldings | LastEditedWhen | BR-03 |
| Warehouse.StockItems | ValidFrom | BR-01, BR-03, DR-02 |
| Warehouse.StockItemStockGroups | LastEditedWhen | BR-01 |
| Warehouse.StockItemTransactions | LastEditedWhen | BR-03 |
| Purchasing.Suppliers_Archive | ValidTo | DR-02 |
| Sales.Customers_Archive | ValidTo | DR-02 |
| Warehouse.StockItems_Archive | ValidTo | DR-02 |

The `_Archive` tables hold the earlier versions of rows in the three SCD2
dimensions; a plain `SELECT` on a temporal table returns only current rows.
They use `ValidTo` as the watermark because an archived version keeps its
original `ValidFrom`: when a row changes at time T, its old version arrives in
the archive with `ValidTo = T`, so only `ValidTo` moves forward.

`Warehouse.StockItemStockGroups` is the bridge between stock items and stock
groups. BR-01 asks to slice revenue by stock group, and the relationship is
many-to-many, so without the bridge there is no join path from an invoice
line to a stock group.

## Note on Application.Countries

Its `max_last_edited` is 2022-10-07, six years after every other table's
watermark, which all sit in the 2013-2016 range of the sample data. This is
how the shipped backup is, not a load error. It only affects where that
table's watermark starts, and the simulator does not touch Countries.
