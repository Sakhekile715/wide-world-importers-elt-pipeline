# ADR 0002: Backfill SCD2 history from the temporal archive tables

- **Status:** Accepted
- **Date:** 2026-11-15
- **Deciders:** project owner

## Context

DR-02 (Must): *Customers, stock items and suppliers keep full history with valid-from and valid-to dates. Facts join to the version that was valid on the transaction date.*

`Sales.Customers`, `Warehouse.StockItems` and `Purchasing.Suppliers` are system-versioned temporal tables. A plain `SELECT` returns current rows only; every earlier version sits in the history table (`<table>_Archive`). The pipeline extracted only the current tables, so bronze held one version per entity (663 customers, 663 rows). Building SCD2 from that would give a history that starts at the first pipeline load. Every 2013–2016 invoice would then join to the version captured at load time, which fails DR-02's acceptance criterion.

The options were:

1. **Forward-only:** SCD2 accumulates from the first load. This is normal in real pipelines, but DR-02 would need rewording, and historic facts would never meet it.
2. **Backfill:** extract the three `_Archive` tables too, and build SCD2 from archive + current versions.

## Decision

**Backfill.** Add `Sales.Customers_Archive`, `Warehouse.StockItems_Archive` and `Purchasing.Suppliers_Archive` to the extraction scope (now 27 tables) and land them unchanged into `bronze.<table>_archive`. Silver will build each SCD2 dimension as the union of current and archive rows, using the source's own `ValidFrom` / `ValidTo` as the validity range.

The archive tables use **`ValidTo` as the watermark**, not `ValidFrom`. When a row changes at time T, its old version moves into the archive with `ValidTo = T` but keeps its original, older `ValidFrom`. Only `ValidTo` moves forward, so a `ValidFrom` watermark would skip every newly archived version.

The other 14 temporal tables in scope (Cities, People, StockGroups and so on) also have archives. They are not extracted, because DR-02 asks for history only on these three dimensions.

## What the archives actually contain

Profiled on the restored baseline (15 Nov 2026):

| Dimension | Versions (current + archive) | Entities | What changes between versions |
|---|---|---|---|
| Customers | 714 | 663 | `CreditLimit` for 46 customers, yearly between 2013 and 2015. **Real business history.** |
| Stock items | 671 | 227 | `CustomFields` and `Tags` (JSON) for 172 items. All changes fall within 12 minutes on 2016-05-31 23:00, a post-processing step in Microsoft's sample-data generation. Prices and other attributes never change. |
| Suppliers | 26 | 13 | Nothing. The 13 archive rows are identical to the current rows, valid from 2013-01-01 00:00 to 00:05, an artifact of the initial load. |

Integrity checks across current + archive, on both the source and bronze:

- No gaps or overlaps: each version's `ValidTo` equals the next version's `ValidFrom`.
- Exactly one open version per entity (`ValidTo = 9999-12-31 23:59:59.9999999`).
- History starts at 2013-01-01 for every entity, so every fact date has a matching version.
- **One zero-length version.** Customer 890 has an archived version with `ValidFrom = ValidTo = 2014-07-01 16:00`: two updates happened at the same instant. That version was never in effect, and SQL Server's own `FOR SYSTEM_TIME AS OF` never returns it.

## Consequences

- DR-02's acceptance criterion can be met for all historic facts. Every 2013–2016 transaction joins to a version that was valid on its date.
- The honest description is narrower than "full history for three dimensions": customers have real history, while stock items and suppliers have technically correct but nearly empty history. Portfolio write-ups and the DR-02 test should say so, rather than imply supplier attributes changed.
- Rules silver must follow:
  0. **Take only the latest snapshot of each current row first.** Bronze is append-only, so when a customer changes, `bronze.sales_customers` still holds the earlier batch's copy with `valid_to = 9999-12-31`, which now overlaps the archived version. Deduplicate the current table to one row per key (latest `_batch_id`) *before* unioning it with the archive. The archive tables never need this, because an archived version is final.
  1. **Drop zero-length versions** (`valid_from >= valid_to`) before building the dimension.
  2. **Use a half-open join:** `valid_from <= event_time < valid_to`. With a closed interval, an event exactly at a change instant matches two versions.
  3. **Collapse no-op versions.** Merge consecutive versions whose tracked attributes are identical, ignoring `LastEditedBy` and the period columns. This removes the supplier artifacts and the stock item versions where nothing a report uses changed. Decide which attributes are "tracked" per dimension (for example, collapse stock items on everything except `CustomFields`/`Tags`, if those aren't reported on).
  4. **Choose the right join column.** Fact dates are `date` while version boundaries are `datetime2`. Join on the end of the transaction day, or on a real event timestamp where one exists, and document the choice. Changes in this data happen at 16:00 or 23:xx, so for most rows the date-level join gives the same answer either way, but not on change days.
- Incremental behaviour: the simulator's changes produce new archive rows (old version, new `ValidTo`) plus updated current rows (new `ValidFrom`), and each table's own watermark picks them up independently. Like the current tables, this relies on the source not committing rows with a period timestamp older than the high watermark already read. That holds for the simulator, but would not hold for long-running transactions on a live system.

## Alternatives considered

- **Forward-only SCD2:** rejected because it can't satisfy DR-02 for 2013–2016 facts.
- **Extract with `FOR SYSTEM_TIME ALL` from the current table** instead of reading the archive separately: one query, but the watermark logic gets muddled. The current rows need `ValidFrom` and the archived rows need `ValidTo`, and filtering both through one query makes the window predicate a disjunction that's hard to reason about. Two tables, each with its own watermark, reuse the existing landing code unchanged.
