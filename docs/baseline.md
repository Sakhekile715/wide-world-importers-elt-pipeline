# Source baseline: WideWorldImporters

Captured 14 Oct 2026 after restoring `WideWorldImporters-Full.bak` (v1.0)
into SQL Server 2022 CU27 (Docker).

- Query: [`sql/baseline.sql`](../sql/baseline.sql)
- Results: [`baseline.csv`](baseline.csv)

`max_last_edited` is the starting watermark for incremental extraction.
Row counts are the reconciliation target for the first full load.