# wide-world-importers-elt-pipeline

End-to-end ELT pipeline on Microsoft's Wide World Importers OLTP database.

SQL Server (Docker) -> PySpark incremental extract -> Parquet bronze -> Postgres -> dbt -> Power BI, orchestrated by Airflow.

Work in progress (Oct-Nov 2026).
