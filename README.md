# wide-world-importers-elt-pipeline

End-to-end ELT pipeline on Microsoft's Wide World Importers OLTP database.

SQL Server (Docker) -> PySpark incremental extract -> Parquet landing (MinIO) -> Postgres bronze -> dbt -> Power BI, orchestrated by Airflow.

Work in progress (Oct-Nov 2026). Built so far: source restore, landing, bronze.

## Run it from a fresh clone

### 1. Prerequisites

| Tool | Version used | Notes |
|---|---|---|
| Docker Desktop | any recent | runs SQL Server 2022 and MinIO |
| Python | 3.11 | |
| Java (JDK) | 17 | must be on `PATH` (or `JAVA_HOME` set) for PySpark |
| PostgreSQL | 18 | local install, listening on 5432 (not in docker-compose) |

On Windows you do **not** need `winutils.exe` or `HADOOP_HOME`. The "Did not find winutils.exe" warning Spark prints at startup is harmless.

### 2. Configure

```sh
cp .env.example .env
```

Fill in every value. Typical local values:

| Setting | Value |
|---|---|
| `MSSQL_SERVER` / `MSSQL_PORT` / `MSSQL_DATABASE` / `MSSQL_USER` | `localhost` / `1433` / `WideWorldImporters` / `sa` |
| `MSSQL_SA_PASSWORD` | a strong password (SQL Server rejects weak ones) |
| `POSTGRES_HOST` / `POSTGRES_PORT` | `localhost` / `5432` |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | the role and database created in step 5 |
| `MINIO_ENDPOINT` | `http://localhost:9000` |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | MinIO admin login (password at least 8 characters) |
| `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | leave empty to use the root login |
| `MINIO_BUCKET` | `landing` |

### 3. Start SQL Server and MinIO, and restore the source

```sh
docker compose up -d
```

Download the sample database backup (about 120 MB, kept out of git) into `backups/`:

```sh
curl -L -o backups/WideWorldImporters-Full.bak \
  https://github.com/Microsoft/sql-server-samples/releases/download/wide-world-importers-v1.0/WideWorldImporters-Full.bak
```

Restore it (`./backups` is mounted at `/var/opt/mssql/backup` in the container). On Git Bash, prefix the command with `MSYS_NO_PATHCONV=1`:

```sh
docker exec wwi-sqlserver bash -c '/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -i /var/opt/mssql/backup/restore_wwi.sql'
```

The restored database should match [docs/baseline.csv](docs/baseline.csv): 27 tables, data up to 2016-05-31. Re-running the restore resets the source to this baseline, including removing any days added by `sql/simulate_one_day.sql`.

### 4. Create the landing bucket

Open the MinIO console at <http://localhost:9001>, log in with `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`, and create a bucket named `landing` (or whatever `MINIO_BUCKET` says).

### 5. Create the Postgres role and database

As a Postgres superuser, using the values from `.env`:

```sh
psql -U postgres -h localhost -v user=Pipeline_User -v password='...' \
     -v db=world_wide_importers_dw -f sql/postgres/setup_database.sql
```

The scripts create the `bronze`, `bronze_staging` and `control` schemas and their tables themselves.

### 6. Python environment and Spark jars

```sh
python -m venv .venv
.venv/Scripts/python -m pip install -r requirements.txt   # Windows
# .venv/bin/python -m pip install -r requirements.txt     # macOS / Linux

.venv/Scripts/python scripts/fetch_jars.py
```

`fetch_jars.py` downloads the JDBC drivers and the S3A connector pinned in [jars/jars.lock](jars/jars.lock) into `jars/` and checks each SHA-1. It's about 700 MB, mostly the AWS SDK bundle. Spark loads only these jars; nothing is resolved at runtime.

### 7. Run the pipeline

```sh
# SQL Server -> MinIO landing (Parquet), incremental by watermark
.venv/Scripts/python Notebooks/source_to_landing.py

# MinIO landing -> Postgres bronze
.venv/Scripts/python Notebooks/landing_to_bronze.py
```

Expected on a fresh run: `Summary: 27 landed` from landing, then `Summary: 27 batches loaded` from bronze. Running either script again without new source data is a no-op (`no new rows` and `up to date`).

Check the result in Postgres:

```sql
SELECT table_name, last_batch_id, status FROM control.watermark ORDER BY 1;   -- 27 rows, all success
SELECT count(*) FROM bronze.sales_orders;                                      -- 73595
```

## Layout

| Path | What |
|---|---|
| `Notebooks/source_to_landing.py` | SQL Server -> MinIO, one batch folder per run, checked against the source |
| `Notebooks/landing_to_bronze.py` | MinIO -> Postgres: staging table, then a one-transaction swap into `bronze.<table>` |
| `Notebooks/generate_bronze_ddl.py` | regenerates `sql/postgres/bronze_tables.sql` from the SQL Server catalog (run only when the source schema changes) |
| `sql/postgres/` | Postgres DDL: role/database setup, bronze tables (explicit types), control tables |
| `sql/simulate_one_day.sql` | advances the source by one business day (for incremental testing) |
| `backups/` | restore scripts; the `.bak` itself is not in git |
| `jars/jars.lock` | pinned Spark JVM dependencies |
| `docs/adr/` | architecture decisions |

## Notes

- **Timestamps are microseconds.** SQL Server `datetime2(7)` keeps 100 ns, but Spark and Postgres stop at microseconds, so the 7th digit is dropped. Only `Application.Countries` shows it (`.8930093` becomes `.893009`). Watermarks keep all 7 digits as text. See the comment in `generate_bronze_ddl.py`.
- **Time zones.** Spark's JVM and session are pinned to UTC, and bronze converts staged `timestamptz` at UTC explicitly. Without both, every timestamp shifts by the machine's UTC offset.
- **SCD2 history** for customers, stock items and suppliers comes from the temporal `_Archive` tables, which are extracted alongside the current tables. See [docs/adr/0002-scd2-history-from-temporal-archives.md](docs/adr/0002-scd2-history-from-temporal-archives.md).
- **Two watermark stores** (landing in MinIO, bronze in `control.watermark`) are deliberate. See [docs/adr/0001-separate-watermark-stores.md](docs/adr/0001-separate-watermark-stores.md).
