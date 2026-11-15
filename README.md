# wide-world-importers-elt-pipeline

End-to-end ELT pipeline on Microsoft's Wide World Importers OLTP database.

SQL Server (Docker) -> PySpark incremental extract -> Parquet landing (MinIO) -> Postgres bronze -> dbt -> Power BI, orchestrated by Airflow.

Work in progress (Oct-Nov 2026). Built so far: source restore, landing, bronze, dbt project setup.

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

- **Windows:** the Postgres installer doesn't put `psql` on `PATH`. Call it by its full path from PowerShell, for example `& 'C:\Program Files\PostgreSQL\18\bin\psql.exe' -U postgres ...`, keeping the rest of the command the same.
- **The password prompt shows nothing as you type.** That's normal: type the `postgres` password and press Enter. If the prompt won't take input, set it for the current PowerShell session first with `$env:PGPASSWORD = Read-Host 'postgres password' -MaskInput` (PowerShell 7), and run `Remove-Item Env:PGPASSWORD` afterwards.
- On success psql prints only `CREATE ROLE` (on a first run), `CREATE DATABASE` and `GRANT`.

The scripts create the `bronze`, `bronze_staging` and `control` schemas and their tables themselves.

### 6. Python environment and Spark jars

Create and activate a virtual environment. Every later command assumes it is active.

```sh
python -m venv .venv

source .venv/bin/activate        # macOS / Linux
source .venv/Scripts/activate    # Windows, Git Bash
.venv\Scripts\Activate.ps1       # Windows, PowerShell

python -m pip install -r requirements.txt
python scripts/fetch_jars.py
```

`fetch_jars.py` downloads the JDBC drivers and the S3A connector pinned in [jars/jars.lock](jars/jars.lock) into `jars/` and checks each SHA-1. It's about 700 MB, mostly the AWS SDK bundle. Spark loads only these jars; nothing is resolved at runtime.

### 7. Run the pipeline

```sh
# SQL Server -> MinIO landing (Parquet), incremental by watermark
python Notebooks/source_to_landing.py

# MinIO landing -> Postgres bronze
python Notebooks/landing_to_bronze.py
```

Expected on a fresh run: `Summary: 27 landed` from landing, then `Summary: 27 batches loaded` from bronze. Running either script again without new source data is a no-op (`no new rows` and `up to date`).

Check the result in Postgres:

```sql
SELECT table_name, last_batch_id, status FROM control.watermark ORDER BY 1;   -- 27 rows, all success
SELECT count(*) FROM bronze.sales_orders;                                      -- 73595
```

`73595` is the count straight after the step 3 restore. If the source has been advanced with `sql/simulate_one_day.sql`, compare against `SELECT COUNT(*) FROM Sales.Orders` in SQL Server instead.

### 8. Check dbt

dbt reads its connection from `.env` through a small wrapper, so the same command works in PowerShell, Git Bash and on macOS/Linux. Run dbt through it rather than calling `dbt` directly:

```sh
python scripts/run_dbt.py debug                      # expect: All checks passed!
python scripts/run_dbt.py ls --resource-type source  # expect: 27 bronze sources
```

The project lives in `dbt/`, with its connection profile in `dbt/profiles.yml` (no secrets; it reads `POSTGRES_*` from the environment). Models will build into the `silver` and `gold` schemas.

## Layout

| Path | What |
|---|---|
| `Notebooks/source_to_landing.py` | SQL Server -> MinIO, one batch folder per run, checked against the source |
| `Notebooks/landing_to_bronze.py` | MinIO -> Postgres: staging table, then a one-transaction swap into `bronze.<table>` |
| `Notebooks/generate_bronze_ddl.py` | regenerates `sql/postgres/bronze_tables.sql` from the SQL Server catalog (run only when the source schema changes) |
| `sql/postgres/` | Postgres DDL: role/database setup, bronze tables (explicit types), control tables |
| `sql/simulate_one_day.sql` | advances the source by one business day (for incremental testing) |
| `backups/` | restore scripts; the `.bak` itself is not in git |
| `dbt/` | dbt project: bronze declared as sources (`models/sources.yml`), silver/gold models to come |
| `scripts/run_dbt.py` | runs dbt with `.env` loaded and `dbt/` as project and profile dir |
| `jars/jars.lock` | pinned Spark JVM dependencies |
| `docs/adr/` | architecture decisions |

## Notes

- **Timestamps are microseconds.** SQL Server `datetime2(7)` keeps 100 ns, but Spark and Postgres stop at microseconds, so the 7th digit is dropped. Only `Application.Countries` shows it (`.8930093` becomes `.893009`). Watermarks keep all 7 digits as text. See the comment in `generate_bronze_ddl.py`.
- **Time zones.** Spark's JVM and session are pinned to UTC, and bronze converts staged `timestamptz` at UTC explicitly. Without both, every timestamp shifts by the machine's UTC offset.
- **SCD2 history** for customers, stock items and suppliers comes from the temporal `_Archive` tables, which are extracted alongside the current tables. See [docs/adr/0002-scd2-history-from-temporal-archives.md](docs/adr/0002-scd2-history-from-temporal-archives.md).
- **Two watermark stores** (landing in MinIO, bronze in `control.watermark`) are deliberate. See [docs/adr/0001-separate-watermark-stores.md](docs/adr/0001-separate-watermark-stores.md).

## Known limitations

Accepted for a local, single-machine portfolio pipeline. Revisit before running it anywhere shared.

- **One runner at a time.** Landing rewrites a single `watermarks.json`, and bronze reuses one staging table per table. Overlapping runs aren't supported. Bronze refuses a staging table that holds the wrong batch, and Airflow should run the DAG with `max_active_runs=1`.
- **Late-committing rows.** Extraction trusts that the source never commits a row whose watermark is at or below a high watermark already read. That holds for the simulator, but not for long-running transactions on a live OLTP system (a lookback window or change tracking would fix it).
- **Hard deletes aren't captured.** Watermark extraction only sees inserts and updates. WWI's simulator doesn't hard-delete; a real source would need tombstones or CDC.
- **Bronze is append-only history.** An updated row appears once per batch that landed it, including the older "current" snapshots of temporal dimensions. Silver takes the latest version per key (by `_batch_id`) before it uses a table. See ADR 0002.
- **Schema drift stops the load.** If the source schema changes, bronze rejects the batch instead of altering tables. Regenerate `bronze_tables.sql` and migrate the affected table by hand.
- **Local-only security.** Ports bind to `127.0.0.1`, connections use `sa`, the MinIO root login, plain HTTP and `trustServerCertificate=true`, and anyone who can write to the landing bucket is trusted. Use least-privilege accounts and TLS for anything beyond a laptop.
