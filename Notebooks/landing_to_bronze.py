"""
Bronze: MinIO landing (Parquet) -> Postgres bronze.<table>.

For every table, for every landing batch not yet in bronze (oldest first):
  1. Find passed batches: folders under s3a://landing/<table>/ that have a
     _manifest.json. A folder without one is a failed landing and is ignored.
  2. Read ONLY that batch folder (never the table root, which would pull in
     every earlier batch) and check the row count against the manifest.
  3. Rename columns to snake_case and add metadata:
         _batch_id     landing batch id
         _source_file  Parquet file the row came from (_metadata.file_path)
         _landed_at    when landing wrote the row (landing's _ingested_at)
         _ingested_at  when this load ran (current_timestamp)
  4. Write to a staging table bronze_staging.<table> (Spark JDBC) and check
     its row count.
  5. Swap into bronze in ONE Postgres transaction:
         DELETE FROM bronze.<table> WHERE _batch_id = <batch>
         INSERT INTO bronze.<table> SELECT <cast to bronze types> FROM staging
         DROP the staging table
     Commit only if all three succeed (and inserted = staged); otherwise roll
     back. Deleting the batch first makes reruns safe: loading the same batch
     twice leaves exactly one copy.
  6. Only after that commit: update control.watermark (status = success) and
     add a row to control.run_log with the counts from each stage.

Any failure stops the run, rolls back the swap, and records status = failed
in control.watermark (keeping the last good watermark) and control.run_log.

Bronze tables are created up front from sql/postgres/bronze_tables.sql, which
has explicit types (see Notebooks/generate_bronze_ddl.py). Spark only ever
creates the throwaway staging tables.

Usage:
    .venv/Scripts/python Notebooks/landing_to_bronze.py
    .venv/Scripts/python Notebooks/landing_to_bronze.py --tables Sales.Orders
    # Reload one batch (rerun test): replaces that batch's rows in bronze
    .venv/Scripts/python Notebooks/landing_to_bronze.py --batch-id 20261115T165208Z

Config comes from .env (POSTGRES_*, MINIO_*), see .env.example.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg import sql
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent))
import source_to_landing as landing  # noqa: E402  (shared config, Spark, Lake)
from generate_bronze_ddl import bronze_table, snake  # noqa: E402

REPO_ROOT = landing.REPO_ROOT
CONTROL_DDL = REPO_ROOT / "sql" / "postgres" / "control.sql"
BRONZE_DDL = REPO_ROOT / "sql" / "postgres" / "bronze_tables.sql"

# The driver jar itself is pinned in jars/jars.lock.
POSTGRES_JDBC_DRIVER = "org.postgresql.Driver"
STAGING_SCHEMA = "bronze_staging"

log = logging.getLogger("landing_to_bronze")


class LoadError(Exception):
    pass


# --------------------------------------------------------------------------
# Postgres
# --------------------------------------------------------------------------

class Warehouse:
    """psycopg connection (DDL, swap, control) + Spark JDBC settings (staging)."""

    def __init__(self):
        e = landing.env
        self.host = e("POSTGRES_HOST", "localhost")
        self.port = e("POSTGRES_PORT", "5432")
        self.db = e("POSTGRES_DB")
        self.user = e("POSTGRES_USER")
        self.password = e("POSTGRES_PASSWORD")
        # autocommit: every block below opens its own explicit transaction.
        self.conn = psycopg.connect(
            host=self.host, port=self.port, dbname=self.db,
            user=self.user, password=self.password, autocommit=True,
        )
        # reWriteBatchedInserts: one multi-row INSERT per batch, much faster.
        self.jdbc_url = (
            f"jdbc:postgresql://{self.host}:{self.port}/{self.db}"
            "?reWriteBatchedInserts=true"
        )

    def close(self) -> None:
        self.conn.close()

    def apply_ddl(self) -> None:
        with self.conn.transaction():
            self.conn.execute(CONTROL_DDL.read_text(encoding="utf-8"))
            self.conn.execute(BRONZE_DDL.read_text(encoding="utf-8"))
            self.conn.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(STAGING_SCHEMA))
            )

    def columns(self, schema: str, table: str) -> dict[str, str]:
        """Column name -> Postgres type (e.g. 'numeric(18,2)'), in table order."""
        rows = self.conn.execute(
            """
            SELECT a.attname, format_type(a.atttypid, a.atttypmod)
            FROM pg_attribute a
            WHERE a.attrelid = to_regclass(%s) AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
            """,
            (f"{schema}.{table}",),
        ).fetchall()
        return dict(rows)

    def last_batch_id(self, table: str) -> str | None:
        row = self.conn.execute(
            "SELECT last_batch_id FROM control.watermark WHERE table_name = %s", (table,)
        ).fetchone()
        return row[0] if row else None

    def loaded_batch_ids(self, table: str) -> set[str]:
        """Every batch that has ever been loaded successfully for this table.

        Pending work is "passed in landing but not in this set", not
        "newer than last_batch_id": reloading a later batch by hand must not
        hide an older batch that was never loaded.
        """
        rows = self.conn.execute(
            "SELECT DISTINCT batch_id FROM control.run_log "
            "WHERE table_name = %s AND layer = 'bronze' AND status = 'success'",
            (table,),
        ).fetchall()
        return {r[0] for r in rows}

    def staging_count(self, table: str) -> int:
        return self.conn.execute(
            sql.SQL("SELECT count(*) FROM {}.{}").format(
                sql.Identifier(STAGING_SCHEMA), sql.Identifier(table))
        ).fetchone()[0]

    # -- the swap ----------------------------------------------------------

    def swap(self, table: str, batch_id: str, expected_rows: int) -> tuple[int, int]:
        """Delete batch, insert from staging, drop staging: one transaction."""
        target = self.columns("bronze", table)
        staged = self.columns(STAGING_SCHEMA, table)

        # Spark (session time zone UTC) stages timestamps as timestamptz. A plain
        # ::timestamp cast would convert with the Postgres *session* time zone
        # and shift every value, so convert at UTC explicitly.
        # format_type() puts the precision in the middle: "timestamp(6) with time zone".
        def kind(t: str) -> str | None:
            if t.startswith("timestamp"):
                return "ntz" if t.endswith("without time zone") else "tz"
            return None

        select_exprs = []
        for col, pg_type in target.items():
            src = sql.Identifier(col)
            k_staged, k_target = kind(staged[col]), kind(pg_type)
            if k_staged and k_target and k_staged != k_target:
                expr = sql.SQL("({} AT TIME ZONE 'UTC')::{}").format(src, sql.SQL(pg_type))
            else:
                expr = sql.SQL("{}::{}").format(src, sql.SQL(pg_type))
            select_exprs.append(expr)

        bronze = sql.SQL("{}.{}").format(sql.Identifier("bronze"), sql.Identifier(table))
        stage = sql.SQL("{}.{}").format(sql.Identifier(STAGING_SCHEMA), sql.Identifier(table))

        with self.conn.transaction():  # COMMIT on success, ROLLBACK on any exception
            # Staging is one shared table per bronze table; make sure it holds
            # this batch and nothing else (e.g. if two loads ever overlapped).
            staged_ids = [r[0] for r in self.conn.execute(
                sql.SQL("SELECT DISTINCT _batch_id FROM {}").format(stage)).fetchall()]
            if staged_ids != [batch_id]:
                raise LoadError(
                    f"{table}: staging holds batch ids {staged_ids}, expected [{batch_id!r}]"
                )
            deleted = self.conn.execute(
                sql.SQL("DELETE FROM {} WHERE _batch_id = %s").format(bronze), (batch_id,)
            ).rowcount
            inserted = self.conn.execute(
                sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
                    bronze,
                    sql.SQL(", ").join(map(sql.Identifier, target)),
                    sql.SQL(", ").join(select_exprs),
                    stage,
                )
            ).rowcount
            if inserted != expected_rows:
                raise LoadError(
                    f"{table}: inserted {inserted} rows, staged {expected_rows}; rolled back"
                )
            self.conn.execute(sql.SQL("DROP TABLE {}").format(stage))
        return deleted, inserted

    # -- control -----------------------------------------------------------

    def record_success(self, run: dict) -> None:
        with self.conn.transaction():
            # Never move the watermark backwards when an older batch is reloaded.
            self.conn.execute(
                """
                INSERT INTO control.watermark
                    (table_name, watermark_column, last_value, last_batch_id, status, error, updated_at)
                VALUES (%(table_name)s, %(watermark_column)s, %(last_value)s, %(batch_id)s,
                        'success', NULL, now())
                ON CONFLICT (table_name) DO UPDATE SET
                    watermark_column = EXCLUDED.watermark_column,
                    last_value = CASE WHEN control.watermark.last_batch_id IS NULL
                                        OR EXCLUDED.last_batch_id >= control.watermark.last_batch_id
                                      THEN EXCLUDED.last_value
                                      ELSE control.watermark.last_value END,
                    last_batch_id = GREATEST(control.watermark.last_batch_id, EXCLUDED.last_batch_id),
                    -- A success only clears the failure it resolves: reloading
                    -- some other (older) batch leaves a pending failure visible.
                    status = CASE WHEN control.watermark.failed_batch_id IS NULL
                                    OR control.watermark.failed_batch_id = EXCLUDED.last_batch_id
                                  THEN 'success' ELSE control.watermark.status END,
                    error = CASE WHEN control.watermark.failed_batch_id IS NULL
                                   OR control.watermark.failed_batch_id = EXCLUDED.last_batch_id
                                 THEN NULL ELSE control.watermark.error END,
                    failed_batch_id = CASE WHEN control.watermark.failed_batch_id = EXCLUDED.last_batch_id
                                           THEN NULL ELSE control.watermark.failed_batch_id END,
                    updated_at = now()
                """,
                run,
            )
            self._log_run(run, "success")

    def record_failure(self, run: dict) -> None:
        """Keep the last good watermark; only flag the failure."""
        with self.conn.transaction():
            self.conn.execute(
                """
                INSERT INTO control.watermark
                    (table_name, watermark_column, status, error, failed_batch_id, updated_at)
                VALUES (%(table_name)s, %(watermark_column)s, 'failed', %(error)s,
                        %(batch_id)s, now())
                ON CONFLICT (table_name) DO UPDATE SET
                    status = 'failed', error = EXCLUDED.error,
                    failed_batch_id = EXCLUDED.failed_batch_id, updated_at = now()
                """,
                run,
            )
            self._log_run(run, "failed")

    def _log_run(self, run: dict, status: str) -> None:
        self.conn.execute(
            """
            INSERT INTO control.run_log
                (run_id, table_name, batch_id, status, landing_rows, read_rows,
                 staged_rows, deleted_rows, inserted_rows, started_at, error)
            VALUES (%(run_id)s, %(table_name)s, %(batch_id)s, %(status)s, %(landing_rows)s,
                    %(read_rows)s, %(staged_rows)s, %(deleted_rows)s, %(inserted_rows)s,
                    %(started_at)s, %(error)s)
            """,
            {**run, "status": status},
        )


# --------------------------------------------------------------------------
# Landing side
# --------------------------------------------------------------------------

def passed_batches(lake: landing.Lake, table: str) -> list[tuple[dict, str]]:
    """(manifest, batch folder) for every batch with a _manifest.json, oldest first."""
    folder = bronze_table(table)
    pattern = f"{lake.root}/{folder}/ingest_date=*/batch_id=*/_manifest.json"
    statuses = lake._fs(pattern).globStatus(lake._Path(pattern)) or []
    batches = []
    for st in statuses:
        uri = st.getPath().toString()
        manifest = json.loads(lake.read_text(uri))
        if manifest.get("status") == "passed":
            batches.append((manifest, uri.rsplit("/", 1)[0]))
    return sorted(batches, key=lambda b: b[0]["batch_id"])


def prepare(df: DataFrame, batch_id: str) -> DataFrame:
    """Source columns -> snake_case, plus the four bronze metadata columns."""
    source_cols = [c for c in df.columns if c not in landing.LINEAGE_COLUMNS]
    return df.select(
        *[F.col(f"`{c}`").alias(snake(c)) for c in source_cols],
        F.lit(batch_id).alias("_batch_id"),
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_ingested_at").alias("_landed_at"),
        F.current_timestamp().alias("_ingested_at"),
    )


# --------------------------------------------------------------------------
# One batch of one table
# --------------------------------------------------------------------------

def load_batch(spark: SparkSession, wh: Warehouse, table: str, manifest: dict,
               path: str, run: dict) -> None:
    name = bronze_table(table)
    batch_id = manifest["batch_id"]
    run["landing_rows"] = manifest["checks"]["landed_row_count"]

    # Read ONLY this batch folder.
    raw = spark.read.parquet(path)
    log.debug("%s %s parquet schema:\n%s", table, batch_id, raw._jdf.schema().treeString())

    landed_ids = [r[0] for r in raw.select("_batch_id").distinct().collect()]
    if landed_ids != [batch_id]:
        raise LoadError(f"{table}: folder {path} holds batch ids {landed_ids}, expected {batch_id}")

    df = prepare(raw, batch_id).cache()
    run["read_rows"] = df.count()
    if run["read_rows"] != run["landing_rows"]:
        raise LoadError(f"{table}: read {run['read_rows']} rows, manifest says {run['landing_rows']}")

    # Columns must match the bronze table exactly (schema drift -> stop).
    target_cols = list(wh.columns("bronze", name))
    if not target_cols:
        raise LoadError(f"bronze.{name} does not exist; check {BRONZE_DDL.name}")
    missing, extra = set(target_cols) - set(df.columns), set(df.columns) - set(target_cols)
    if missing or extra:
        raise LoadError(f"{table}: column mismatch vs bronze.{name}: "
                        f"missing={sorted(missing)} unexpected={sorted(extra)}")

    # --- Stage (Spark creates/replaces the staging table) -----------------
    (
        df.select(*target_cols).write.format("jdbc")
        .option("url", wh.jdbc_url)
        .option("driver", POSTGRES_JDBC_DRIVER)
        .option("user", wh.user)
        .option("password", wh.password)
        .option("dbtable", f"{STAGING_SCHEMA}.{name}")
        .option("batchsize", 10000)
        .mode("overwrite")
        .save()
    )
    df.unpersist()
    run["staged_rows"] = wh.staging_count(name)
    if run["staged_rows"] != run["read_rows"]:
        raise LoadError(f"{table}: staged {run['staged_rows']} rows, read {run['read_rows']}")

    # --- Swap into bronze (one transaction) -------------------------------
    run["deleted_rows"], run["inserted_rows"] = wh.swap(name, batch_id, run["staged_rows"])


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--tables", nargs="+", choices=sorted(landing.TABLES),
                        help="Subset of tables (default: all)")
    parser.add_argument("--batch-id",
                        help="Load (or reload) only this landing batch, even if already in bronze")
    parser.add_argument("-v", "--verbose", action="store_true", help="Log Parquet schemas")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)
    landing.load_dotenv(REPO_ROOT / ".env")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    tables = args.tables or list(landing.TABLES)

    wh = Warehouse()
    wh.apply_ddl()
    spark = landing.build_spark("wwi-landing-to-bronze")
    spark.sparkContext.setLogLevel("WARN")
    lake = landing.Lake(spark, landing.env("MINIO_BUCKET", "landing"))
    lake.check_bucket()

    log.info("run_id=%s tables=%d%s", run_id, len(tables),
             f" batch_id={args.batch_id}" if args.batch_id else "")

    loaded = skipped = 0
    try:
        for table in tables:
            last = wh.last_batch_id(table)
            batches = passed_batches(lake, table)
            if args.batch_id:
                pending = [b for b in batches if b[0]["batch_id"] == args.batch_id]
            else:
                done_ids = wh.loaded_batch_ids(table)
                pending = [b for b in batches if b[0]["batch_id"] not in done_ids]

            if not pending:
                log.info("%-34s up to date (last batch %s)", table, last)
                skipped += 1
                continue

            for manifest, path in pending:
                run = {
                    "run_id": run_id, "table_name": table,
                    "batch_id": manifest["batch_id"],
                    "watermark_column": manifest["watermark_column"],
                    "last_value": manifest["window"]["high_inclusive"],
                    "landing_rows": None, "read_rows": None, "staged_rows": None,
                    "deleted_rows": None, "inserted_rows": None,
                    "started_at": datetime.now(timezone.utc), "error": None,
                }
                try:
                    load_batch(spark, wh, table, manifest, path, run)
                except Exception as exc:
                    run["error"] = f"{type(exc).__name__}: {exc}"[:4000]
                    wh.record_failure(run)
                    raise
                wh.record_success(run)
                loaded += 1
                log.info(
                    "%-34s %8d rows  batch %s  (replaced %d)  -> bronze.%s",
                    table, run["inserted_rows"], manifest["batch_id"],
                    run["deleted_rows"], bronze_table(table),
                )
    except Exception as exc:  # noqa: BLE001 - logged, recorded, non-zero exit
        log.error("LOAD FAILED -> stopping. Bronze swap rolled back, watermark kept. %s", exc)
        return 1
    finally:
        log.info("Summary: %d batches loaded, %d tables up to date", loaded, skipped)
        spark.stop()
        wh.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
