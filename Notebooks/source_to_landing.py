"""
Landing: SQL Server (WideWorldImporters) -> MinIO as Parquet.

For every table:
  1. Read the low watermark from the control file (first run = full load).
  2. Freeze the high watermark: MAX(<watermark column>) in the source right now.
  3. Extract rows with  low < wm <= high  (filter runs INSIDE the JDBC subquery).
  4. Append to a brand-new batch folder:
         s3a://landing/<table>/ingest_date=<date>/batch_id=<id>/
  5. Read the batch folder back and check against the source, same window:
         - row count matches
         - null count per required column matches
         - every expected (source) column is present
         - MAX(watermark) in the batch equals the high watermark (catches
           time-zone shifts in timestamp values)
  6. All checks pass  -> write _manifest.json in the batch folder and advance
                         the watermark for that table.
     Any check fails  -> stop the run. The watermark is NOT updated, so the
                         next run re-extracts the same window.

Usage:
    .venv/Scripts/python Notebooks/source_to_landing.py
    .venv/Scripts/python Notebooks/source_to_landing.py --tables Sales.Orders Sales.OrderLines

Config comes from .env (MSSQL_*, MINIO_*), see .env.example.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[1]

# Table -> watermark column. Same 27 tables as docs/baseline.csv.
# Temporal (system-versioned) tables use ValidFrom: it changes on every update.
# Their history (_Archive) tables use ValidTo (see the comment below).
TABLES: dict[str, str] = {
    "Application.Cities": "ValidFrom",
    "Application.Countries": "ValidFrom",
    "Application.DeliveryMethods": "ValidFrom",
    "Application.People": "ValidFrom",
    "Application.StateProvinces": "ValidFrom",
    "Application.TransactionTypes": "ValidFrom",
    "Purchasing.PurchaseOrderLines": "LastEditedWhen",
    "Purchasing.PurchaseOrders": "LastEditedWhen",
    "Purchasing.SupplierCategories": "ValidFrom",
    "Purchasing.Suppliers": "ValidFrom",
    "Purchasing.SupplierTransactions": "LastEditedWhen",
    "Sales.BuyingGroups": "ValidFrom",
    "Sales.CustomerCategories": "ValidFrom",
    "Sales.Customers": "ValidFrom",
    "Sales.CustomerTransactions": "LastEditedWhen",
    "Sales.InvoiceLines": "LastEditedWhen",
    "Sales.Invoices": "LastEditedWhen",
    "Sales.OrderLines": "LastEditedWhen",
    "Sales.Orders": "LastEditedWhen",
    "Warehouse.StockGroups": "ValidFrom",
    "Warehouse.StockItemHoldings": "LastEditedWhen",
    "Warehouse.StockItems": "ValidFrom",
    "Warehouse.StockItemStockGroups": "LastEditedWhen",
    "Warehouse.StockItemTransactions": "LastEditedWhen",
    # History tables of the three SCD2 dimensions (DR-02). A plain SELECT on a
    # temporal table returns current rows only; earlier versions live here.
    # Watermark is ValidTo, NOT ValidFrom: when a row changes at time T its
    # old version is moved here with ValidTo = T but its original (older)
    # ValidFrom, so a ValidFrom watermark would skip every newly archived row.
    "Purchasing.Suppliers_Archive": "ValidTo",
    "Sales.Customers_Archive": "ValidTo",
    "Warehouse.StockItems_Archive": "ValidTo",
}

# Extra columns to treat as "required" for the null-count check, on top of
# every NOT NULL column in the source. Add entries here if a nullable column
# matters downstream, e.g. {"Sales.Invoices": ["ConfirmedDeliveryTime"]}.
EXTRA_REQUIRED_COLUMNS: dict[str, list[str]] = {}

# Lowest datetime2 value. Used as the low watermark on the first (full) load.
INITIAL_WATERMARK = "0001-01-01 00:00:00.0000000"
# Watermarks are datetime2(7) text (CONVERT style 121); nothing else is allowed.
WATERMARK_FORMAT = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{7}")
# Batch ids sort chronologically as text; bronze relies on that.
BATCH_ID_FORMAT = re.compile(r"\d{8}T\d{6}Z")

# SQL types Spark JDBC cannot read. They're converted to text in the query.
SPATIAL_TYPES = {"geography", "geometry"}
TO_STRING_TYPES = {"hierarchyid"}
CAST_TO_NVARCHAR_TYPES = {"xml", "sql_variant"}

# Lineage columns added to every landed row (not part of the source checks).
LINEAGE_COLUMNS = ["_batch_id", "_ingested_at"]

log = logging.getLogger("source_to_landing")


def load_dotenv(path: Path) -> None:
    """Minimal .env loader (no python-dotenv dependency). Real env vars win."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name) or default
    if not value:
        raise SystemExit(f"Missing required setting {name} (set it in .env)")
    return value


# --------------------------------------------------------------------------
# Spark session (JDBC driver + S3A for MinIO)
# --------------------------------------------------------------------------

def locked_jars() -> list[Path]:
    """The jars pinned in jars/jars.lock, which must already be downloaded."""
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    from fetch_jars import JARS_DIR, jar_name, read_lock

    jars = [JARS_DIR / jar_name(a, v) for _, a, v, _ in read_lock()]
    missing = [j.name for j in jars if not j.exists()]
    if missing:
        raise SystemExit(
            f"Missing Spark jars {missing}. Run:  python scripts/fetch_jars.py"
        )
    return jars


def build_spark(app_name: str = "wwi-source-to-landing") -> SparkSession:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    endpoint = env("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY") or env("MINIO_ROOT_USER")
    secret_key = os.environ.get("MINIO_SECRET_KEY") or env("MINIO_ROOT_PASSWORD")

    # JDBC drivers turn datetime2/timestamp values into instants using the
    # JVM default time zone (e.g. Africa/Johannesburg, UTC+2), while Spark
    # renders them in spark.sql.session.timeZone. If the two differ, every
    # timestamp is shifted. Pin the JVM itself to UTC so wall-clock values
    # pass through unchanged.
    utc_jvm = "-Duser.timezone=UTC"

    # Jars come from jars/jars.lock (scripts/fetch_jars.py), the same on every
    # OS. Not spark.jars.packages: resolving at runtime chmods the downloaded
    # jars through winutils.exe, which a plain Windows machine doesn't have.
    # extraClassPath covers local mode, where executors run in the driver JVM.
    classpath = os.pathsep.join(str(j) for j in locked_jars())

    return (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.driver.extraClassPath", classpath)
        .config("spark.executor.extraClassPath", classpath)
        .config("spark.driver.extraJavaOptions", utc_jvm)
        .config("spark.executor.extraJavaOptions", utc_jvm)
        # datetime2 has no time zone. Pin Spark to UTC so values are never
        # shifted (e.g. across DST changes) between source and Parquet.
        .config("spark.sql.session.timeZone", "UTC")
        # --- S3A -> MinIO (Hadoop AWS guide) ---
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Buffer uploads in memory, not on local disk: no local temp dirs,
        # which on Windows would otherwise need winutils.exe.
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
        .getOrCreate()
    )


# --------------------------------------------------------------------------
# Source (SQL Server over JDBC)
# --------------------------------------------------------------------------

class Source:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        server = env("MSSQL_SERVER", "localhost")
        port = env("MSSQL_PORT", "1433")
        database = env("MSSQL_DATABASE", "WideWorldImporters")
        self.url = (
            f"jdbc:sqlserver://{server}:{port};databaseName={database};"
            "encrypt=true;trustServerCertificate=true"
        )
        self.props = {
            "user": env("MSSQL_USER", "sa"),
            "password": env("MSSQL_SA_PASSWORD"),
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }

    def _reader(self):
        r = self.spark.read.format("jdbc").option("url", self.url)
        for k, v in self.props.items():
            r = r.option(k, v)
        return r.option("fetchsize", 10000)

    def query(self, sql: str) -> list:
        """Run a small query (metadata/counts) and collect it."""
        return self._reader().option("query", sql).load().collect()

    def table(self, subquery: str) -> DataFrame:
        """Read via dbtable = '(<subquery>) AS src'."""
        return self._reader().option("dbtable", f"({subquery}) AS src").load()

    # -- helpers ---------------------------------------------------------

    def columns(self, table: str) -> list[dict]:
        """Column name, type and nullability, in table order."""
        rows = self.query(
            f"""
            SELECT c.column_id, c.name AS column_name, ty.name AS type_name,
                   c.is_nullable
            FROM sys.columns c
            JOIN sys.types ty ON ty.user_type_id = c.user_type_id
            WHERE c.object_id = OBJECT_ID('{table}')
            """
        )
        if not rows:
            raise RuntimeError(f"{table}: table not found in source")
        rows = sorted(rows, key=lambda r: r["column_id"])
        return [
            {
                "name": r["column_name"],
                "type": r["type_name"].lower(),
                "nullable": bool(r["is_nullable"]),
            }
            for r in rows
        ]

    def max_watermark(self, table: str, wm_col: str) -> str | None:
        """MAX(wm) as a full-precision datetime2 string (style 121)."""
        rows = self.query(
            f"SELECT CONVERT(varchar(27), MAX([{wm_col}]), 121) AS hwm FROM {quote_table(table)}"
        )
        return rows[0]["hwm"]


def quote_table(table: str) -> str:
    schema, name = table.split(".")
    return f"[{schema}].[{name}]"


def select_list(cols: list[dict]) -> str:
    """Column list for the extract. Unsupported types are converted to text."""
    parts = []
    for c in cols:
        n = f"[{c['name']}]"
        if c["type"] in SPATIAL_TYPES:
            parts.append(f"{n}.STAsText() AS {n}")
        elif c["type"] in TO_STRING_TYPES:
            parts.append(f"{n}.ToString() AS {n}")
        elif c["type"] in CAST_TO_NVARCHAR_TYPES:
            parts.append(f"CAST({n} AS nvarchar(max)) AS {n}")
        else:
            parts.append(n)
    return ",\n       ".join(parts)


def window_predicate(wm_col: str, low: str, high: str) -> str:
    return (
        f"[{wm_col}] > CAST('{low}' AS datetime2(7)) "
        f"AND [{wm_col}] <= CAST('{high}' AS datetime2(7))"
    )


def extraction_query(table: str, cols: list[dict], wm_col: str, low: str, high: str) -> str:
    return (
        f"SELECT {select_list(cols)}\n"
        f"FROM {quote_table(table)}\n"
        f"WHERE {window_predicate(wm_col, low, high)}"
    )


def source_profile_query(table: str, required: list[str], wm_col: str, low: str, high: str) -> str:
    """Row count + null count per required column, same window as the extract."""
    null_exprs = [
        f"ISNULL(SUM(CASE WHEN [{c}] IS NULL THEN 1 ELSE 0 END), 0) AS n{i}"
        for i, c in enumerate(required)
    ]
    select = ",\n       ".join(["COUNT_BIG(*) AS row_count", *null_exprs])
    return (
        f"SELECT {select}\n"
        f"FROM {quote_table(table)}\n"
        f"WHERE {window_predicate(wm_col, low, high)}"
    )


# --------------------------------------------------------------------------
# Landing zone helpers (MinIO via the Hadoop FileSystem API)
# --------------------------------------------------------------------------

class Lake:
    def __init__(self, spark: SparkSession, bucket: str):
        self.spark = spark
        self.root = f"s3a://{bucket}"
        self.watermark_file = f"{self.root}/_control/watermarks.json"
        jvm = spark._jvm
        self._Path = jvm.org.apache.hadoop.fs.Path
        self._conf = spark._jsc.hadoopConfiguration()
        self._jvm = jvm

    def _fs(self, uri: str):
        return self._Path(uri).getFileSystem(self._conf)

    def check_bucket(self) -> None:
        hint = (f"Does the bucket exist in MinIO "
                f"(create it in the console at http://localhost:9001)?")
        # exists() is no use here: S3A reports a bucket's root as present even
        # when the bucket isn't. Listing it raises UnknownStoreException instead.
        try:
            self._fs(self.root + "/").listStatus(self._Path(self.root + "/"))
        except Exception as exc:  # noqa: BLE001 - surface a clear message
            first_line = str(exc).strip().splitlines()[0][:200]
            raise SystemExit(f"Bucket {self.root} not reachable. {hint} ({first_line})") from None

    def passed_manifests(self, table: str) -> list[dict]:
        """Manifests of every passed batch of this table, oldest batch first."""
        folder = table.replace(".", "_").lower()
        pattern = f"{self.root}/{folder}/ingest_date=*/batch_id=*/_manifest.json"
        statuses = self._fs(pattern).globStatus(self._Path(pattern)) or []
        manifests = [json.loads(self.read_text(s.getPath().toString())) for s in statuses]
        return sorted((m for m in manifests if m.get("status") == "passed"),
                      key=lambda m: m["batch_id"])

    def exists(self, uri: str) -> bool:
        return self._fs(uri).exists(self._Path(uri))

    def read_text(self, uri: str) -> str:
        fs = self._fs(uri)
        stream = fs.open(self._Path(uri))
        try:
            reader = self._jvm.java.io.BufferedReader(
                self._jvm.java.io.InputStreamReader(stream, "UTF-8")
            )
            lines = []
            line = reader.readLine()
            while line is not None:
                lines.append(line)
                line = reader.readLine()
            return "\n".join(lines)
        finally:
            stream.close()

    def write_text(self, uri: str, text: str) -> None:
        out = self._fs(uri).create(self._Path(uri), True)  # overwrite=True
        try:
            out.write(bytearray(text.encode("utf-8")))
        finally:
            out.close()

    # -- watermarks ------------------------------------------------------

    def load_watermarks(self) -> dict:
        if not self.exists(self.watermark_file):
            return {}
        return json.loads(self.read_text(self.watermark_file))

    def save_watermark(self, table: str, watermark: str, batch_id: str) -> None:
        # Re-read so we only ever change this one table's entry.
        state = self.load_watermarks()
        state[table] = {
            "watermark": watermark,
            "batch_id": batch_id,
            "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        self.write_text(self.watermark_file, json.dumps(state, indent=2, sort_keys=True))

    def batch_path(self, table: str, ingest_date: str, batch_id: str) -> str:
        folder = table.replace(".", "_").lower()  # Sales.Orders -> sales_orders
        return f"{self.root}/{folder}/ingest_date={ingest_date}/batch_id={batch_id}"


# --------------------------------------------------------------------------
# Checks
# --------------------------------------------------------------------------

class ValidationError(Exception):
    pass


def validate(
    spark: SparkSession,
    source: Source,
    table: str,
    batch_path: str,
    expected_cols: list[str],
    required_cols: list[str],
    wm_col: str,
    low: str,
    high: str,
) -> dict:
    """Read the batch folder back and compare with the source for the same window."""
    landed = spark.read.parquet(batch_path)

    # 1. Expected columns present
    missing = [c for c in expected_cols if c not in landed.columns]

    # 2 + 3. Row count and null counts, in one pass over the landed data
    aggs = [F.count(F.lit(1)).alias("row_count")] + [
        F.sum(F.when(F.col(f"`{c}`").isNull(), 1).otherwise(0)).alias(f"n{i}")
        for i, c in enumerate(required_cols)
        if c in landed.columns
    ]
    # 4. Values survived the trip: landed MAX(watermark) == source high
    #    watermark. Catches time-zone shifts that counts alone never see.
    #    Spark keeps microseconds and its JDBC read truncates the 7th digit,
    #    so compare at datetime2(6): high[:26] drops the 100 ns digit. This
    #    is deliberately looser than the source (see generate_bronze_ddl.py).
    aggs.append(F.date_format(F.max(wm_col), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("wm_max"))
    landed_row = landed.agg(*aggs).first()
    src_row = source.query(source_profile_query(table, required_cols, wm_col, low, high))[0]

    src_count = int(src_row["row_count"])
    landed_count = int(landed_row["row_count"])

    null_mismatches = {}
    for i, c in enumerate(required_cols):
        if c not in landed.columns:
            continue  # already reported as missing
        s, l = int(src_row[f"n{i}"]), int(landed_row[f"n{i}"] or 0)
        if s != l:
            null_mismatches[c] = {"source": s, "landed": l}

    failures = []
    if src_count != landed_count:
        failures.append(f"row count: source={src_count}, landed={landed_count}")
    if null_mismatches:
        failures.append(f"null counts differ: {null_mismatches}")
    if missing:
        failures.append(f"missing columns: {missing}")
    if landed_row["wm_max"] != high[:26]:
        failures.append(
            f"max({wm_col}) differs: source={high[:26]}, landed={landed_row['wm_max']}"
        )

    result = {
        "source_row_count": src_count,
        "landed_row_count": landed_count,
        "required_columns_checked": len(required_cols),
        "null_mismatches": null_mismatches,
        "missing_columns": missing,
        "landed_max_watermark": landed_row["wm_max"],
    }
    if failures:
        raise ValidationError(f"{table}: " + "; ".join(failures))
    return result


# --------------------------------------------------------------------------
# One table
# --------------------------------------------------------------------------

def land_table(
    spark: SparkSession,
    source: Source,
    lake: Lake,
    table: str,
    wm_col: str,
    watermarks: dict,
    ingest_date: str,
    batch_id: str,
) -> dict:
    low = watermarks.get(table, {}).get("watermark", INITIAL_WATERMARK)

    # The manifest is the commit point. If a run died after writing a passed
    # manifest but before saving the watermark, resume from the manifest's
    # high watermark instead of re-extracting (and duplicating) that window.
    manifests = lake.passed_manifests(table)
    if manifests:
        latest = manifests[-1]
        if latest["window"]["high_inclusive"] > low:
            log.warning("%s: watermark %s is behind passed batch %s; resuming from %s",
                        table, low, latest["batch_id"], latest["window"]["high_inclusive"])
            low = latest["window"]["high_inclusive"]
            lake.save_watermark(table, low, latest["batch_id"])  # repair the control file
        if batch_id <= latest["batch_id"]:
            raise ValidationError(
                f"{table}: batch id {batch_id} is not newer than existing batch "
                f"{latest['batch_id']}; batch ids must increase (bronze orders by them)"
            )

    high = source.max_watermark(table, wm_col)
    # Both values are pasted into the SQL Server query: accept only the exact
    # datetime2(7) text format, never arbitrary text from the control file.
    for name, value in (("low", low), ("high", high)):
        if value is not None and not WATERMARK_FORMAT.fullmatch(value):
            raise ValidationError(f"{table}: {name} watermark {value!r} is not datetime2(7) text")

    if high is None or high <= low:  # same fixed-width format -> string compare is safe
        log.info("%-34s no new rows (watermark %s)", table, low)
        return {"table": table, "status": "no_new_rows", "watermark": low}

    cols = source.columns(table)
    expected_cols = [c["name"] for c in cols]
    required_cols = [c["name"] for c in cols if not c["nullable"]]
    for extra in EXTRA_REQUIRED_COLUMNS.get(table, []):
        if extra not in required_cols:
            required_cols.append(extra)

    # --- Extract: watermark window filtered inside the JDBC subquery -----
    query = extraction_query(table, cols, wm_col, low, high)
    log.debug("%s extraction query:\n%s", table, query)
    df = (
        source.table(query)
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingested_at", F.current_timestamp())
    )

    # --- Land: new folder per batch, append (the path is always new) -----
    path = lake.batch_path(table, ingest_date, batch_id)
    if lake.exists(path):
        raise ValidationError(f"{table}: batch folder already exists: {path}")
    df.write.mode("append").parquet(path)

    # --- Checks ----------------------------------------------------------
    checks = validate(spark, source, table, path, expected_cols, required_cols, wm_col, low, high)

    # --- Commit: manifest first, then advance the watermark ---------------
    manifest = {
        "table": table,
        "batch_id": batch_id,
        "ingest_date": ingest_date,
        "watermark_column": wm_col,
        "window": {"low_exclusive": low, "high_inclusive": high},
        "columns": expected_cols,
        "checks": checks,
        "status": "passed",
    }
    lake.write_text(f"{path}/_manifest.json", json.dumps(manifest, indent=2))
    lake.save_watermark(table, high, batch_id)

    log.info(
        "%-34s %8d rows  (%s, %s]  -> %s",
        table, checks["landed_row_count"], low, high, path,
    )
    return {"table": table, "status": "landed", "rows": checks["landed_row_count"],
            "watermark": high, "path": path}


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--tables", nargs="+", choices=sorted(TABLES),
                        help="Subset of tables (default: all)")
    parser.add_argument("--batch-id",
                        help="Override batch id (default: UTC timestamp, YYYYMMDDTHHMMSSZ). "
                             "Must be newer than every existing batch of each table.")
    parser.add_argument("-v", "--verbose", action="store_true", help="Log extraction queries")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)
    load_dotenv(REPO_ROOT / ".env")

    now = datetime.now(timezone.utc)
    ingest_date = now.strftime("%Y-%m-%d")
    batch_id = args.batch_id or now.strftime("%Y%m%dT%H%M%SZ")
    if not BATCH_ID_FORMAT.fullmatch(batch_id):
        parser.error(f"--batch-id must look like 20261115T172904Z, got {batch_id!r}")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    source = Source(spark)
    lake = Lake(spark, env("MINIO_BUCKET", "landing"))
    lake.check_bucket()

    tables = args.tables or list(TABLES)
    watermarks = lake.load_watermarks()
    log.info("batch_id=%s ingest_date=%s tables=%d", batch_id, ingest_date, len(tables))

    results = []
    try:
        for table in tables:
            results.append(
                land_table(spark, source, lake, table, TABLES[table],
                           watermarks, ingest_date, batch_id)
            )
    except ValidationError as exc:
        log.error("CHECK FAILED -> stopping. Watermark NOT updated. %s", exc)
        log.error("The batch folder has no _manifest.json; downstream must ignore it.")
        return 1
    finally:
        landed = [r for r in results if r["status"] == "landed"]
        log.info("Summary: %d landed, %d unchanged, %d of %d processed",
                 len(landed), len(results) - len(landed), len(results), len(tables))
        spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())
