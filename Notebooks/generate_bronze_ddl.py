"""
Generate sql/postgres/bronze_tables.sql from the SQL Server catalog.

Run once (and again only if the source schema changes). The output file is
committed, so every bronze column type is reviewable and chosen on purpose,
instead of whatever Spark would infer on a first load.

Type mapping (SQL Server -> Postgres):
    int / bigint / smallint      -> integer / bigint / smallint
    tinyint                      -> smallint        (Postgres has no 1-byte int)
    bit                          -> boolean
    decimal(p,s) / numeric(p,s)  -> numeric(p,s)    (exact, never float)
    money / smallmoney           -> numeric(19,4) / numeric(10,4)
    float / real                 -> double precision / real
    date                         -> date
    datetime2(n)                 -> timestamp(min(n,6))   Postgres and Spark
                                    stop at microseconds; datetime2(7) loses
                                    its 100 ns digit (already lost in landing)
    datetime / smalldatetime     -> timestamp(3) / timestamp(0)
    datetimeoffset(n)            -> timestamptz(min(n,6))
    time(n)                      -> time(min(n,6))
    nvarchar(n) / varchar(n)     -> varchar(n)      (n in characters)
    nvarchar(max) / varchar(max) -> text
    nchar(n) / char(n)           -> char(n)
    uniqueidentifier             -> uuid
    varbinary / binary / image   -> bytea
    geography / geometry         -> text            (landed as WKT via STAsText)
    hierarchyid                  -> text            (landed via ToString)
    xml / sql_variant            -> text            (landed as nvarchar(max))
    sysname                      -> varchar(128)

Nullability follows the source: NOT NULL columns stay NOT NULL, so a bad batch
fails loudly at the swap instead of silently landing nulls.

No primary key on the business key: bronze is append-only history, so the same
OrderID appears again in every batch that re-lands an updated row.

Usage:
    .venv/Scripts/python Notebooks/generate_bronze_ddl.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import source_to_landing as landing  # noqa: E402  (shared config + TABLES)

OUT_FILE = landing.REPO_ROOT / "sql" / "postgres" / "bronze_tables.sql"

# Metadata columns added by the bronze load (after the source columns).
METADATA_DDL = [
    ("_batch_id", "text NOT NULL", "landing batch the row came from"),
    ("_source_file", "text NOT NULL", "Parquet file the row was read from"),
    ("_landed_at", "timestamptz NOT NULL", "when the row was written to landing"),
    ("_ingested_at", "timestamptz NOT NULL", "when the row was loaded into bronze"),
]


def snake(name: str) -> str:
    """PascalCase -> snake_case: SalespersonPersonID -> salesperson_person_id."""
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
    return s.lower()


def bronze_table(source_table: str) -> str:
    """Sales.Orders -> sales_orders (same as the landing folder name)."""
    return source_table.replace(".", "_").lower()


def pg_type(c: dict) -> str:
    t, length, prec, scale = c["type"], c["max_length"], c["precision"], c["scale"]
    chars = None if length == -1 else (length // 2 if t in ("nvarchar", "nchar") else length)
    frac = min(scale, 6)
    simple = {
        "int": "integer", "bigint": "bigint", "smallint": "smallint",
        "tinyint": "smallint", "bit": "boolean", "float": "double precision",
        "real": "real", "date": "date", "datetime": "timestamp(3)",
        "smalldatetime": "timestamp(0)", "money": "numeric(19,4)",
        "smallmoney": "numeric(10,4)", "uniqueidentifier": "uuid",
        "varbinary": "bytea", "binary": "bytea", "image": "bytea",
        "geography": "text", "geometry": "text", "hierarchyid": "text",
        "xml": "text", "sql_variant": "text", "sysname": "varchar(128)",
        "text": "text", "ntext": "text",
    }
    if t in simple:
        return simple[t]
    if t in ("decimal", "numeric"):
        return f"numeric({prec},{scale})"
    if t == "datetime2":
        # NOT A BUG: datetime2(7) keeps 100 ns, Postgres timestamp (and Spark)
        # stop at microseconds, so the 7th fractional digit is dropped. It is
        # already gone in landing: Spark's JDBC read TRUNCATES (never rounds).
        #   source  Application.Countries MAX(ValidFrom) 2022-10-07 21:11:44.8930093
        #   bronze                                       2022-10-07 21:11:44.893009
        # Every other WWI watermark ends in .0000000, so Countries is the only
        # table where it shows. Consequences:
        #   - compare bronze timestamps to the source at microseconds
        #     (truncate the source string to 26 chars, as validate() does);
        #   - the full 7 digits survive as text in the landing manifest and in
        #     control.watermark.last_value, so incremental windows stay exact.
        # Storing timestamps as text to keep the 7th digit would be worse.
        return f"timestamp({frac})"
    if t == "datetimeoffset":
        return f"timestamptz({frac})"
    if t == "time":
        return f"time({frac})"
    if t in ("varchar", "nvarchar"):
        return "text" if chars is None else f"varchar({chars})"
    if t in ("char", "nchar"):
        return f"char({chars})"
    raise ValueError(f"No Postgres mapping for SQL Server type {t!r} ({c['name']})")


def source_columns(source: landing.Source, table: str) -> list[dict]:
    rows = source.query(
        f"""
        SELECT c.column_id, c.name, ty.name AS type_name, c.max_length,
               c.precision, c.scale, c.is_nullable
        FROM sys.columns c
        JOIN sys.types ty ON ty.user_type_id = c.user_type_id
        WHERE c.object_id = OBJECT_ID('{table}')
        """
    )
    if not rows:
        raise RuntimeError(f"{table}: not found in source")
    return [
        {
            "name": r["name"], "type": r["type_name"].lower(),
            "max_length": int(r["max_length"]), "precision": int(r["precision"]),
            "scale": int(r["scale"]), "nullable": bool(r["is_nullable"]),
        }
        for r in sorted(rows, key=lambda r: r["column_id"])
    ]


def source_type_label(c: dict) -> str:
    """Readable SQL Server type for the DDL comments, e.g. nvarchar(50), decimal(18,2)."""
    t = c["type"]
    if t in ("varchar", "nvarchar", "char", "nchar", "varbinary", "binary"):
        n = "max" if c["max_length"] == -1 else (
            c["max_length"] // 2 if t in ("nvarchar", "nchar") else c["max_length"])
        return f"{t}({n})"
    if t in ("decimal", "numeric"):
        return f"{t}({c['precision']},{c['scale']})"
    if t in ("datetime2", "datetimeoffset", "time"):
        return f"{t}({c['scale']})"
    return t


def table_ddl(table: str, cols: list[dict]) -> str:
    name = bronze_table(table)
    lines = [
        (f"{snake(c['name'])} {pg_type(c)}{'' if c['nullable'] else ' NOT NULL'}",
         f"{c['name']} {source_type_label(c)}")
        for c in cols
    ]
    lines += [(f"{n} {t}", note) for n, t, note in METADATA_DDL]

    # Comma inside the padded part so the trailing comments line up.
    width = max(len(d) for d, _ in lines) + 2
    body = "\n".join(
        f"    {d + (',' if i < len(lines) - 1 else ''):<{width}}-- {note}"
        for i, (d, note) in enumerate(lines)
    )
    return (
        f"-- source: {table}\n"
        f"CREATE TABLE IF NOT EXISTS bronze.{name} (\n{body}\n);\n"
        f"CREATE INDEX IF NOT EXISTS ix_{name}__batch_id ON bronze.{name} (_batch_id);\n"
    )


def main() -> int:
    landing.load_dotenv(landing.REPO_ROOT / ".env")
    spark = landing.build_spark("wwi-generate-bronze-ddl")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        source = landing.Source(spark)
        parts = [
            "-- GENERATED by Notebooks/generate_bronze_ddl.py from the SQL Server catalog.\n"
            "-- Edit the mapping in the generator, not this file, then re-run it.\n"
            "-- Comments show the original SQL Server column name and type.\n\n"
            "CREATE SCHEMA IF NOT EXISTS bronze;\n"
        ]
        for table in landing.TABLES:
            parts.append(table_ddl(table, source_columns(source, table)))
    finally:
        spark.stop()

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUT_FILE.write_text("\n".join(parts), encoding="utf-8", newline="\n")
    print(f"wrote {OUT_FILE.relative_to(landing.REPO_ROOT)} ({len(landing.TABLES)} tables)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
