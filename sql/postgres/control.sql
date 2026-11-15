-- Control tables for the bronze load (Notebooks/landing_to_bronze.py).
-- Idempotent: the script runs this on every start.

CREATE SCHEMA IF NOT EXISTS control;

-- One row per bronze table: how far bronze has got.
-- Updated only AFTER the bronze swap for a batch has committed.
CREATE TABLE IF NOT EXISTS control.watermark (
    table_name       text        PRIMARY KEY,           -- source table, e.g. Sales.Orders
    watermark_column text        NOT NULL,              -- ValidFrom / LastEditedWhen
    last_value       text,                              -- high watermark of the last loaded
                                                        -- batch, as SQL Server datetime2(7)
                                                        -- text (keeps all 7 digits)
    last_batch_id    text,                              -- last batch loaded into bronze
    status           text        NOT NULL
                     CHECK (status IN ('success', 'failed')),
    error            text,                              -- last error when status = failed
    updated_at       timestamptz NOT NULL DEFAULT now()
);

-- One row per table per batch attempt, with the row counts at each stage.
CREATE TABLE IF NOT EXISTS control.run_log (
    run_log_id     bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id         text        NOT NULL,                -- one id per script run
    layer          text        NOT NULL DEFAULT 'bronze',
    table_name     text        NOT NULL,
    batch_id       text        NOT NULL,
    status         text        NOT NULL
                   CHECK (status IN ('success', 'failed')),
    landing_rows   bigint,     -- row count recorded in the landing _manifest.json
    read_rows      bigint,     -- rows read from the batch folder
    staged_rows    bigint,     -- rows in the staging table after the Spark write
    deleted_rows   bigint,     -- rows of this batch already in bronze (rerun)
    inserted_rows  bigint,     -- rows inserted into bronze
    started_at     timestamptz NOT NULL,
    finished_at    timestamptz NOT NULL DEFAULT now(),
    error          text
);
CREATE INDEX IF NOT EXISTS ix_run_log__table_batch ON control.run_log (table_name, batch_id);
