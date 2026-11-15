# ADR 0001: Keep landing and bronze watermarks in separate stores

- **Status:** Accepted
- **Date:** 2026-11-15
- **Deciders:** project owner

## Context

The pipeline has two incremental steps, and each needs to remember how far it has got:

1. **Landing** (`Notebooks/source_to_landing.py`, SQL Server to MinIO) needs the last extracted source value per table, for example `MAX(LastEditedWhen)`. It uses this as the exclusive low bound of the next extraction window.
2. **Bronze** (`Notebooks/landing_to_bronze.py`, MinIO to Postgres) needs to know which landing batches it has already loaded.

Right now these live in two places:

| | Landing | Bronze |
|---|---|---|
| Store | `s3a://landing/_control/watermarks.json` (MinIO) | `control.watermark` (Postgres) |
| Key | source table | source table |
| Drives | the extraction window `low < wm <= high` | which batch folders are still pending (`batch_id > last_batch_id`) |
| Value that matters | `watermark`: source datetime2(7) as text | `last_batch_id` (`last_value` is a copy for humans) |
| Updated | after a batch folder passes its checks and its `_manifest.json` is written | after the bronze swap transaction commits |

The bronze build raised the question of merging them into a single `control.watermark` table.

## Decision

Keep two stores. They answer different questions, and each lives next to the data it describes:

- **Landing's watermark answers "what has been extracted from the source?"** It lives in the lake, beside the batch folders and manifests it refers to. Landing can then run, be replayed or be rebuilt without Postgres running at all. The lake describes itself: the manifests plus `watermarks.json` are a complete record of what landing contains.
- **`control.watermark` answers "what has been loaded into the warehouse?"** It lives in Postgres, beside the bronze tables. That lets bronze update it in the same database right after the swap commits, and dbt, Airflow and anyone querying SQL can read load state without touching object storage.

Bronze does **not** read landing's `watermarks.json`. It finds pending work from the `_manifest.json` files themselves, so the only link between the layers is the batch folder contract: a folder with a passing manifest is complete and immutable.

## Consequences

**Good**

- Each layer can be rebuilt on its own:
  - Wipe Postgres: bronze reloads every manifest-bearing batch, and landing is untouched.
  - Wipe the lake and its watermarks: landing re-extracts from the source. Bronze then needs its rows for the old batch ids cleared, or a full reload.
- Neither step has to write to the other layer's storage, and neither step has a cross-system transaction to get wrong.
- Landing's watermark keeps the full `datetime2(7)` precision as text, so extraction windows stay exact even though Spark and Postgres timestamps truncate to microseconds.

**Costs, and how they are handled**

- **Two places to look when debugging.** `control.watermark.last_value` copies the high watermark from the batch's manifest, so a single SQL query shows both how far bronze has got and the matching source value.
- **The stores can disagree.** This is expected. Bronze is allowed to be behind landing, and that is exactly the pending work. Bronze must never be *ahead* of landing. That would mean `control.watermark.last_batch_id` names a batch that no longer exists in the lake, which only happens if the lake is wiped and Postgres isn't. The fix is to reset `control.watermark` (and truncate bronze) whenever the landing zone is reset.
- **Resets need care.** A full reset means wiping both stores together: the landing bucket including `_control/`, plus the `bronze` tables and `control.watermark`. This belongs in the reset runbook and, later, in an Airflow "full refresh" task.

## Alternatives considered

1. **One table in Postgres for both layers** (for example `control.watermark` with a `layer` column, and landing reading and writing it over JDBC/psycopg).
   - Rejected for now: landing would depend on Postgres being up, and the lake would stop describing itself. A landing batch could also commit to MinIO while its watermark update to Postgres fails, which is a cross-system write with no shared transaction. Today the watermark sits in the same store as the data it guards, so that failure can't happen.
2. **One file in MinIO for both layers** (bronze writes its state back to the lake).
   - Rejected: bronze state would sit away from the data it describes, and SQL consumers (dbt, Airflow sensors, ad-hoc checks) couldn't see it.
3. **No bronze watermark: compute pending batches from `SELECT DISTINCT _batch_id FROM bronze.<table>`.**
   - Workable, but it scans every bronze table on each run and leaves nowhere to record a failed load or the per-run counts. `control.run_log` and `status = failed` need a home anyway.

## Revisit when

- Airflow orchestration arrives. If the DAG wants one place to read "how far has each layer got", a read-only view over both may be enough, without merging the stores.
- Landing moves to a table format with its own transaction log (Delta or Iceberg). The format's commit history would then replace `watermarks.json`.
