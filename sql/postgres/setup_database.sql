-- One-time Postgres setup: the pipeline's login role and warehouse database.
-- Run as a superuser (e.g. postgres) with psql, passing the values from .env:
--
--   psql -U postgres -h localhost -v user=Pipeline_User -v password='...' \
--        -v db=world_wide_importers_dw -f sql/postgres/setup_database.sql
--
-- Schemas and tables are NOT created here: landing_to_bronze.py applies
-- control.sql and bronze_tables.sql itself (idempotent) on every run.

SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'user', :'password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'user')
\gexec

SELECT format('CREATE DATABASE %I OWNER %I', :'db', :'user')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'db')
\gexec
