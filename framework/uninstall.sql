-- SSDF Framework Uninstaller
-- WARNING: drops schema df together with ALL logs and history
-- (batch_log, exec_log, dbg, flow_rep_dttm, flow_rep_dttm_clc) — irreversible.
-- Run example/uninstall.sql FIRST: dwh objects depend on df types.
-- psql -d demo -f uninstall.sql

\echo Dropping schema df (all logs and history go with it)...
DROP SCHEMA IF EXISTS df CASCADE;

\echo SSDF framework uninstalled successfully.
