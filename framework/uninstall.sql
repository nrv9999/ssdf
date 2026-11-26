-- SSDF Framework Uninstaller
-- WARNING: drops schema ssdf together with ALL logs and history
-- (batch_log, exec_log, dbg, flow_rep_dttm, flow_rep_dttm_clc) — irreversible.
-- Run example/uninstall.sql FIRST: smpl_dwh objects depend on ssdf types.
-- psql -d demo -f uninstall.sql

\echo Dropping schema ssdf (all logs and history go with it)...
DROP SCHEMA IF EXISTS ssdf CASCADE;

\echo SSDF framework uninstalled successfully.
