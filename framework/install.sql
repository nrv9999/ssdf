-- SSDF Framework Installer
-- Apply in order to install the complete framework

\echo Installing schema df...
\i schema_df/01_schema.sql

\echo Installing types...
\i schema_df/01_types/01_point.sql
\i schema_df/01_types/02_exec_status.sql

\echo Installing tables...
\i schema_df/02_tables/01_horizon.sql
\i schema_df/02_tables/02_flow.sql
\i schema_df/02_tables/03_flow_rep_dttm.sql
\i schema_df/02_tables/04_flow_rep_dttm_clc.sql
\i schema_df/02_tables/05_exec_log.sql
\i schema_df/02_tables/06_batch_log.sql
\i schema_df/02_tables/07_dbg.sql

\echo Installing views...
\i schema_df/03_views/01_vw_flow_1.sql
\i schema_df/03_views/02_vw_flow_2.sql
\i schema_df/03_views/03_vw_flow.sql

\echo Installing functions...
\i schema_df/04_functions/01_get_for_resource.sql
\i schema_df/04_functions/02_find_free_job.sql

\echo Installing procedures...
\i schema_df/05_procedures/01_exec_id.sql
\i schema_df/05_procedures/02_exec_batch_from_id.sql
\i schema_df/05_procedures/03_exec_flow.sql
\i schema_df/05_procedures/04_wait_for_resource.sql

\echo SSDF framework installed successfully.
