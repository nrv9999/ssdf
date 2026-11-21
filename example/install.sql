-- SSDF Example Installer
-- Applies the demo DWH on top of an already installed framework.
-- Run from this directory:  psql -d demo -f install.sql

\echo Installing schema dwh...
\i schema_dwh/01_schema.sql

\echo Installing dwh tables...
\i schema_dwh/01_tables/01_sa_collection_event.sql
\i schema_dwh/01_tables/02_sa_debt.sql
\i schema_dwh/01_tables/03_h_collection_event.sql
\i schema_dwh/01_tables/04_h_debt.sql
\i schema_dwh/01_tables/05_srs_collection_event.sql
\i schema_dwh/01_tables/06_srs_debt.sql
\i schema_dwh/01_tables/07_glb_collection_event.sql
\i schema_dwh/01_tables/08_glb_debt.sql
\i schema_dwh/01_tables/09_dwh_collection_event.sql

\echo Installing dwh views...
\i schema_dwh/02_views/01_vw_dwh_collection_event.sql

\echo Installing dwh functions and procedures...
\i schema_dwh/procedures/01_uf_sa_wait_contact_collection_event.sql
\i schema_dwh/procedures/02_uf_sa_wait_contact_debt.sql
\i schema_dwh/procedures/03_usp_sa_load_contact_collection_event.sql
\i schema_dwh/procedures/04_usp_sa_load_contact_debt.sql
\i schema_dwh/procedures/05_usp_srs_collection_event.sql
\i schema_dwh/procedures/06_usp_glb_collection_event.sql
\i schema_dwh/procedures/07_usp_srs_debt.sql
\i schema_dwh/procedures/08_usp_glb_debt.sql

-- horizons must precede flow: df.flow.horizon references df.horizon(name)
\echo Registering horizons...
\i horizon_definitions.sql

\echo Registering flow points...
\i flow_definitions.sql

\echo SSDF example installed successfully.
