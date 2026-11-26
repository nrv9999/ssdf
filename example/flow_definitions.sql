-- Flow definitions for the demo data flow
-- Collection process
--
-- Definitions are authoritative: clear and re-register on every run so that
-- edits take effect. RESTART IDENTITY keeps point ids stable across reinstalls.
--
-- Each StrtF point targets the sa-level load procedure it feeds (waits for the
-- external source and returns its rep date). The load targets the sa-table and
-- depends on the StrtF point ('f', its only builder), so the chain stays connected
-- without a self-dependency.
TRUNCATE TABLE ssdf.flow RESTART IDENTITY;

INSERT INTO ssdf.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
SELECT 'StrtF', 'Collection', NULL, NULL, 100, 'smpl_dwh.uf_sa_wait_contact_collection_event', 'smpl_dwh.usp_sa_load_contact_collection_event', 1, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM ssdf.flow WHERE dest = 'smpl_dwh.usp_sa_load_contact_collection_event' AND type = 'StrtF');

INSERT INTO ssdf.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
SELECT 'StrtF', 'Collection', NULL, NULL, 100, 'smpl_dwh.uf_sa_wait_contact_debt', 'smpl_dwh.usp_sa_load_contact_debt', 1, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM ssdf.flow WHERE dest = 'smpl_dwh.usp_sa_load_contact_debt' AND type = 'StrtF');

INSERT INTO ssdf.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
SELECT 'P', 'Collection', 'Collection', ARRAY[('smpl_dwh.usp_sa_load_contact_collection_event', 'f')::ssdf.point], 100,
    'smpl_dwh.usp_sa_load_contact_collection_event', 'smpl_dwh.sa_collection_event', 2, 'dt', NULL
WHERE NOT EXISTS (SELECT 1 FROM ssdf.flow WHERE dest = 'smpl_dwh.sa_collection_event' AND type = 'P');

INSERT INTO ssdf.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
SELECT 'P', 'Collection', 'Collection', ARRAY[('smpl_dwh.usp_sa_load_contact_debt', 'f')::ssdf.point], 100,
    'smpl_dwh.usp_sa_load_contact_debt', 'smpl_dwh.sa_debt', 2, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM ssdf.flow WHERE dest = 'smpl_dwh.sa_debt' AND type = 'P');

INSERT INTO ssdf.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
SELECT 'V', 'Collection', 'Collection', ARRAY[('smpl_dwh.sa_collection_event', 'f')::ssdf.point], 1000,
    'smpl_dwh.sa_collection_event', 'smpl_dwh.h_collection_event', 1, 'dt', NULL
WHERE NOT EXISTS (SELECT 1 FROM ssdf.flow WHERE dest = 'smpl_dwh.h_collection_event');

INSERT INTO ssdf.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
SELECT 'V', 'Collection', 'Collection', ARRAY[('smpl_dwh.sa_debt', 'f')::ssdf.point], 1000,
    'smpl_dwh.sa_debt', 'smpl_dwh.h_debt', 1, 'dt', NULL
WHERE NOT EXISTS (SELECT 1 FROM ssdf.flow WHERE dest = 'smpl_dwh.h_debt');

INSERT INTO ssdf.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
SELECT 'P', 'Collection', 'Collection', ARRAY[('smpl_dwh.sa_collection_event', 'f')::ssdf.point], 100,
    'smpl_dwh.usp_srs_collection_event', 'smpl_dwh.srs_collection_event', 1, 'dttm', NULL
WHERE NOT EXISTS (SELECT 1 FROM ssdf.flow WHERE dest = 'smpl_dwh.srs_collection_event');

INSERT INTO ssdf.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
SELECT 'P', 'Collection', 'Collection', ARRAY[('smpl_dwh.srs_collection_event', 'f')::ssdf.point], 100,
    'smpl_dwh.usp_glb_collection_event', 'smpl_dwh.glb_collection_event', 1, 'dttm', NULL
WHERE NOT EXISTS (SELECT 1 FROM ssdf.flow WHERE dest = 'smpl_dwh.glb_collection_event');

INSERT INTO ssdf.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
SELECT 'P', 'Collection', 'Collection', ARRAY[('smpl_dwh.sa_debt', 'f')::ssdf.point], 100,
    'smpl_dwh.usp_srs_debt', 'smpl_dwh.srs_debt', 1, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM ssdf.flow WHERE dest = 'smpl_dwh.srs_debt');

INSERT INTO ssdf.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
SELECT 'P', 'Collection', 'Collection', ARRAY[('smpl_dwh.srs_debt', 'f')::ssdf.point], 100,
    'smpl_dwh.usp_glb_debt', 'smpl_dwh.glb_debt', 1, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM ssdf.flow WHERE dest = 'smpl_dwh.glb_debt');

INSERT INTO ssdf.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
SELECT 'V', 'Collection', 'Collection',
    ARRAY[('smpl_dwh.srs_collection_event', 'f')::ssdf.point, ('smpl_dwh.glb_collection_event', 'f')::ssdf.point,
          ('smpl_dwh.srs_debt', 'f')::ssdf.point, ('smpl_dwh.glb_debt', 'f')::ssdf.point,
          ('smpl_dwh.sa_collection_event', 'f')::ssdf.point],
    100, 'smpl_dwh.vw_dwh_collection_event', 'smpl_dwh.dwh_collection_event', 1, 'dttm', NULL
WHERE NOT EXISTS (SELECT 1 FROM ssdf.flow WHERE dest = 'smpl_dwh.dwh_collection_event');

INSERT INTO ssdf.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
SELECT 'V', 'Collection', 'Collection',
    ARRAY[('smpl_dwh.srs_debt', 'f')::ssdf.point, ('smpl_dwh.glb_debt', 'f')::ssdf.point,
          ('smpl_dwh.sa_debt', 'f')::ssdf.point],
    100, 'smpl_dwh.vw_dwh_debt', 'smpl_dwh.dwh_debt', 1, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM ssdf.flow WHERE dest = 'smpl_dwh.dwh_debt');

INSERT INTO ssdf.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
SELECT 'V', 'Main', 'Main_Payment', NULL, 100,
    'smpl_dwh.vw_main_dwh_payment', 'smpl_dwh.dwh_payment', 1, 'dttm', NULL
WHERE NOT EXISTS (SELECT 1 FROM ssdf.flow WHERE dest = 'smpl_dwh.dwh_payment');

INSERT INTO ssdf.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
SELECT 'V', 'Collection', 'Collection',
    ARRAY[('smpl_dwh.dwh_collection_event', 'f')::ssdf.point, ('smpl_dwh.dwh_payment', 'f')::ssdf.point],
    100, 'smpl_dwh.vw_ams_collection_event', 'smpl_dwh.ams_collection_event', 1, 'dttm', NULL
WHERE NOT EXISTS (SELECT 1 FROM ssdf.flow WHERE dest = 'smpl_dwh.ams_collection_event');
