-- Flow definitions for the demo data flow
-- Collection process
--
-- Definitions are authoritative: clear and re-register on every run so that
-- edits take effect. RESTART IDENTITY keeps point ids stable across reinstalls.
TRUNCATE TABLE df.flow RESTART IDENTITY;

INSERT INTO df.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
SELECT 'StrtP', 'Collection', NULL, NULL, 100, 'dwh.usp_sa_wait_contact_collection_event', 'contact.collection_event', 1, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM df.flow WHERE dest = 'contact.collection_event' AND type = 'StrtP');

INSERT INTO df.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
SELECT 'StrtP', 'Collection', NULL, NULL, 100, 'dwh.usp_sa_wait_contact_debt', 'contact.debt', 1, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM df.flow WHERE dest = 'contact.debt' AND type = 'StrtP');

INSERT INTO df.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
SELECT 'P', 'Collection', 'Collection', ARRAY[('contact.collection_event', 'f')::df.point], 100,
    'dwh.usp_sa_load_contact_collection_event', 'dwh.sa_collection_event', 1, 'dt', NULL
WHERE NOT EXISTS (SELECT 1 FROM df.flow WHERE dest = 'dwh.sa_collection_event');

INSERT INTO df.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
SELECT 'P', 'Collection', 'Collection', ARRAY[('contact.debt', 'f')::df.point], 100,
    'dwh.usp_sa_load_contact_debt', 'dwh.sa_debt', 1, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM df.flow WHERE dest = 'dwh.sa_debt');

INSERT INTO df.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
SELECT 'V', 'Collection', 'Collection', ARRAY[('dwh.sa_collection_event', 'f')::df.point], 1000,
    'dwh.sa_collection_event', 'dwh.h_collection_event', 1, 'dt', NULL
WHERE NOT EXISTS (SELECT 1 FROM df.flow WHERE dest = 'dwh.h_collection_event');

INSERT INTO df.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
SELECT 'V', 'Collection', 'Collection', ARRAY[('dwh.sa_debt', 'f')::df.point], 1000,
    'dwh.sa_debt', 'dwh.h_debt', 1, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM df.flow WHERE dest = 'dwh.h_debt');

INSERT INTO df.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
SELECT 'P', 'Collection', 'Collection', ARRAY[('dwh.sa_collection_event', 'f')::df.point], 100,
    'dwh.usp_srs_collection_event', 'dwh.srs_collection_event', 1, 'dttm', NULL
WHERE NOT EXISTS (SELECT 1 FROM df.flow WHERE dest = 'dwh.srs_collection_event');

INSERT INTO df.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
SELECT 'P', 'Collection', 'Collection', ARRAY[('dwh.srs_collection_event', 'f')::df.point], 100,
    'dwh.usp_glb_collection_event', 'dwh.glb_collection_event', 1, 'dttm', NULL
WHERE NOT EXISTS (SELECT 1 FROM df.flow WHERE dest = 'dwh.glb_collection_event');

INSERT INTO df.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
SELECT 'P', 'Collection', 'Collection', ARRAY[('dwh.sa_debt', 'f')::df.point], 100,
    'dwh.usp_srs_debt', 'dwh.srs_debt', 1, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM df.flow WHERE dest = 'dwh.srs_debt');

INSERT INTO df.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
SELECT 'P', 'Collection', 'Collection', ARRAY[('dwh.srs_debt', 'f')::df.point], 100,
    'dwh.usp_glb_debt', 'dwh.glb_debt', 1, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM df.flow WHERE dest = 'dwh.glb_debt');

INSERT INTO df.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
SELECT 'V', 'Collection', 'Collection',
    ARRAY[('dwh.srs_collection_event', 'f')::df.point, ('dwh.glb_collection_event', 'f')::df.point,
          ('dwh.srs_debt', 'f')::df.point, ('dwh.glb_debt', 'f')::df.point,
          ('dwh.sa_collection_event', 'f')::df.point],
    100, 'dwh.vw_dwh_collection_event', 'dwh.dwh_collection_event', 1, 'dttm', NULL
WHERE NOT EXISTS (SELECT 1 FROM df.flow WHERE dest = 'dwh.dwh_collection_event');

INSERT INTO df.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
SELECT 'V', 'Collection', 'Collection',
    ARRAY[('dwh.srs_debt', 'f')::df.point, ('dwh.glb_debt', 'f')::df.point,
          ('dwh.sa_debt', 'f')::df.point],
    100, 'dwh.vw_dwh_debt', 'dwh.dwh_debt', 1, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM df.flow WHERE dest = 'dwh.dwh_debt');

INSERT INTO df.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
SELECT 'V', 'Main', 'Main_Payment', NULL, 100,
    'dwh.vw_main_dwh_payment', 'dwh.dwh_payment', 1, 'dttm', NULL
WHERE NOT EXISTS (SELECT 1 FROM df.flow WHERE dest = 'dwh.dwh_payment');

INSERT INTO df.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
SELECT 'V', 'Collection', 'Collection',
    ARRAY[('dwh.dwh_collection_event', 'f')::df.point, ('dwh.dwh_payment', 'f')::df.point],
    100, 'dwh.vw_ams_collection_event', 'dwh.ams_collection_event', 1, 'dttm', NULL
WHERE NOT EXISTS (SELECT 1 FROM df.flow WHERE dest = 'dwh.ams_collection_event');
