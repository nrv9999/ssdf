-- Horizon definitions for the demo data flow
INSERT INTO df.horizon (name, dttm, dttm_end, dttm_type, id, id_end)
SELECT 'Collection', '2025-01-01', NULL, 'DTTM', NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM df.horizon WHERE name = 'Collection');

INSERT INTO df.horizon (name, dttm, dttm_end, dttm_type, id, id_end)
SELECT 'Main_Payment', '2025-01-01', NULL, 'DTTM', NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM df.horizon WHERE name = 'Main_Payment');
