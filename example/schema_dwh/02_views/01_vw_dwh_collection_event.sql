DROP VIEW IF EXISTS dwh.vw_dwh_collection_event CASCADE;

CREATE VIEW dwh.vw_dwh_collection_event AS
SELECT
    gc.RK,
    gd.RK AS DEBT_RK,
    sa.DT AS DTTM,
    sa.comment AS comm
FROM dwh.sa_collection_event sa
JOIN dwh.srs_debt sd ON sd.source_id = sa.id_debt
JOIN dwh.glb_debt gd ON gd.source_cd = 'CONT' AND gd.source_id = sd.id
JOIN dwh.srs_collection_event sc ON sc.source_id = sa.id
JOIN dwh.glb_collection_event gc ON gc.source_cd = 'CONT' AND gc.source_id = sc.id;
