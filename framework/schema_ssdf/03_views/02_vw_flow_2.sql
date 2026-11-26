DROP VIEW IF EXISTS ssdf.vw_flow_2 CASCADE;

CREATE OR REPLACE VIEW ssdf.vw_flow_2 AS
WITH RECURSIVE r AS (
    SELECT *,
        NULL::ssdf.point AS dep,
        1 AS vw_ordr
    FROM ssdf.vw_flow_1
    WHERE deps IS NULL

    UNION ALL

    SELECT f.*,
        r.vw_ordr + 1 AS vw_ordr
    FROM (
        SELECT f2.*, UNNEST(f2.deps) AS dep
        FROM ssdf.vw_flow_1 f2
    ) f
    JOIN r ON f.dep = (r.dest, r.dest_tier_f)::ssdf.point
)
SELECT
    id, type, process, horizon,
    ARRAY_AGG(r.dep) AS deps,
    priority, source, dest, dest_tier,
    cond_col, rep_dttm_col, dest_tier_f,
    MAX(vw_ordr) AS vw_ordr
FROM r
GROUP BY id, type, process, horizon, priority, source, dest, dest_tier,
         cond_col, rep_dttm_col, dest_tier_f
ORDER BY vw_ordr, priority, id;
