DROP VIEW IF EXISTS ssdf.vw_flow CASCADE;

CREATE OR REPLACE VIEW ssdf.vw_flow AS
SELECT
    r2.id, r2.type, r2.process, r2.horizon, r2.deps, r2.priority,
    r2.source, r2.dest, r2.dest_tier, r2.cond_col, r2.rep_dttm_col,
    r2.dest_tier_f,
    r2.vw_ordr,
    COALESCE(MIN(f2.vw_ordr) - 1, MAX(r2.vw_ordr) OVER ()) AS vw_ordr_max
FROM ssdf.vw_flow_2 r2
LEFT JOIN (
    SELECT f2.vw_ordr, UNNEST(f2.deps) AS dep
    FROM ssdf.vw_flow_2 f2
) f2 ON f2.dep = (r2.dest, r2.dest_tier_f)::ssdf.point
GROUP BY
    r2.id, r2.type, r2.process, r2.horizon, r2.deps, r2.priority,
    r2.source, r2.dest, r2.dest_tier, r2.cond_col, r2.rep_dttm_col,
    r2.dest_tier_f, r2.vw_ordr
ORDER BY vw_ordr, priority, id;
