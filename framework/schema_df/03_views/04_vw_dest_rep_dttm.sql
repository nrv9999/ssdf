DROP VIEW IF EXISTS df.vw_dest_rep_dttm CASCADE;

-- Watermark of a destination stage: min over its building points,
-- known only when every builder has reported a rep date.
CREATE OR REPLACE VIEW df.vw_dest_rep_dttm AS
SELECT
    (f.dest, f.dest_tier_f)::df.point AS p,
    CASE WHEN bool_and(r.dttm IS NOT NULL) THEN min(r.dttm) END AS dttm
FROM df.vw_flow_1 f
LEFT JOIN df.flow_rep_dttm r ON r.id = f.id
GROUP BY f.dest, f.dest_tier_f;
