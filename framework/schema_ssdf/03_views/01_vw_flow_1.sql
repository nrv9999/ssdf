DROP VIEW IF EXISTS ssdf.vw_flow_1 CASCADE;

CREATE OR REPLACE VIEW ssdf.vw_flow_1 AS
SELECT *,
    CASE
        WHEN dense_rank() OVER (PARTITION BY dest ORDER BY dest_tier DESC) = 1 THEN 'f'
        ELSE dest_tier::char(2)
    END AS dest_tier_f
FROM ssdf.flow;
