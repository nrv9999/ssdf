DROP VIEW IF EXISTS df.vw_flow_1 CASCADE;

CREATE OR REPLACE VIEW df.vw_flow_1 AS
SELECT *,
    CASE
        WHEN dense_rank() OVER (PARTITION BY dest ORDER BY dest_tier DESC) = 1 THEN 'f'
        ELSE dest_tier::char(2)
    END AS dest_tier_f
FROM df.flow;
