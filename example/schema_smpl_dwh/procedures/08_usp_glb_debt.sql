DROP PROCEDURE IF EXISTS smpl_dwh.usp_glb_debt;

CREATE OR REPLACE PROCEDURE smpl_dwh.usp_glb_debt(IN _id_batch BIGINT)
AS $$
DECLARE
    mx BIGINT;
BEGIN
    SELECT MAX(RK) INTO mx FROM smpl_dwh.glb_debt;

    INSERT INTO smpl_dwh.glb_debt (id_batch, LoadDate, RK, source_id, source_cd)
    SELECT DISTINCT _id_batch, CURRENT_TIMESTAMP,
        COALESCE(mx, 0) + ROW_NUMBER() OVER (), id, 'CONT'
    FROM smpl_dwh.srs_debt s
    WHERE NOT EXISTS (
        SELECT 1 FROM smpl_dwh.glb_debt g
        WHERE g.source_id = s.id AND g.source_cd = 'CONT'
    );
END;
$$ LANGUAGE plpgsql;
