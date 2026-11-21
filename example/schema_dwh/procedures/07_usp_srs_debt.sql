DROP PROCEDURE IF EXISTS dwh.usp_srs_debt;

CREATE OR REPLACE PROCEDURE dwh.usp_srs_debt(IN _id_batch BIGINT)
AS $$
BEGIN
    INSERT INTO dwh.srs_debt (id_batch, LoadDate, source_id)
    SELECT DISTINCT _id_batch, CURRENT_TIMESTAMP, id
    FROM dwh.sa_debt sa
    WHERE NOT EXISTS (
        SELECT 1 FROM dwh.srs_debt s WHERE s.source_id = sa.id
    );
END;
$$ LANGUAGE plpgsql;
