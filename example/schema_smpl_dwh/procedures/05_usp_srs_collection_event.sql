DROP PROCEDURE IF EXISTS smpl_dwh.usp_srs_collection_event;

CREATE OR REPLACE PROCEDURE smpl_dwh.usp_srs_collection_event(IN _id_batch BIGINT)
AS $$
BEGIN
    INSERT INTO smpl_dwh.srs_collection_event (id_batch, LoadDate, source_id)
    SELECT DISTINCT _id_batch, CURRENT_TIMESTAMP, id
    FROM smpl_dwh.sa_collection_event sa
    WHERE NOT EXISTS (
        SELECT 1 FROM smpl_dwh.srs_collection_event s WHERE s.source_id = sa.id
    );
END;
$$ LANGUAGE plpgsql;
