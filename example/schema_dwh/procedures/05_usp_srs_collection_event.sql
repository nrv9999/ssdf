DROP PROCEDURE IF EXISTS dwh.usp_srs_collection_event;

CREATE OR REPLACE PROCEDURE dwh.usp_srs_collection_event(IN _id_batch BIGINT)
AS $$
BEGIN
    INSERT INTO dwh.srs_collection_event (id_batch, LoadDate, source_id)
    SELECT DISTINCT _id_batch, CURRENT_TIMESTAMP, id
    FROM dwh.sa_collection_event sa
    WHERE NOT EXISTS (
        SELECT 1 FROM dwh.srs_collection_event s WHERE s.source_id = sa.id
    );
END;
$$ LANGUAGE plpgsql;
