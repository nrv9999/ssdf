DROP PROCEDURE IF EXISTS smpl_dwh.usp_sa_load_contact_collection_event;

-- Stands in for the external source table contact.collection_event, which in a
-- real deployment lives in a third-party database. Instead of transferring rows
-- it waits a second and fabricates one, so the flow point behaves like a real
-- load and the downstream points have something to work with.
--
-- There is no real source, so the current timestamp serves as both the event
-- date (dt) and the reporting date (REP_DATE) stored in the sa-table.
--
-- The ids are taken from the history tables as a workaround for this example
-- only; in a real case they arrive from the source, so no extra flow
-- dependencies are declared for them.
--
-- The point's rep date is not derived from the loaded rows: ssdf.exec_id takes it
-- as the min of the deps' watermarks (here - the rep date returned by the StrtF
-- point smpl_dwh.uf_sa_wait_contact_collection_event). User procedures never write to
-- ssdf.* system tables themselves.
CREATE OR REPLACE PROCEDURE smpl_dwh.usp_sa_load_contact_collection_event(
    IN _id_batch BIGINT
)
AS $$
DECLARE
    _dttm timestamp := CURRENT_TIMESTAMP;
BEGIN
    PERFORM pg_sleep(1);

    INSERT INTO smpl_dwh.sa_collection_event (id_batch, LoadDate, REP_DATE, id, dt, comment, id_debt)
    SELECT _id_batch
        , _dttm
        , _dttm
        , coalesce((SELECT max(id) FROM smpl_dwh.h_collection_event), 1)
        , _dttm
        , 'demo collection event'
        , coalesce((SELECT id FROM smpl_dwh.h_debt LIMIT 1), 1);
END;
$$ LANGUAGE plpgsql;
