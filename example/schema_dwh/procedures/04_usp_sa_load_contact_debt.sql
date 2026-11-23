DROP PROCEDURE IF EXISTS dwh.usp_sa_load_contact_debt;

-- Stands in for the external source table contact.debt, which in a real
-- deployment lives in a third-party database. Instead of transferring rows it
-- waits a second and fabricates one, so the flow point behaves like a real load
-- and the downstream points have something to work with.
--
-- There is no real source, so the current timestamp serves as both the event
-- date (dt) and the reporting date (REP_DATE) stored in the sa-table.
--
-- The id is taken from the history table as a workaround for this example only;
-- in a real case it arrives from the source, so no extra flow dependency is
-- declared for it.
--
-- The point's rep date is not derived from the loaded rows: df.exec_id takes it
-- as the min of the deps' watermarks (here - the rep date returned by the StrtF
-- point dwh.uf_sa_wait_contact_debt). User procedures never write to df.* system
-- tables themselves.
CREATE OR REPLACE PROCEDURE dwh.usp_sa_load_contact_debt(
    IN _id_batch BIGINT
)
AS $$
DECLARE
    _dttm timestamp := CURRENT_TIMESTAMP;
BEGIN
    PERFORM pg_sleep(1);

    INSERT INTO dwh.sa_debt (id_batch, LoadDate, REP_DATE, id, dt, comment, amount)
    SELECT _id_batch
        , _dttm
        , _dttm
        , coalesce((SELECT max(id) FROM dwh.h_debt), 1)
        , _dttm
        , 'demo debt'
        , 1000.00;
END;
$$ LANGUAGE plpgsql;
