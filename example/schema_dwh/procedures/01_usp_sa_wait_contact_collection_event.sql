DROP PROCEDURE IF EXISTS dwh.usp_sa_wait_contact_collection_event;

-- Starting point of the flow: waits for the external source
-- contact.collection_event to become available. The demo has no real source, so
-- there is nothing to wait for and the body is empty.
--
-- Unlike before, this procedure does not write to df.flow_rep_dttm_clc: the rep
-- date is now derived by df.exec_id from the data actually loaded, so user
-- procedures never touch df.* system tables.
CREATE OR REPLACE PROCEDURE dwh.usp_sa_wait_contact_collection_event(
    IN _id_batch BIGINT,
    IN _id INT,
    IN _p df.point
)
AS $$
BEGIN
END;
$$ LANGUAGE plpgsql;
