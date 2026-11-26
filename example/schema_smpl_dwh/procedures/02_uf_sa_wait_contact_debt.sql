DROP FUNCTION IF EXISTS smpl_dwh.uf_sa_wait_contact_debt();

-- Starting point of the flow (StrtF): waits for the external source
-- contact.debt to become available and returns its rep date.
-- The demo has no real source, so the stub simply returns the current timestamp.
CREATE OR REPLACE FUNCTION smpl_dwh.uf_sa_wait_contact_debt()
RETURNS timestamp
AS $$
BEGIN
    RETURN CURRENT_TIMESTAMP;
END;
$$ LANGUAGE plpgsql;
