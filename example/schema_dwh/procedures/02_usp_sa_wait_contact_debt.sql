DROP PROCEDURE IF EXISTS dwh.usp_sa_wait_contact_debt;

CREATE OR REPLACE PROCEDURE dwh.usp_sa_wait_contact_debt(
    IN _id_batch BIGINT,
    IN _id INT,
    IN _p df.point
)
AS $$
BEGIN
    INSERT INTO df.flow_rep_dttm_clc (id_batch, p, id, dttm, rep_id)
    SELECT _id_batch, _p, _id, CURRENT_TIMESTAMP, NULL;
END;
$$ LANGUAGE plpgsql;
