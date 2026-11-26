DROP TABLE IF EXISTS smpl_dwh.h_collection_event CASCADE;

CREATE TABLE smpl_dwh.h_collection_event (
    id_batch BIGINT NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    UPDATE_DTTM TIMESTAMP NOT NULL,
    REP_DATE TIMESTAMP NOT NULL,
    id BIGINT NOT NULL,
    dt TIMESTAMP NOT NULL,
    comment TEXT,
    id_debt BIGINT NOT NULL
);
