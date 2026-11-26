DROP TABLE IF EXISTS smpl_dwh.dwh_collection_event CASCADE;

CREATE TABLE smpl_dwh.dwh_collection_event (
    id_batch BIGINT NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    REP_DATE TIMESTAMP NOT NULL,
    RK BIGINT NOT NULL,
    DEBT_RK BIGINT NOT NULL,
    DTTM TIMESTAMP NOT NULL,
    comm TEXT
);
