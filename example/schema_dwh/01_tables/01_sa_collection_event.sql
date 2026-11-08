DROP TABLE IF EXISTS dwh.sa_collection_event CASCADE;

CREATE TABLE dwh.sa_collection_event (
    id_batch BIGINT NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    REP_DATE TIMESTAMP NOT NULL,
    id BIGINT NOT NULL,
    dt TIMESTAMP NOT NULL,
    comment TEXT,
    id_debt BIGINT NOT NULL
);
