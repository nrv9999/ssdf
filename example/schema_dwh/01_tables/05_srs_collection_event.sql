DROP TABLE IF EXISTS dwh.srs_collection_event CASCADE;

CREATE TABLE dwh.srs_collection_event (
    id_batch BIGINT NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    source_id BIGINT NOT NULL
);
