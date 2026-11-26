DROP TABLE IF EXISTS ssdf.flow_rep_dttm CASCADE;

-- One row per flow point (id = ssdf.flow.id): rep date up to which the point is loaded.
CREATE TABLE ssdf.flow_rep_dttm (
    id int NOT NULL PRIMARY KEY,
    dttm timestamp,
    rep_id bigint,
    id_batch bigint,
    CONSTRAINT choose CHECK (dttm IS NULL OR rep_id IS NULL)
);
