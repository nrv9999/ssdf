DROP TABLE IF EXISTS df.flow_rep_dttm CASCADE;

-- One row per flow point (id = df.flow.id): rep date up to which the point is loaded.
CREATE TABLE df.flow_rep_dttm (
    id int NOT NULL PRIMARY KEY,
    dttm timestamp,
    rep_id bigint,
    id_batch bigint,
    CONSTRAINT choose CHECK (dttm IS NULL OR rep_id IS NULL)
);
