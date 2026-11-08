DROP TABLE IF EXISTS df.flow_rep_dttm_clc CASCADE;

CREATE TABLE df.flow_rep_dttm_clc (
    id_batch bigint,
    p df.point NOT NULL,
    id int,
    dttm timestamp,
    rep_id bigint,
    CONSTRAINT choose CHECK (dttm IS NULL OR rep_id IS NULL)
);
