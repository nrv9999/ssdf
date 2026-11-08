DROP TABLE IF EXISTS df.flow_rep_dttm CASCADE;

CREATE TABLE df.flow_rep_dttm (
    p df.point NOT NULL,
    dttm timestamp,
    rep_id bigint,
    CONSTRAINT choose CHECK (dttm IS NULL OR rep_id IS NULL)
);
