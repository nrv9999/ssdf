DROP TABLE IF EXISTS ssdf.flow CASCADE;

CREATE TABLE ssdf.flow (
    id int NOT NULL GENERATED ALWAYS AS IDENTITY,
    type char(7) NOT NULL,
    process varchar(100) NOT NULL,
    horizon varchar(100) REFERENCES ssdf.horizon(name),
    deps ssdf.point[],
    priority smallint NOT NULL,
    source varchar(100) NOT NULL,
    dest varchar(100) NOT NULL,
    dest_tier smallint NOT NULL,
    cond_col varchar(100),
    rep_dttm_col varchar(100)
);
