DROP TABLE IF EXISTS df.flow CASCADE;

CREATE TABLE df.flow (
    id int NOT NULL GENERATED ALWAYS AS IDENTITY,
    type char(7) NOT NULL,
    process varchar(100) NOT NULL,
    horizon varchar(100) REFERENCES df.horizon(name),
    deps df.point[],
    priority smallint NOT NULL,
    source varchar(100) NOT NULL,
    dest varchar(100) NOT NULL,
    dest_tier smallint NOT NULL,
    cond_col varchar(100),
    rep_dttm_col varchar(100)
);
