DROP TABLE IF EXISTS df.horizon CASCADE;

CREATE TABLE df.horizon (
    name varchar(100) NOT NULL PRIMARY KEY,
    dttm timestamp,
    dttm_end timestamp,
    dttm_type varchar(100),  -- REP_DTTM, DTTM
    id bigint,
    id_end bigint
);
