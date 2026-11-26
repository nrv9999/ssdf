DROP TABLE IF EXISTS ssdf.horizon CASCADE;

CREATE TABLE ssdf.horizon (
    name varchar(100) NOT NULL PRIMARY KEY,
    dttm timestamp,
    dttm_end timestamp,
    dttm_type varchar(100),  -- REP_DTTM, DTTM
    id bigint,
    id_end bigint
);
