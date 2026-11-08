DROP TABLE IF EXISTS df.batch_log CASCADE;

CREATE TABLE df.batch_log (
    id_batch int8 NOT NULL,
    id int4 NOT NULL,
    vw_ordr smallint NOT NULL,
    vw_ordr_max smallint NOT NULL,
    priority smallint NOT NULL,
    loaddate timestamp NOT NULL,
    executed df.exec_status DEFAULT NULL,
    enabled bool DEFAULT true,
    job smallint DEFAULT NULL,
    startdate timestamp DEFAULT NULL,
    _sqlstate text DEFAULT NULL,
    _sqlerrm text DEFAULT NULL
);
