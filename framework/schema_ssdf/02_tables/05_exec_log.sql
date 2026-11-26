DROP TABLE IF EXISTS ssdf.exec_log CASCADE;

CREATE TABLE ssdf.exec_log (
    id_batch int8 NOT NULL,
    id int4 NOT NULL,
    startdate timestamp NOT NULL,
    enddate timestamp,
    executed ssdf.exec_status NOT NULL,
    command text NOT NULL,
    row_count bigint,
    _sqlstate text,
    _sqlerrm text,
    CONSTRAINT exec_log_pkey PRIMARY KEY (id_batch, id, startdate)
);
