DROP TABLE IF EXISTS ssdf.flow_rep_dttm_clc CASCADE;

-- Per-batch contributions of points to destination stages:
-- multi-tier readiness of a destination is derived from these rows.
CREATE TABLE ssdf.flow_rep_dttm_clc (
    id_batch bigint,
    p ssdf.point NOT NULL,
    id int,
    dttm timestamp,
    rep_id bigint,
    CONSTRAINT choose CHECK (dttm IS NULL OR rep_id IS NULL)
);
