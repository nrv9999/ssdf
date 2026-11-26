DROP TABLE IF EXISTS smpl_dwh.glb_debt CASCADE;

CREATE TABLE smpl_dwh.glb_debt (
    id_batch BIGINT NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    RK BIGINT NOT NULL,
    source_id BIGINT NOT NULL,
    source_cd VARCHAR(7) NOT NULL
);
