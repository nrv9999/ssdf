DROP TYPE IF EXISTS ssdf.point CASCADE;

CREATE TYPE ssdf.point AS (
    name varchar(100),
    tier char(3)
);
