DROP TYPE IF EXISTS df.point CASCADE;

CREATE TYPE df.point AS (
    name varchar(100),
    tier char(3)
);
