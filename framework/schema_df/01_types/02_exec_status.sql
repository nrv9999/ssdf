DROP TYPE IF EXISTS df.exec_status CASCADE;

CREATE TYPE df.exec_status AS ENUM ('done', 'error', 'running');
