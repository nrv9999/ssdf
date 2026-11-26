DROP TYPE IF EXISTS ssdf.exec_status CASCADE;

CREATE TYPE ssdf.exec_status AS ENUM ('done', 'error', 'running');
