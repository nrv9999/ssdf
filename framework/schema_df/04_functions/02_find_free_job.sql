CREATE OR REPLACE FUNCTION df.find_free_job(_id_batch BIGINT, _id INT)
RETURNS SMALLINT
AS $$
DECLARE
    u INT;
    _stf TEXT;
    _stfi BIGINT;
BEGIN
    DROP TABLE IF EXISTS m;

    IF NOT COALESCE(array['ssdf_postgres_conn'::TEXT] <@ dblink_get_connections(), FALSE) THEN
        SELECT dblink_connect('ssdf_postgres_conn', 'dbname=postgres') INTO _stf;
    END IF;

    SELECT MAX(num) INTO u
    FROM dblink('ssdf_postgres_conn',
        'SELECT split_part(j.jobname, ''_'', 2)::smallint FROM cron.job j WHERE j.jobname LIKE ''ssdf%'';')
    AS t(num SMALLINT);

    CREATE TEMP TABLE m AS
    SELECT g.*
    FROM generate_series(1, u) g(num)
    LEFT JOIN dblink('ssdf_postgres_conn',
        'SELECT split_part(j.jobname, ''_'', 2)::smallint FROM cron.job j WHERE j.jobname LIKE ''ssdf%'';')
        AS t(num SMALLINT)
        ON t.num = g.num
    WHERE t.num IS NULL;

    FOR u IN (SELECT num FROM m)
    LOOP
        SELECT schedule INTO _stfi
        FROM dblink('ssdf_postgres_conn',
            'SELECT cron.schedule_in_database(''ssdf_'' || u::varchar(5) || ''''', ''0 0 * * *'', ''select pg_sleep(1);'', ''demo'');')
            AS t(schedule BIGINT);
    END LOOP;

    u = NULL;

    SELECT num INTO u
    FROM dblink('ssdf_postgres_conn',
        'SELECT split_part(j.jobname, ''_'', 2)::smallint FROM cron.job j
         LEFT JOIN cron.job_run_details r ON r.jobid = j.jobid AND r.status = ''running''
         WHERE j.jobname LIKE ''ssdf%'' AND r.jobid IS NULL;')
        AS t(num SMALLINT)
    LEFT JOIN df.batch_log b ON b.executed = 'running' AND b.job = t.num
    WHERE b.id_batch IS NULL
    ORDER BY 1
    LIMIT 1;

    IF u IS NULL THEN
        SELECT num + 1 INTO u
        FROM dblink('ssdf_postgres_conn',
            'SELECT MAX(split_part(j.jobname, ''_'', 2)::smallint) FROM cron.job j WHERE j.jobname LIKE ''ssdf%'';')
            AS t(num SMALLINT);
    END IF;

    IF u IS NULL THEN
        u = 1;
    END IF;

    UPDATE df.batch_log
    SET job = u,
        executed = 'running',
        startdate = CURRENT_TIMESTAMP
    WHERE id_batch = _id_batch AND id = _id;

    DROP TABLE IF EXISTS m;

    RETURN u;
END;
$$ LANGUAGE plpgsql;
