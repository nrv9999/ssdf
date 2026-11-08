DROP PROCEDURE IF EXISTS df.exec_batch_from_id;

CREATE OR REPLACE PROCEDURE df.exec_batch_from_id(
    IN _id_batch BIGINT,
    IN _id INT,
    IN _mode VARCHAR(100),
    IN _use_rep_dttm BOOLEAN
)
AS $$
<<local>>
DECLARE
    _v INT;
    _i INT := 0;
    _curid INT;
    r RECORD;
    _stf TEXT;
    _stfi BIGINT;
    _lockhandle VARCHAR;
    _strt_dt TIMESTAMP;
BEGIN
    INSERT INTO df.dbg
    SELECT _id_batch, _id, CURRENT_TIMESTAMP::TEXT || ' start';

    CALL dbms_lock.allocate_unique(lockname => 'ssdf_exec_batch_from_id_lock', lockhandle => _lockhandle);

    IF (SELECT dbms_lock.request(_lockhandle)) <> 0 THEN
        RAISE EXCEPTION 'lock failed for ssdf_exec_batch_from_id';
    END IF;

    INSERT INTO df.dbg
    SELECT _id_batch, _id, CURRENT_TIMESTAMP::TEXT || ' lock';

    DROP TABLE IF EXISTS fb;
    DROP TABLE IF EXISTS t;
    DROP TABLE IF EXISTS t2;
    DROP TABLE IF EXISTS t3;

    DECLARE
        _rowcount BIGINT;
    BEGIN
        CALL df.exec_id(_id_batch, _id, _strt_dt);

        UPDATE df.batch_log AS b
        SET executed = l.executed
        FROM df.exec_log l
        WHERE l.id_batch = _id_batch AND l.id = _id AND l.startdate = _strt_dt
          AND b.id_batch = l.id_batch AND b.id = l.id;

        IF (SELECT executed FROM df.batch_log WHERE id_batch = _id_batch AND id = _id) = 'error' THEN
            CREATE TEMP TABLE t2 (id INT);

            LOOP
                INSERT INTO t2
                SELECT f2.id
                FROM (
                    SELECT ARRAY_AGG((f.dest, f.dest_tier_f)::df.point) AS dests
                    FROM (
                        SELECT *, COUNT(*) OVER (PARTITION BY dest, dest_tier_f) AS eff_cnt
                        FROM f
                    ) f
                    WHERE (_dests IS NULL OR NOT (ARRAY[(f.dest, f.dest_tier_f)::df.point] <@ _excls))
                        AND (_mode = 'full' OR (_mode = 'strict' AND f.eff_cnt =
                            (SELECT cnt FROM (
                                SELECT *, COUNT(*) OVER (PARTITION BY dest, dest_tier_f) AS cnt
                                FROM df.vw_flow_1 fr
                            ) fr WHERE fr.id = f.id)))
                ) fa
                JOIN df.vw_flow_1 f2 ON f2.deps && fa.dests
                WHERE f2.id NOT IN (SELECT id FROM f)
                  AND NOT EXISTS (SELECT 1 FROM t2 WHERE t2.id = f2.id)
                  AND (
                      (_mode = 'full' AND (
                          SELECT ARRAY_AGG(p)
                          FROM (
                              SELECT UNNEST(fa.dests) AS p
                              UNION ALL
                              (SELECT p FROM df.flow_rep_dttm rd WHERE _use_rep_dttm AND rd.dttm
                               <= (SELECT dttm FROM df.flow_rep_dttm rd2 WHERE rd2.p = (f2.dest, f2.dest_tier_f)::df.point))
                          ) sub
                      ) @> f2.deps)
                      OR (_mode = 'strict' AND EXISTS (
                          (SELECT UNNEST(fa.dests)
                           UNION ALL
                           (SELECT p FROM df.flow_rep_dttm rd WHERE _use_rep_dttm AND rd.dttm
                            > (SELECT dttm FROM df.flow_rep_dttm rd2 WHERE rd2.p = (f2.dest, f2.dest_tier_f)::df.point)))
                          INTERSECT
                          SELECT UNNEST(f2.deps)
                      ))
                  );

                GET DIAGNOSTICS _rowcount := ROW_COUNT;
                EXIT WHEN _rowcount = 0;
            END LOOP;

            UPDATE df.batch_log AS b
            SET enabled = FALSE
            WHERE id_batch = _id_batch AND id IN (SELECT id FROM t2);

            DROP TABLE IF EXISTS t2;
        END IF;
    END;

    IF NOT EXISTS (
        SELECT 1 FROM df.batch_log b2
        WHERE b2.id_batch = _id_batch
          AND (b2.executed IS NULL OR b2.executed = 'running')
          AND b2.enabled = TRUE
          AND vw_ordr = (SELECT vw_ordr FROM df.batch_log b WHERE b.id_batch = _id_batch AND b.id = _id)
    ) THEN
        SELECT MIN(vw_ordr) INTO _v
        FROM df.batch_log b2
        WHERE b2.id_batch = _id_batch
          AND b2.executed IS NULL AND b2.enabled = TRUE
          AND vw_ordr > (SELECT vw_ordr FROM df.batch_log b WHERE b.id_batch = _id_batch AND b.id = _id);

        IF _v IS NULL THEN
            SELECT vw_ordr INTO _v
            FROM df.batch_log b WHERE b.id_batch = _id_batch AND b.id = _id;
        END IF;

        CREATE TEMP TABLE fb AS
        SELECT * FROM df.batch_log b
        WHERE b.id_batch = _id_batch
          AND b.executed IS NULL AND b.enabled = TRUE AND b.vw_ordr <= _v;

        CREATE TEMP TABLE t AS
        SELECT *
        FROM (
            SELECT id FROM fb ORDER BY fb.vw_ordr, fb.priority LIMIT 1
        ) sub1
        UNION ALL
        SELECT fb2.id
        FROM (
            SELECT * FROM fb ORDER BY fb.vw_ordr, fb.priority LIMIT 1
        ) fb
        JOIN fb fb2 ON fb2.id <> fb.id AND fb2.vw_ordr <= fb.vw_ordr_max;

        DELETE FROM fb
        WHERE id NOT IN (SELECT id FROM t);

        LOOP
            _curid := NULL;
            SELECT id INTO _curid FROM fb b ORDER BY priority, vw_ordr, id LIMIT 1 OFFSET _i;

            IF _curid IS NULL THEN
                EXIT;
            END IF;

            IF df.get_for_resource(_curid) THEN
                INSERT INTO df.dbg
                SELECT _id_batch, _id, CURRENT_TIMESTAMP::TEXT || ' first ' || _curid::TEXT;

                SELECT schedule INTO _stfi
                FROM dblink('ssdf_postgres_conn',
                    'SELECT cron.schedule_in_database(''ssdf_'' || df.find_free_job(_id_batch, _curid)::varchar(5) || ''''',
                        ''' || EXTRACT(minute FROM CURRENT_TIMESTAMP + INTERVAL ''2 minute'')::TEXT || '' ''
                        || EXTRACT(hour FROM CURRENT_TIMESTAMP + INTERVAL ''2 minute'')::TEXT
                        || '' * * *'',
                        ''call df.exec_batch_from_id(' || _id_batch::VARCHAR(10) || ', ' || _curid::VARCHAR(10) || '', ''''' || _mode || ''''', ' || _use_rep_dttm::VARCHAR(10) || '')'',
                        ''demo'');')
                    AS t(schedule BIGINT);
            END IF;
            _i := _i + 1;
        END LOOP;

        _curid := NULL;
        SELECT id INTO _curid FROM fb b ORDER BY priority, vw_ordr, id LIMIT 1;

        IF _curid IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM df.batch_log b WHERE b.id_batch = _id_batch AND executed = 'running'
        ) THEN
            CALL df.wait_for_resource(_curid);

            INSERT INTO df.dbg
            SELECT _id_batch, _id, CURRENT_TIMESTAMP::TEXT || ' second ' || _curid::TEXT;

            SELECT schedule INTO _stfi
            FROM dblink('ssdf_postgres_conn',
                'SELECT cron.schedule_in_database(''ssdf_'' || df.find_free_job(_id_batch, _curid)::varchar(5) || ''''',
                    ''' || EXTRACT(minute FROM CURRENT_TIMESTAMP + INTERVAL ''2 minute'')::TEXT || '' ''
                    || EXTRACT(hour FROM CURRENT_TIMESTAMP + INTERVAL ''2 minute'')::TEXT
                    || '' * * *'',
                    ''call df.exec_batch_from_id(' || _id_batch::VARCHAR(10) || ', ' || _curid::VARCHAR(10) || '', ''''' || _mode || ''''', ' || _use_rep_dttm::VARCHAR(10) || '')'',
                    ''demo'');')
                AS t(schedule BIGINT);
        END IF;
    END IF;

    DROP TABLE IF EXISTS fb;
    DROP TABLE IF EXISTS t;
    DROP TABLE IF EXISTS t2;
    DROP TABLE IF EXISTS t3;

    PERFORM dbms_lock.release(lockhandle => _lockhandle);

    INSERT INTO df.dbg
    SELECT _id_batch, _id, CURRENT_TIMESTAMP::TEXT || ' end';
END;
$$ LANGUAGE plpgsql;
