DROP PROCEDURE IF EXISTS df.exec_flow;

CREATE OR REPLACE PROCEDURE df.exec_flow(
    IN _mode VARCHAR(100),
    IN _procs VARCHAR(100)[] DEFAULT NULL,
    IN _start_ids INT[] DEFAULT NULL,
    IN _dests df.point[] DEFAULT NULL,
    IN _excls df.point[] DEFAULT NULL,
    IN _use_rep_dttm BOOLEAN DEFAULT TRUE
)
AS $$
DECLARE
    _r RECORD;
    _i INT := 1;
    _rowcount INT;
    _id_batch BIGINT;
    _stf TEXT;
    _stfi BIGINT;
BEGIN
    IF _mode NOT IN ('full', 'strict') THEN
        RAISE EXCEPTION 'mode could be only full and strict';
    END IF;

    DROP TABLE IF EXISTS f CASCADE;
    DROP TABLE IF EXISTS f2 CASCADE;

    -- Get start points
    CREATE TEMP TABLE f AS
    SELECT *
    FROM df.vw_flow_1
    WHERE (_procs IS NULL OR process IN (SELECT UNNEST(_procs)))
      AND ((deps IS NULL AND _start_ids IS NULL) OR id IN (SELECT UNNEST(_start_ids)))
      AND (_excls IS NULL OR ARRAY[(dest, dest_tier_f)::df.point] <@ _excls);

    LOOP
        -- Get next points
        INSERT INTO f
        SELECT f2.*
        FROM (
            SELECT ARRAY_AGG((f.dest, f.dest_tier_f)::df.point) AS dests
            FROM (
                SELECT *, COUNT(*) OVER (PARTITION BY dest, dest_tier_f) AS eff_cnt
                FROM f
            ) f
            WHERE (_dests IS NULL OR NOT (ARRAY[(f.dest, f.dest_tier_f)::df.point] <@ _dests))
                AND (_mode = 'full' OR (_mode = 'strict' AND f.eff_cnt =
                    (SELECT cnt FROM (
                        SELECT *, COUNT(*) OVER (PARTITION BY dest, dest_tier_f) AS cnt
                        FROM df.vw_flow_1 fr
                    ) fr WHERE fr.id = f.id)))
        ) fa
        JOIN df.vw_flow_1 f2 ON (
            (_mode = 'strict' AND (
                SELECT ARRAY_AGG(p)
                FROM (
                    SELECT UNNEST(fa.dests) AS p
                    UNION ALL
                    (SELECT p FROM df.flow_rep_dttm rd WHERE _use_rep_dttm AND rd.dttm
                     > (SELECT dttm FROM df.flow_rep_dttm rd2 WHERE rd2.p = (f2.dest, f2.dest_tier_f)::df.point))
                ) sub
            ) @> f2.deps)
            OR (_mode = 'full' AND EXISTS (
                (SELECT UNNEST(fa.dests)
                 UNION ALL
                 (SELECT p FROM df.flow_rep_dttm rd WHERE _use_rep_dttm AND rd.dttm
                  > (SELECT dttm FROM df.flow_rep_dttm rd2 WHERE rd2.p = (f2.dest, f2.dest_tier_f)::df.point)))
                INTERSECT
                SELECT UNNEST(f2.deps)
            ))
        )
        WHERE f2.id NOT IN (SELECT id FROM f)
          AND (_procs IS NULL OR f2."process" IN (SELECT UNNEST(_procs)))
          AND (_excls IS NULL OR ARRAY[(f2.dest, f2.dest_tier_f)::df.point] <@ _excls);

        GET DIAGNOSTICS _rowcount := ROW_COUNT;
        EXIT WHEN _rowcount = 0;
    END LOOP;

    -- Get correct priority
    CREATE TEMP TABLE f2 AS
    SELECT f.id, v.vw_ordr, v.vw_ordr_max, v.priority
    FROM f
    JOIN df.vw_flow v ON v.id = f.id;

    _id_batch := (SELECT COALESCE(MAX(d.id_batch), 0::BIGINT) FROM df.batch_log d) + 1;

    -- Generate batch's work set/log
    INSERT INTO df.batch_log (id_batch, id, vw_ordr, vw_ordr_max, priority, LoadDate)
    SELECT _id_batch, id, vw_ordr, vw_ordr_max, priority, CURRENT_TIMESTAMP
    FROM f2;

    -- Print batch
    FOR _r IN (SELECT (id, vw_ordr) FROM f2 ORDER BY vw_ordr, priority)
    LOOP
        RAISE NOTICE '(id,vw_ordr): %', _r;
    END LOOP;

    IF NOT COALESCE(array['ssdf_postgres_conn'::TEXT] <@ dblink_get_connections(), FALSE) THEN
        SELECT dblink_connect('ssdf_postgres_conn', 'dbname=postgres') INTO _stf;
    END IF;

    -- Start executing batch from start points
    FOR _r IN (SELECT id FROM f2 WHERE vw_ordr = (SELECT MIN(vw_ordr) FROM f2) ORDER BY priority, id)
    LOOP
        IF _i = 1 THEN
            CALL df.wait_for_resource(_r.id);
        ELSE
            IF NOT df.get_for_resource(_r.id) THEN
                CONTINUE;
            END IF;
        END IF;

        SELECT schedule INTO _stfi
        FROM dblink('ssdf_postgres_conn',
            'SELECT cron.schedule_in_database(''ssdf_'' || df.find_free_job(_id_batch, _r.id)::varchar(5) || ''''',
                ''' || EXTRACT(minute FROM CURRENT_TIMESTAMP + INTERVAL ''2 minute'')::TEXT || '' ''
                || EXTRACT(hour FROM CURRENT_TIMESTAMP + INTERVAL ''2 minute'')::TEXT
                || '' * * *'',
                ''call df.exec_batch_from_id(' || _id_batch::VARCHAR(10) || ', ' || _r.id::VARCHAR(10) || '', ''''' || _mode || ''''', ' || _use_rep_dttm::VARCHAR(10) || ')'',
                ''demo'');')
            AS t(schedule BIGINT);
        _i := _i + 1;
    END LOOP;

    DROP TABLE IF EXISTS f CASCADE;
    DROP TABLE IF EXISTS f2 CASCADE;

    COMMIT;
END;
$$ LANGUAGE plpgsql;
