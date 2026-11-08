DROP PROCEDURE IF EXISTS df.exec_id;

CREATE OR REPLACE PROCEDURE df.exec_id(
    IN _id_batch BIGINT,
    IN _id INT,
    OUT _strt_dt TIMESTAMP
)
AS $$
DECLARE
    _type CHAR(7);
    _source VARCHAR(1000);
    _stf1 VARCHAR(1000);
    _stf2 CHAR(2);
    _dest df.point;
    _cond_col TEXT;
    _horizon TEXT;
    _dest_id_batch BOOLEAN := FALSE;
    _dest_LoadDate BOOLEAN := FALSE;
    _dest_UPDATE_DTTM BOOLEAN := FALSE;
    _dest_REP_DATE BOOLEAN := FALSE;
    _rep_date TIMESTAMP;
    _cmd TEXT;
BEGIN
    DROP TABLE IF EXISTS clmn;

    SELECT "type", "source", dest, dest_tier_f, cond_col, horizon
    INTO _type, _source, _stf1, _stf2, _cond_col, _horizon
    FROM df.vw_flow
    WHERE id = _id;

    _dest := (_stf1, _stf2)::df.point;

    SELECT TRUE INTO _dest_id_batch
    FROM information_schema.columns
    WHERE table_name = split_part(_dest."name", '.', -1)
      AND table_schema = split_part(_dest."name", '.', -2)
      AND column_name = 'ID_Batch';

    SELECT TRUE INTO _dest_LoadDate
    FROM information_schema.columns
    WHERE table_name = split_part(_dest."name", '.', -1)
      AND table_schema = split_part(_dest."name", '.', -2)
      AND column_name = 'LoadDate';

    SELECT TRUE INTO _dest_UPDATE_DTTM
    FROM information_schema.columns
    WHERE table_name = split_part(_dest."name", '.', -1)
      AND table_schema = split_part(_dest."name", '.', -2)
      AND column_name = 'UPDATE_DTTM';

    SELECT TRUE INTO _dest_REP_DATE
    FROM information_schema.columns
    WHERE table_name = split_part(_dest."name", '.', -1)
      AND table_schema = split_part(_dest."name", '.', -2)
      AND column_name = 'REP_DATE';

    _rep_date := (SELECT MIN(dttm) FROM df.flow_rep_dttm_clc c WHERE c.id_batch = _id_batch AND c.p = _dest);

    IF _type = 'StrtP' THEN
        _cmd := FORMAT('call %s(%s, %s, (''%s'', ''%s'')::df.point)', _source, _id_batch, _id, _dest.name, _dest.tier);
    ELSIF _type = 'P' THEN
        _cmd := FORMAT('call %s(%s)', _source, _id_batch);
    ELSIF _type = 'V' THEN
        CREATE TEMP TABLE clmn AS
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = split_part(_source, '.', -1)
          AND table_schema = split_part(_source, '.', -2)
          AND column_name NOT IN ('id_batch', 'loaddate', 'update_dttm', 'rep_date')
        INTERSECT
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = split_part(_dest."name", '.', -1)
          AND table_schema = split_part(_dest."name", '.', -2)
          AND column_name NOT IN ('id_batch', 'loaddate', 'update_dttm', 'rep_date');

        _cmd := FORMAT(
            'delete from %s where %s between %s and %s;',
            _dest."name",
            _cond_col,
            (SELECT '''' || COALESCE(dttm, '1900-01-01')::TEXT || '''' FROM df.horizon WHERE "name" = _horizon),
            (SELECT '''' || COALESCE(dttm_end, '2220-01-01')::TEXT || '''' FROM df.horizon WHERE "name" = _horizon)
        ) || E'\n\n' || FORMAT(
            'insert into %s(%s%s%s%s%s)\nselect %s%s%s%s%s\nfrom %s\nwhere %s between %s and %s;',
            _dest."name",
            CASE WHEN _dest_id_batch THEN 'id_batch, ' ELSE '' END,
            CASE WHEN _dest_loadDate THEN 'LoadDate, ' ELSE '' END,
            CASE WHEN _dest_UPDATE_DTTM THEN 'UPDATE_DTTM, ' ELSE '' END,
            CASE WHEN _dest_REP_DATE THEN 'REP_DATE, ' ELSE '' END,
            (SELECT STRING_AGG(column_name, ', ' ORDER BY column_name) FROM clmn),
            CASE WHEN _dest_id_batch THEN _id_batch::TEXT || ', ' ELSE '' END,
            CASE WHEN _dest_loadDate THEN '''' || CURRENT_TIMESTAMP::TEXT || ''', ' ELSE '' END,
            CASE WHEN _dest_UPDATE_DTTM THEN '''' || CURRENT_TIMESTAMP::TEXT || ''', ' ELSE '' END,
            CASE WHEN _dest_REP_DATE THEN '''' || _rep_date::TEXT || ''', ' ELSE '' END,
            (SELECT STRING_AGG(column_name, ', ' ORDER BY column_name) FROM clmn),
            _source,
            _cond_col,
            (SELECT '''' || COALESCE(dttm, '1900-01-01')::TEXT || '''' FROM df.horizon WHERE "name" = _horizon),
            (SELECT '''' || COALESCE(dttm_end, '2220-01-01')::TEXT || '''' FROM df.horizon WHERE "name" = _horizon)
        );
    ELSE
        _cmd := 'select pg_sleep(random()*(10-1)+1);';
    END IF;

    INSERT INTO df.exec_log (id_batch, id, startdate, executed, command)
    SELECT _id_batch, _id, CURRENT_TIMESTAMP, 'running', _cmd
    RETURNING CURRENT_TIMESTAMP INTO _strt_dt;

    DECLARE
        _rowcount BIGINT;
    BEGIN
        EXECUTE _cmd;
        GET DIAGNOSTICS _rowcount := ROW_COUNT;

        UPDATE df.exec_log
        SET executed = 'done',
            enddate = CURRENT_TIMESTAMP,
            row_count = _rowcount
        WHERE id_batch = _id_batch AND id = _id AND startdate = _strt_dt;

    EXCEPTION
        WHEN OTHERS THEN
            UPDATE df.exec_log
            SET executed = 'error',
                _SQLSTATE = SQLSTATE,
                _SQLERRM = SQLERRM
            WHERE id_batch = _id_batch AND id = _id AND startdate = _strt_dt;
    END;

    IF _type <> 'StrtP' THEN
        INSERT INTO df.flow_rep_dttm_clc (id_batch, p, id, dttm, rep_id)
        SELECT _id_batch, _dest, _id,
            (SELECT MIN(dttm) FROM df.flow_rep_dttm r WHERE p IN (SELECT UNNEST(deps) FROM df.flow f WHERE f.id = _id)),
            NULL;
    END IF;

    IF (SELECT COUNT(*) FROM df.flow_rep_dttm_clc WHERE id_batch = _id_batch AND p = _dest)
       = (SELECT COUNT(*) FROM df.vw_flow WHERE (dest, dest_tier_f)::df.point = _dest) THEN
        UPDATE df.flow_rep_dttm f
        SET dttm = _rep_date
        WHERE p = _dest;
    END IF;

    DROP TABLE IF EXISTS clmn;
END;
$$ LANGUAGE plpgsql;
