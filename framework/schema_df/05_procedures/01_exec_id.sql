DROP PROCEDURE IF EXISTS df.exec_id(bigint, int);

create or replace procedure df.exec_id(in _id_batch bigint, in _id int, out _strt_dt timestamp)
as $$
declare
_type char(7);
_source varchar(1000);
_stf1 varchar(1000);
_stf2 char(2);
_dest df.point;
_cond_col text;
_horizon text;
_dest_id_batch bool = false;
_dest_LoadDate bool = false;
_dest_UPDATE_DTTM bool = false;
_dest_REP_DATE bool = false;
_rep_date timestamp;
_cmd text;
begin

drop table if exists clmn;

select "type", "source", dest, dest_tier_f, cond_col, horizon into _type, _source, _stf1, _stf2, _cond_col, _horizon from df.vw_flow where id = _id;

_dest = (_stf1, _stf2)::df.point;

SELECT true into _dest_id_batch
FROM information_schema.columns
WHERE table_name = split_part(_dest."name", '.', -1) and table_schema = split_part(_dest."name", '.', -2) and column_name = 'id_batch';

SELECT true into _dest_LoadDate
FROM information_schema.columns
WHERE table_name = split_part(_dest."name", '.', -1) and table_schema = split_part(_dest."name", '.', -2) and column_name = 'loaddate';

SELECT true into _dest_UPDATE_DTTM
FROM information_schema.columns
WHERE table_name = split_part(_dest."name", '.', -1) and table_schema = split_part(_dest."name", '.', -2) and column_name = 'update_dttm';

SELECT true into _dest_REP_DATE
FROM information_schema.columns
WHERE table_name = split_part(_dest."name", '.', -1) and table_schema = split_part(_dest."name", '.', -2) and column_name = 'rep_date';

_rep_date = (select min(dttm) from df.flow_rep_dttm_clc c where c.id_batch = _id_batch and c.p = _dest);

if _type = 'StrtP' then
	_cmd = FORMAT('call %s(%s, %s, (''%s'', ''%s'')::df.point)', _source, _id_batch, _id, _dest.name, _dest.tier);
elsif _type = 'P' then
	_cmd = FORMAT('call %s(%s)', _source, _id_batch);
ELSIF _type = 'V' then
	create temp table clmn as
	SELECT column_name
	FROM information_schema.columns
	WHERE table_name = split_part(_source, '.', -1) and table_schema = split_part(_source, '.', -2) 
		and column_name not in ('id_batch', 'loaddate', 'update_dttm', 'rep_date')
	intersect
	SELECT column_name
	FROM information_schema.columns
	WHERE table_name = split_part(_dest."name", '.', -1) and table_schema = split_part(_dest."name", '.', -2) 
		and column_name not in ('id_batch', 'loaddate', 'update_dttm', 'rep_date');

	_cmd = FORMAT('delete from %s
where %s between %s and %s;', _dest."name", _cond_col, (select '''' || coalesce(dttm, '1900-01-01')::text || '''' from df.horizon where "name" = _horizon)
	, (select '''' || coalesce(dttm_end, '2220-01-01')::text || '''' from df.horizon where "name" = _horizon)) || '

' 
		|| FORMAT('insert into %s(%s%s%s%s%s)
select %s%s%s%s%s
from %s
where %s between %s and %s;', _dest."name", case when _dest_id_batch then 'id_batch, ' else '' end, case when _dest_loadDate then 'LoadDate, ' else '' end
	, case when _dest_UPDATE_DTTM then 'UPDATE_DTTM, ' else '' end, case when _dest_REP_DATE then 'REP_DATE, ' else '' end
	, (select string_agg(column_name, ', ' order by column_name) from clmn)
	, case when _dest_id_batch then _id_batch::text || ', ' else '' end, case when _dest_loadDate then '''' || CURRENT_TIMESTAMP::text || ''', ' else '' end
	, case when _dest_UPDATE_DTTM then '''' || CURRENT_TIMESTAMP::text || ''', ' else '' end, case when _dest_REP_DATE then '''' || _rep_date::text || ''', ' else '' end
	, (select string_agg(column_name, ', ' order by column_name) from clmn), _source
	, _cond_col, (select '''' || coalesce(dttm, '1900-01-01')::text || '''' from df.horizon where "name" = _horizon)
	, (select '''' || coalesce(dttm_end, '2220-01-01')::text || '''' from df.horizon where "name" = _horizon));
else
	_cmd = 'select pg_sleep(random()*(10-1)+1);';
end if;

insert into df.exec_log (id_batch, id, startdate, executed, command)
select _id_batch, _id, CURRENT_TIMESTAMP, 'running', _cmd
RETURNING CURRENT_TIMESTAMP into _strt_dt;

declare _rowcount bigint;
BEGIN

EXECUTE _cmd;

GET DIAGNOSTICS _rowcount = ROW_COUNT;

update df.exec_log
set executed = 'done'
	, enddate = CURRENT_TIMESTAMP
	, row_count = _rowcount
where id_batch = _id_batch and id = _id and startdate = _strt_dt;

EXCEPTION
    WHEN OTHERS THEN
		update df.exec_log
		set executed = 'error'
			, _SQLSTATE = SQLSTATE
			, _SQLERRM = SQLERRM
		where id_batch = _id_batch and id = _id and startdate = _strt_dt;
END;

if _type <> 'StrtP' then
	insert into df.flow_rep_dttm_clc (id_batch, p, id, dttm, rep_id)
	select _id_batch, _dest, _id
		, (select min(dttm) from df.flow_rep_dttm r where p in (select unnest(deps) from df.flow f where f.id = _id))
		, null;
end if;

if (select count(*) from df.flow_rep_dttm_clc where id_batch = _id_batch and p = _dest)
	= (select count(*) from df.vw_flow where (dest, dest_tier_f)::df.point = _dest) then
	update df.flow_rep_dttm f
	set dttm = _rep_date
	where p = _dest;
end if;

drop table if exists clmn;

end
$$ LANGUAGE plpgsql;
