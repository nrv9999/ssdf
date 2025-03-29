

create schema df;

create schema dwh;

drop type df.point cascade;

CREATE TYPE df.point AS (
    name varchar(100),
    tier char(3)
);

drop table if exists df.horizon cascade;

create table df.horizon (name varchar(100) not null primary key
	, dttm timestamp
	, dttm_end timestamp
	, dttm_type varchar(100) -- REP_DTTM, DTTM
	, id bigint
	, id_end bigint
	);

insert into df.horizon (name, dttm, dttm_end, dttm_type, id, id_end)
select 'Collection', '2025-01-01', null, 'DTTM', null, null;

insert into df.horizon (name, dttm, dttm_end, dttm_type, id, id_end)
select 'Main_Payment', '2025-01-01', null, 'DTTM', null, null;

drop table if exists df.flow cascade;

create table df.flow(id int not null GENERATED ALWAYS AS identity
	, type char(7) not null
	, process varchar(100) not null
	, horizon varchar(100) references df.horizon(name)
	, deps df.point[]
	, priority smallint not null
	, source varchar(100) not null
	, dest varchar(100) not null
	, dest_tier smallint not null
	, cond_col varchar(100)
	, rep_dttm_col varchar(100));

truncate table df.flow;

insert into df.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
select 'StrtP', 'Collection', null, null, 100, 'dwh.usp_sa_wait_contact_collection_event', 'contact.collection_event'
	, 1, null, null;

insert into df.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
select 'StrtP', 'Collection', null, null, 100, 'dwh.usp_sa_wait_contact_debt', 'contact.debt'
	, 1, null, null;


insert into df.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
select 'SA', 'Collection', 'Collection', array[('contact.collection_event', 'f')::df.point], 100
	, 'contact.collection_event', 'dwh.sa_collection_event', 1, 'dt', null;

insert into df.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
select 'SA', 'Collection', 'Collection', array[('contact.debt', 'f')::df.point], 100
	, 'contact.debt', 'dwh.sa_debt', 1, null, null;

insert into df.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
select 'V', 'Collection', 'Collection', array[('dwh.sa_collection_event', 'f')::df.point], 1000
	, 'dwh.sa_collection_event', 'dwh.h_collection_event', 1, 'dt', null;

insert into df.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
select 'V', 'Collection', 'Collection', array[('dwh.sa_debt', 'f')::df.point], 1000
	, 'dwh.sa_debt', 'dwh.h_debt', 1, null, null;

insert into df.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
select 'V', 'Collection', 'Collection', array[('dwh.sa_collection_event', 'f')::df.point], 100
	, 'dwh.usp_srs_collection_event', 'dwh.srs_collection_event', 1, 'dttm', null;

insert into df.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
select 'V', 'Collection', 'Collection', array[('dwh.srs_collection_event', 'f')::df.point], 100
	, 'dwh.usp_glb_collection_event', 'dwh.glb_collection_event', 1, 'dttm', null;

insert into df.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
select 'V', 'Collection', 'Collection', array[('dwh.sa_debt', 'f')::df.point], 100
	, 'dwh.usp_srs_debt', 'dwh.srs_debt', 1, null, null;

insert into df.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
select 'V', 'Collection', 'Collection', array[('dwh.srs_debt', 'f')::df.point], 100
	, 'dwh.usp_glb_debt', 'dwh.glb_debt', 1, null, null;

insert into df.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
select 'V', 'Collection', 'Collection'
	, array[('dwh.srs_collection_event', 'f')::df.point, ('dwh.glb_collection_event', 'f')::df.point
		, ('dwh.srs_debt', 'f')::df.point, ('dwh.glb_debt', 'f')::df.point
		, ('dwh.sa_collection_event', 'f')::df.point]
	, 100, 'dwh.vw_dwh_collection_event', 'dwh.dwh_collection_event', 1, 'dttm', null;

insert into df.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
select 'V', 'Collection', 'Collection'
	, array[('dwh.srs_debt', 'f')::df.point, ('dwh.glb_debt', 'f')::df.point
		, ('dwh.sa_debt', 'f')::df.point]
	, 100, 'dwh.vw_dwh_debt', 'dwh.dwh_debt', 1, null, null;

insert into df.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
select 'V', 'Main', 'Main_Payment'
	, null
	, 100, 'dwh.vw_main_dwh_payment', 'dwh.dwh_payment', 1, 'dttm', null;

insert into df.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
select 'V', 'Collection', 'Collection'
	, array[('dwh.dwh_collection_event', 'f')::df.point, ('dwh.dwh_payment', 'f')::df.point]
	, 100, 'dwh.vw_ams_collection_event', 'dwh.ams_collection_event', 1, 'dttm', null;

go

drop table if exists df.flow_rep_dttm cascade;

create table df.flow_rep_dttm(p df.point not null
	, dttm timestamp
	, rep_id bigint
	, CONSTRAINT choose CHECK (dttm is null or rep_id is null));

drop table if exists df.flow_rep_dttm_clc cascade;

create table df.flow_rep_dttm_clc(id_batch bigint
	, p df.point not null
	, id int
	, dttm timestamp
	, rep_id bigint
	, CONSTRAINT choose CHECK (dttm is null or rep_id is null));

go

drop view if exists df.vw_flow_1;

create or replace view df.vw_flow_1
as
SELECT *, case when dense_rank() over (partition by dest order by dest_tier desc) = 1 then 'f'
			else dest_tier::char(2) end dest_tier_f
FROM df.flow;

go

create or replace view df.vw_flow_2
as
WITH RECURSIVE r AS (
	SELECT *, null::df.point dep, 1/*dense_rank() over (order by priority)*/ vw_ordr
	FROM df.vw_flow_1
	WHERE deps is null

	union all

	SELECT f.*
		, r.vw_ordr + 1/*dense_rank() over (order by f.priority)*/ vw_ordr
	FROM (select *, unnest(f.deps) dep
		from df.vw_flow_1 f) f
		JOIN r
			ON f.dep = (r.dest, r.dest_tier_f)::df.point
)
SELECT id, type, process, horizon, array_agg(r.dep) deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col, dest_tier_f
	, max(vw_ordr) vw_ordr
FROM r
group by id, type, process, horizon, priority, source, dest, dest_tier, cond_col, rep_dttm_col, dest_tier_f
order by vw_ordr, priority, id;

go

drop view if exists df.vw_flow ;

create or replace view df.vw_flow
as
SELECT id, type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col, dest_tier_f
	, r2.vw_ordr, coalesce(min(f2.vw_ordr) - 1, max(r2.vw_ordr) over ()) vw_ordr_max
FROM df.vw_flow_2 r2
	left join (select f2.vw_ordr, unnest(f2.deps) dep
		from df.vw_flow_2 f2) f2
		on f2.dep = (r2.dest, r2.dest_tier_f)::df.point
group by id, type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col, dest_tier_f, r2.vw_ordr
order by vw_ordr, priority, id;

go

create or replace function df.get_for_resource(in _id int) returns bool
as $$
begin
return true;
end
$$ LANGUAGE plpgsql;

create or replace procedure df.wait_for_resource(in _id int)
as $$
begin
end
$$ LANGUAGE plpgsql;

go

CREATE TYPE df.exec_status AS ENUM ('done', 'error', 'running');

go

drop table if exists df.exec_log ;

CREATE TABLE df.exec_log (
	id_batch int8 NOT NULL,
	id int4 NOT NULL,
	startdate timestamp NOT NULL,
	enddate timestamp NULL,
	executed df.exec_status not null,
	command text NOT NULL,
	row_count bigint,
	"_sqlstate" text NULL,
	"_sqlerrm" text NULL,
	CONSTRAINT exec_log_pkey PRIMARY KEY (id_batch, id, startdate)
);

go

drop table if exists df.batch_log ;

CREATE TABLE df.batch_log (
	id_batch int8 not NULL,
	id int4 not NULL,
	vw_ordr smallint not NULL,
	vw_ordr_max smallint not null,
	priority smallint not null,
	loaddate timestamp not NULL,
	executed df.exec_status DEFAULT null,
	enabled bool default true,
	job smallint default null,
	startdate timestamp default null,
	_sqlstate text default null,
	_sqlerrm text default null
);

go

create or replace function df.find_free_job(_id_batch bigint, _id int) returns smallint
as $$
declare
u int;
_stf text;
_stfi bigint;
begin

drop table if exists m;

if not coalesce(array['ssdf_postgres_conn'::text] <@ dblink_get_connections(), false) then
		select dblink_connect('ssdf_postgres_conn', 'dbname=postgres') into _stf;
end if;

select max(num) into u from dblink('ssdf_postgres_conn'
	, 'select split_part(j.jobname, ''_'', 2)::smallint from cron.job j
		where j.jobname like ''ssdf%'';')
	as t(num smallint);

create temp table m as
select g.*
from
	generate_series(1, u) g(num)
	left join dblink('ssdf_postgres_conn'
		, 'select split_part(j.jobname, ''_'', 2)::smallint from cron.job j
			where j.jobname like ''ssdf%'';')
		as t(num smallint)
		on t.num = g.num
where t.num is null;

for u in (select num from m)
loop
	select schedule into _stfi from dblink('ssdf_postgres_conn', 
		'SELECT cron.schedule_in_database(''ssdf_' || u::varchar(5) || ''', ''0 0 * * *'', ''select pg_sleep(1);'', ''demo'');')
		as t(schedule bigint);
end loop;

u = null;

select num into u from dblink('ssdf_postgres_conn'
	, 'select split_part(j.jobname, ''_'', 2)::smallint from cron.job j
			left join cron.job_run_details r on r.jobid = j.jobid and r.status = ''running''
		where j.jobname like ''ssdf%'' and r.jobid is null;')
		as t(num smallint)
	left join df.batch_log b
		on b.executed = 'running' and b.job = t.num
where b.id_batch is null
order by 1
limit 1;

if u is null then
	select num + 1 into u from dblink('ssdf_postgres_conn'
	, 'select max(split_part(j.jobname, ''_'', 2)::smallint) from cron.job j
		where j.jobname like ''ssdf%'';')
			as t(num smallint);
end if;

if u is null then
	u = 1;
end if;

update df.batch_log
set job = u
	, executed = 'running'
	, startdate = CURRENT_TIMESTAMP
where id_batch = _id_batch and id = _id;

drop table if exists m;

return u;

end
$$ LANGUAGE plpgsql;

go

drop procedure df.exec_id;

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
WHERE table_name = split_part(_dest."name", '.', -1) and table_schema = split_part(_dest."name", '.', -2) and column_name = 'ID_Batch';

SELECT true into _dest_LoadDate
FROM information_schema.columns
WHERE table_name = split_part(_dest."name", '.', -1) and table_schema = split_part(_dest."name", '.', -2) and column_name = 'LoadDate';

SELECT true into _dest_UPDATE_DTTM
FROM information_schema.columns
WHERE table_name = split_part(_dest."name", '.', -1) and table_schema = split_part(_dest."name", '.', -2) and column_name = 'UPDATE_DTTM';

SELECT true into _dest_REP_DATE
FROM information_schema.columns
WHERE table_name = split_part(_dest."name", '.', -1) and table_schema = split_part(_dest."name", '.', -2) and column_name = 'REP_DATE';

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

go

DROP PROCEDURE df.exec_batch_from_id;

create or replace procedure df.exec_batch_from_id(in _id_batch bigint, in _id int, in _mode varchar(100), in _use_rep_dttm bool)
as $$
<<local>>
declare
_v int;
_i int = 0;
_curid int;
r record;
_stf text;
_stfi bigint;
_lockhandle varchar;
_strt_dt timestamp;
begin

insert into df.dbg
select _id_batch, _id, CURRENT_TIMESTAMP::text || ' start';

CALL dbms_lock.allocate_unique (lockname => 'ssdf_exec_batch_from_id_lock', lockhandle => _lockhandle);

if (select dbms_lock.request(_lockhandle)) <> 0 then
	raise exception 'lock failed for ssdf_exec_batch_from_id';
end if;

insert into df.dbg
select _id_batch, _id, CURRENT_TIMESTAMP::text || ' lock';

drop table if exists fb;

drop table if exists t;

drop table if exists t2;

drop table if exists t3;

declare _rowcount bigint;
BEGIN

    call df.exec_id(_id_batch, _id, _strt_dt);

	UPDATE df.batch_log as b
	SET executed = l.executed
	FROM df.exec_log l
	WHERE l.id_batch = _id_batch and l.id = _id and l.startdate = _strt_dt
		AND b.id_batch = l.id_batch and b.id = l.id;

	if (select executed from df.batch_log where id_batch = _id_batch and id = _id) = 'error' then

		create temp table t2 (id int);
		
		loop
			insert into t2
			select f2.id from
				(select array_agg((f.dest, f.dest_tier_f)::df.point) dests from
					(select distinct f.dest, f.dest_tier_f from df.batch_log as b
						join df.vw_flow f
							on f.id = b.id
					where b.executed = 'error' and  b.id_batch = _id_batch
					union
					select distinct f.dest, f.dest_tier_f from t2
						join df.vw_flow f
							on f.id = t2.id) f) f
				join df.vw_flow f2
					on f2.deps && f.dests
			where f2.id in (select id from df.batch_log where id_batch = _id_batch and executed is null)
				and not exists (select 1 from t2 where t2.id = f2.id)
				and ((_mode = 'full' and (select array_agg(p) from (select unnest(f.dests) p
					union all
					(select p from df.flow_rep_dttm rd where _use_rep_dttm and rd.dttm
						<= (select dttm from df.flow_rep_dttm rd2 where rd2.p = (f2.dest, f2.dest_tier_f)::df.point)))) @> f2.deps)
				or (_mode = 'strict' /*and exists ((SELECT unnest(f.dests)
							union all
							(select p from df.flow_rep_dttm rd where _use_rep_dttm and rd.dttm
								> (select dttm from df.flow_rep_dttm rd2 where rd2.p = (f2.dest, f2.dest_tier_f)::df.point)))
	    				INTERSECT
	    				SELECT unnest(f2.deps))*/));

			GET DIAGNOSTICS _rowcount := ROW_COUNT;

			EXIT WHEN _rowcount = 0;
		end loop;

		update df.batch_log as b
		set enabled = false
		where id_batch = _id_batch and id in (select id from t2);

		drop table if exists t2;
	end if;
END;

--commit;

--select id_batch into _stfi from df.batch_log where id_batch = _id_batch FOR UPDATE;

/*insert into df.dbg
select _id_batch, _id
	, coalesce((select 1 from df.batch_log b where b.id_batch = _id_batch and executed = false --zatablochit'?
		and vw_ordr = (select vw_ordr from df.batch_log b2 where b2.id_batch = _id_batch and b2.id = _id)), -1);*/

if not coalesce(array['ssdf_postgres_conn'::text] <@ dblink_get_connections(), false) then
	select dblink_connect('ssdf_postgres_conn', 'dbname=postgres') into _stf;
end if;

if not exists (select 1 from df.batch_log b2 where b2.id_batch = _id_batch and (b2.executed is null or b2.executed = 'running') and b2.enabled = true 
	and vw_ordr = (select vw_ordr from df.batch_log b where b.id_batch = _id_batch and b.id = _id))
then

	select min(vw_ordr) into _v
	from df.batch_log b2
	where b2.id_batch = _id_batch and b2.executed is null and b2.enabled = true and vw_ordr > (select vw_ordr
		from df.batch_log b where b.id_batch = _id_batch and b.id = _id);
	
	if _v is null then
		select vw_ordr into _v
		from df.batch_log b where b.id_batch = _id_batch and b.id = _id;
	end if;
	
	create table fb as
	select * from df.batch_log b where b.id_batch = _id_batch and b.executed is null and b.enabled = true and vw_ordr <= _v;
	
	create table t as
	select *
	from (select *
		from (select id from fb
			order by fb.vw_ordr, fb.priority limit 1)
		union all
		select fb2.id from
			(select * from fb
			order by fb.vw_ordr, fb.priority limit 1) fb
			join fb fb2
				on fb2.id <> fb.id and fb2.vw_ordr <= fb.vw_ordr_max);
	
	delete from fb
	where id not in (select id from t);
	
	loop
		_curid = null;
		select id into _curid from fb b order by priority, vw_ordr, id limit 1 offset _i;
		
		if _curid is null then
			exit;
		end if;
		
		if df.get_for_resource(_curid) then
	
			insert into df.dbg
			select _id_batch, _id, CURRENT_TIMESTAMP::text || ' first ' || _curid::text;
	
			select schedule into _stfi from dblink('ssdf_postgres_conn', 
					'SELECT cron.schedule_in_database(''ssdf_' || df.find_free_job(_id_batch, _curid)::varchar(5) || '''
						, ''' || EXTRACT(minute FROM CURRENT_TIMESTAMP + interval '2 minute')::text || ' '
						|| EXTRACT(hour FROM CURRENT_TIMESTAMP + interval '2 minute')::text 
						|| ' * * *'', ''call df.exec_batch_from_id('
						|| _id_batch::varchar(10) || ', ' || _curid::varchar(10) || ', ''''' || _mode || ''''', ' || _use_rep_dttm::varchar(10) || ')'', ''demo'');')
					as t(schedule bigint);
		end if;
		_i = _i + 1;
	
	end loop;
	
	_curid = null;
	
	select id into _curid from fb b order by priority, vw_ordr, id limit 1;
	
	if _curid is not null and not exists (select id from df.batch_log b where b.id_batch = _id_batch and executed = 'running') then
	
		call df.wait_for_resource(_curid);
	
		insert into df.dbg
		select _id_batch, _id, CURRENT_TIMESTAMP::text || ' second ' || _curid::text;
	
		select schedule into _stfi from dblink('ssdf_postgres_conn', 
				'SELECT cron.schedule_in_database(''ssdf_' || df.find_free_job(_id_batch, _curid)::varchar(5) || '''
					, ''' || EXTRACT(minute FROM CURRENT_TIMESTAMP + interval '2 minute')::text || ' '
					|| EXTRACT(hour FROM CURRENT_TIMESTAMP + interval '2 minute')::text 
					|| ' * * *'', ''call df.exec_batch_from_id('
					|| _id_batch::varchar(10) || ', ' || _curid::varchar(10) || ', ''''' || _mode || ''''', ' || _use_rep_dttm::varchar(10) || ')'', ''demo'');')
				as t(schedule bigint);
		
	end if;

end if;

drop table if exists fb;

drop table if exists t;

drop table if exists t2;

drop table if exists t3;

--commit;

perform dbms_lock.release(lockhandle => _lockhandle);

insert into df.dbg
select _id_batch, _id, CURRENT_TIMESTAMP::text || ' end';

end
$$ LANGUAGE plpgsql;

go

drop procedure df.exec_flow;

create or replace procedure df.exec_flow(in _mode varchar(100), in _procs varchar(100)[] default null
	, in _start_ids int[] default null, in _dests df.point[] default null, in _excls df.point[] default null, in _use_rep_dttm bool default true)
as $$
DECLARE
    _r record;
	--b bool;
	_i int = 1;
	_rowcount int;
	_id_batch bigint;
	_stf text;
	_stfi bigint;
begin

if _mode not in ('full', 'strict') then
	raise exception 'mode could be only full and strict';
end if;

drop table if exists f;

drop table if exists f2;


--get start points
create temp table f
as
select *
from df.vw_flow_1
where (_procs is null or process in (select unnest(_procs))) and ((deps is null and _start_ids is null) 
		or id in (select unnest(_start_ids)))
	and (_excls is null or array[(dest, dest_tier_f)::df.point] <@ _excls);

--raise notice '% 1st', (select count(*) from f);

loop
	--get next points
	insert into f
	select f2.*
	from
		--start points with check that dest + dest_tier_f fully completed 
		(select array_agg((f.dest, f.dest_tier_f)::df.point) dests 
			from (select *, count(*) over (partition by dest, dest_tier_f) eff_cnt
				from f) f
			where (_dests is null or not (array[(f.dest, f.dest_tier_f)::df.point] <@ _dests))
				and (_mode = 'full' or (_mode = 'strict' and f.eff_cnt = 
					(select cnt from (select *, count(*) over (partition by dest, dest_tier_f) cnt
						from df.vw_flow_1 fr) fr
					where fr.id = f.id))
					--or  exists (select 1 from df.flow_rep_dttm rd where rd.p = (f.dest, f.dest_tier_f)::df.point
					--	and rd.dttm)
			)) fa
		join df.vw_flow_1 f2
			on (_mode = 'strict' and (select array_agg(p) from (select unnest(fa.dests) p
					union all
					(select p from df.flow_rep_dttm rd where _use_rep_dttm and rd.dttm
						> (select dttm from df.flow_rep_dttm rd2 where rd2.p = (f2.dest, f2.dest_tier_f)::df.point)))) @> f2.deps) 
				or (_mode = 'full' and exists ((SELECT unnest(fa.dests)
							union all
							(select p from df.flow_rep_dttm rd where _use_rep_dttm and rd.dttm
								> (select dttm from df.flow_rep_dttm rd2 where rd2.p = (f2.dest, f2.dest_tier_f)::df.point)))
	    				INTERSECT
	    				SELECT unnest(f2.deps)))
	where f2.id not in (select id from f) and (_procs is null or f2."process" in (select unnest(_procs)))
		and (_excls is null or array[(f2.dest, f2.dest_tier_f)::df.point] <@ _excls);

	GET DIAGNOSTICS _rowcount := ROW_COUNT;

	EXIT WHEN _rowcount = 0;

end loop;

--get correct priority
create temp table f2 as
select f.id, v.vw_ordr, v.vw_ordr_max, v.priority
from f
	join df.vw_flow v on v.id = f.id;

_id_batch = (select coalesce(max(d.id_batch), 0::bigint) from df.batch_log d) + 1;

--generate batch's work set/log 
insert into df.batch_log (id_batch, id, vw_ordr, vw_ordr_max, priority, LoadDate)
select _id_batch, id, vw_ordr, vw_ordr_max, priority, CURRENT_TIMESTAMP
from f2;

--print batch
for _r in (select (id, vw_ordr) from f2
	order by vw_ordr, priority)
loop
	raise notice '(id,vw_ordr): %', _r;
end loop;

if not coalesce(array['ssdf_postgres_conn'::text] <@ dblink_get_connections(), false) then
		select dblink_connect('ssdf_postgres_conn', 'dbname=postgres') into _stf;
end if;

--start executing batch from start points
for _r in (select id from f2 where vw_ordr = (select min(vw_ordr) from f2) order by priority, id)
loop
	if _i = 1 then
		call df.wait_for_resource(_r.id);
	else
		if not df.get_for_resource(_r.id) then
			continue;
		end if;
	end if;

	select schedule into _stfi from dblink('ssdf_postgres_conn', 
		'SELECT cron.schedule_in_database(''ssdf_' || df.find_free_job(_id_batch, _r.id)::varchar(5) || ''', '''
			|| EXTRACT(minute FROM CURRENT_TIMESTAMP + interval '2 minute')::text || ' '
				|| EXTRACT(hour FROM CURRENT_TIMESTAMP + interval '2 minute')::text 
			||' * * *'', ''call df.exec_batch_from_id('
			|| _id_batch::varchar(10) || ', ' || _r.id::varchar(10) || ', ''''' || _mode || ''''', ' || _use_rep_dttm::varchar(10) || ')'', ''demo'');')
		as t(schedule bigint);
	_i = _i + 1;
end loop;

drop table f;

drop table f2;

commit;

end
$$ LANGUAGE plpgsql;


select dbms_lock.request('1073741825');





	