DROP PROCEDURE IF EXISTS df.exec_flow(varchar, varchar[], int[], df.point[], df.point[], bool);

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
