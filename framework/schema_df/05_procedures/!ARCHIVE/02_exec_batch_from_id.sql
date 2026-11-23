DROP PROCEDURE IF EXISTS df.exec_batch_from_id(bigint, int, varchar, bool);

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

drop table if exists tmp_next;

drop table if exists tmp_next_filter;

drop table if exists tmp_exclude;

declare _rowcount bigint;
BEGIN

    call df.exec_id(_id_batch, _id, _strt_dt);

	UPDATE df.batch_log as b
	SET executed = l.executed
	FROM df.exec_log l
	WHERE l.id_batch = _id_batch and l.id = _id and l.startdate = _strt_dt
		AND b.id_batch = l.id_batch and b.id = l.id;

	if (select executed from df.batch_log where id_batch = _id_batch and id = _id) = 'error' then

		create temp table tmp_exclude (id int);
		
		loop
			insert into tmp_exclude
			select f2.id from
				(select array_agg((f.dest, f.dest_tier_f)::df.point) dests from
					(select distinct f.dest, f.dest_tier_f from df.batch_log as b
						join df.vw_flow f
							on f.id = b.id
					where b.executed = 'error' and  b.id_batch = _id_batch
					union
					select distinct f.dest, f.dest_tier_f from tmp_exclude
						join df.vw_flow f
							on f.id = tmp_exclude.id) f) f
				join df.vw_flow f2
					on f2.deps && f.dests
			where f2.id in (select id from df.batch_log where id_batch = _id_batch and executed is null)
				and not exists (select 1 from tmp_exclude where tmp_exclude.id = f2.id)
				and ((_mode = 'full' and (select array_agg(p) from (select unnest(f.dests) p
					union all
					(select p from df.vw_dest_rep_dttm rd where _use_rep_dttm and rd.dttm
						<= (select dttm from df.vw_dest_rep_dttm rd2 where rd2.p = (f2.dest, f2.dest_tier_f)::df.point)))) @> f2.deps)
				or (_mode = 'strict' /*and exists ((SELECT unnest(f.dests)
							union all
							(select p from df.vw_dest_rep_dttm rd where _use_rep_dttm and rd.dttm
								> (select dttm from df.vw_dest_rep_dttm rd2 where rd2.p = (f2.dest, f2.dest_tier_f)::df.point)))
	    				INTERSECT
	    				SELECT unnest(f2.deps))*/));

			GET DIAGNOSTICS _rowcount := ROW_COUNT;

			EXIT WHEN _rowcount = 0;
		end loop;

		update df.batch_log as b
		set enabled = false
		where id_batch = _id_batch and id in (select id from tmp_exclude);

		drop table if exists tmp_exclude;
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
	
	create temp table tmp_next as
	select * from df.batch_log b where b.id_batch = _id_batch and b.executed is null and b.enabled = true and vw_ordr <= _v;

	create temp table tmp_next_filter as
	select *
	from (select *
		from (select id from tmp_next
			order by tmp_next.vw_ordr, tmp_next.priority limit 1)
		union all
		select tmp_next2.id from
			(select * from tmp_next
			order by tmp_next.vw_ordr, tmp_next.priority limit 1) tmp_next
			join tmp_next tmp_next2
				on tmp_next2.id <> tmp_next.id and tmp_next2.vw_ordr <= tmp_next.vw_ordr_max);
	
	delete from tmp_next
	where id not in (select id from tmp_next_filter);
	
	loop
		_curid = null;
		select id into _curid from tmp_next b order by priority, vw_ordr, id limit 1 offset _i;
		
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
	
	select id into _curid from tmp_next b order by priority, vw_ordr, id limit 1;
	
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

drop table if exists tmp_next;

drop table if exists tmp_next_filter;

drop table if exists tmp_exclude;

--commit;

perform dbms_lock.release(lockhandle => _lockhandle);

insert into df.dbg
select _id_batch, _id, CURRENT_TIMESTAMP::text || ' end';

end
$$ LANGUAGE plpgsql;
