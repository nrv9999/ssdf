DROP FUNCTION IF EXISTS df.find_free_job(bigint, int);

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
