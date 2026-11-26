DROP FUNCTION IF EXISTS ssdf.list_running();

-- admin view of points stuck in 'running' after a system fault (network drop,
-- server reboot, dispatcher crash); user SQL errors are handled by ssdf.exec_id itself
create or replace function ssdf.list_running()
returns table(num int, id_batch bigint, id int, source varchar, dest varchar, startdate timestamp)
language sql stable
as $$
	select row_number() over (order by b.id_batch, b.id)::int, b.id_batch, b.id, f.source, f.dest, b.startdate
	from ssdf.batch_log b
		join ssdf.flow f on f.id = b.id
	where b.executed = 'running'
	order by b.id_batch, b.id
$$;
