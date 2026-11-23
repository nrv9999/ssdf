DROP FUNCTION IF EXISTS df.list_running();

-- admin view of points stuck in 'running' after a system fault (network drop,
-- server reboot, dispatcher crash); user SQL errors are handled by df.exec_id itself
create or replace function df.list_running()
returns table(num int, id_batch bigint, id int, source varchar, dest varchar, startdate timestamp)
language sql stable
as $$
	select row_number() over (order by b.id_batch, b.id)::int, b.id_batch, b.id, f.source, f.dest, b.startdate
	from df.batch_log b
		join df.flow f on f.id = b.id
	where b.executed = 'running'
	order by b.id_batch, b.id
$$;
