DROP FUNCTION IF EXISTS df.close_running(int[]);

-- admin closure of stuck 'running' points as error with 'system fault';
-- numbering matches df.list_running(); 0 in the array closes all
create or replace function df.close_running(_nums int[])
returns table(id_batch bigint, id int, source varchar, dest varchar)
language plpgsql volatile
as $$
begin
	return query
	with numbered as (
		select row_number() over (order by b.id_batch, b.id) as num, b.id_batch, b.id
		from df.batch_log b
			join df.flow f on f.id = b.id
		where b.executed = 'running'
	), picked as (
		select n.id_batch, n.id from numbered n
		where n.num = any(_nums) or 0 = any(_nums)
	), upd_exec as (
		update df.exec_log l
		set executed = 'error', _sqlerrm = 'system fault', enddate = CURRENT_TIMESTAMP
		where l.executed = 'running' and (l.id_batch, l.id) in (select p.id_batch, p.id from picked p)
		returning l.id_batch, l.id
	), upd_batch as (
		update df.batch_log b
		set executed = 'error', _sqlerrm = 'system fault'
		where b.executed = 'running' and (b.id_batch, b.id) in (select p.id_batch, p.id from picked p)
		returning b.id_batch, b.id
	)
	select u.id_batch, u.id, f.source, f.dest
	from upd_batch u
		join df.flow f on f.id = u.id;
end
$$;
