# ssdf
## super SQL data flow

tool/framework for realization of the SQL data flow (ETL), also based on SQL. It uses PostgreSQL.

### Requirements
postgres extensions:\
dblink\
[pg_background](https://github.com/vibhorkum/pg_background) - required by pg_dbms_lock\
[pg_dbms_lock](https://github.com/HexaCluster/pg_dbms_lock)\
[pg_cron](https://github.com/citusdata/pg_cron). pg_cron should be installed in postgres database.\
Set `cron.timezone` to the server's `timezone` (pg_cron defaults to GMT, while ssdf builds schedules from `CURRENT_TIMESTAMP`); it is a postmaster-level setting, so restart after changing.\
Your database (for now) should be named "demo".

### deploying
apply framework/install.sql, then example/install.sql at your "demo" database:
```
psql -d demo -f framework/install.sql
psql -d demo -f example/install.sql
```

### short description
project includes SQL code of the tool, sample data flow, and sample DWH for sample data flow.\
Project have 2 main features:
1) describing points of SQL data flow in setting table, where your have source and destination and code generation for them.
2) describing dependencies between points of SQL data flow directly as is and calculating fact order of execution at the moment of execution.
3) also I hope that SQL realization of the tool quite tranparent (excluding calculating of the dependencies, hah) and it could be modifyed for various needs, for example, types of points of data flow.

how it looks:\
![ssdf](https://github.com/nrv9999/ssdf/blob/main/screenshot_dataflow.png)

### description
so, we have table for describing SQL data flow:
```sql
select * from df.flow
```
If you want to do changes in flow, you should directly insert/update to `df.flow`.\
\
and horizon table `df.horizon` (for now, only ddtm's is functional)\
\
Also, as you can see, tool have one mini feature - tier of the point of data flow. What is this?\
For example, you have big table, which builds for many stages. Tier of the point uses special type - `df.point` - with 2 fields - name of table and building stage of it. So, you can address each stage of building of the table for further using it as dependencie. If you want address final stage, use "f" (only in dependencies, in targets you only use numbers, because "f" calculates at moment (as last existing stage).\
\
In tool, presents one thing - rep date. Each point when executing, rep date calculating for. It uses for information, and, for calculating points of dataflow will executed (further will be about). See table `df.flow_rep_dttm` - one row per point of flow, with rep date, up to that the point is loaded. Rep date of a destination (stage) is min of rep dates of points, building it, and it is known only when all of them reported - see view `df.vw_dest_rep_dttm`. Also table `df.flow_rep_dttm_clc` keeps contributions of points per batch, for multi-tier readiness of a destination.\
\
and we have a view, calculating order of executing data flow:
```sql
select * from df.vw_flow;
```
view has following calculated fields:
- `dest_tier_f` - replaces value of dest_tier by "f" if it last point(s) for building destination.
- `vw_ordr` - first number, at that point can be already executed.
- `vw_ordr_max` - last number, at that point can be executed.

So we can see, order of execution is undefined. On the idea, it resolves at moment of execution by the available resources. Also degree of parallelism defines by available resources - view have no limit for parallelism.\
Also we have `priority` field, that directly used for calculating order. By the resource limitation, we consider limited number of points at each stage of execution, and; at each stage we prefer more prior points (from available) for execution.\
\
Dynamic parallelism based on available resources.\
For now, it is not realized - available resources are infinitly. But realized structure for - it is needed to wrote content of functions `df.get_for_resource` and `df.wait_for_resource`.\
So, it each stage of execution, program asked for resource for each point can be runned (and runs if get). If nothing runned program waits resource for most prior point can be runned.\
\
So, we going to execution of data flow and appropriate procedures.\
\
`df.exec_id(in _id_batch bigint, in _id int, out _strt_dt timestamp)` - procedure, which executes given point of data flow. It called when executing a flow and it can be called when you test created point in development.
Speaking about this procedure, it necessary to tell about:
- `select * from df.exec_log el order by startdate desc;` - log of SQL code execution of this procedure
- types of points of data flow. Now realized several points: "V" - view, "P" - procedure, "StrtF" - function, starting the flow and returning rep date of a source. About types will be separate paragraph.

\
`df.exec_flow(in _mode varchar(100), in _procs varchar(100)[] default null
	, in _start_ids int[] default null, in _dests df.point[] default null, in _excls df.point[] default null, in _use_rep_dttm bool default true)` - main procedure, that's executes a data flow. As you can see, it has many parameters:
- `_mode` - "strict" or "full". Difference between then is in what in "strict" mode point is executed when all od it dependencies are executed in current batch (or have newer rep date) and in "full" mode only one dependencie (executed or has newer rep date) is enough.
- `_procs` - array of names of processes. Process, here (in tool) is conditional way for separate different workload, but by default, data flow consider at all. So, if it null, data flow consider at all, if it given, data flow limits by given processes.
Handling errors: errors handles as excluded from flow points.
- `_start_ids`. If it null, data flow starts by points having no dependencies, if it given - by then.
- `_dests` - destinations, that is will not generate data flow after them.
- `_excls` - points, to be excluded from executing data flow.
- `_use_rep_dttm bool` - use rep date for calculating dependecies (if false, it will be considered only dependencies, runned in the batch.

Point types\
\"P\" - procedure. It works as call \_source\_(id_batch)\
\"V\" - view. It works as insert into dest select from view in source. Column names of view must match dest.\
\"StrtF\" - starting function. It suppose to use for waiting a source and returning its rep date.\
Call looks like:
```sql
select dwh.uf_sa_wait_contact_collection_event()
```
Content (excluding waiting) should be:
```sql
create or replace function dwh.uf_sa_wait_contact_collection_event()
returns timestamp
as $$
begin
return CURRENT_TIMESTAMP;
end
$$ LANGUAGE plpgsql;
```
so, as you see, function should return apropriate REP DATE; `df.exec_id` stores it in `df.flow_rep_dttm`. Destination of a StrtF point is the sa-level load procedure it feeds; the load targets the sa-table and depends on the StrtF point ("f").\
\
some internals\
declared procedure `df.exec_flow` uses proc `df.exec_batch_from_id` for each runned point and generates content of table `df.batch_log` for current batch.\
In table `df.batch_log` wrotes all data flow planned to run in batch.\
In case when `df.exec_batch_from_id` see the error in runned code - it disables all related points of data flow (according to load mode  politics - strict or full).

### examples
adding horizon:
```sql
insert into df.horizon (name, dttm, dttm_end, dttm_type, id, id_end)
select 'Collection', '2025-01-01', null, 'DTTM', null, null;
```
adding point of data flow:
```sql
insert into df.flow (type, process, horizon, deps, priority, source, dest, dest_tier, cond_col, rep_dttm_col)
select 'StrtF', 'Collection', null, null, 100, 'dwh.uf_sa_wait_contact_collection_event', 'dwh.usp_sa_load_contact_collection_event'
	, 1, null, null;
```
executing point:
```sql
do $$
declare _d timestamp;
begin
call df.exec_id(0, 25, _d);
end
$$;
```
executing flow:
```sql
--full flow in strict mode
call df.exec_flow('strict');
--full flow in full mode
call df.exec_flow('full');
--full flow starting from point id=4 in strict mode
call df.exec_flow('strict', null, array[4], null); 
--process "Collection" in full mode
call df.exec_flow('full', array['Collection']);
--process "Collection" in strict mode
call df.exec_flow('strict', array['Collection']);
```

### Python dispatcher
`framework/python/ssdf.py` is a client-thread replacement for `df.exec_flow` / `df.exec_batch_from_id`. It uses the same planning, error-propagation and wave-selection SQL, but launches points as threads, each with its own client connection, instead of pg_cron jobs - so no +2 minute delay and no background workers. `df.exec_id` is still called as SQL; the original SQL procedures remain installed and usable. Batch state transitions are serialized with a PostgreSQL advisory lock instead of `dbms_lock` (points themselves execute outside the lock, in parallel), so the Python path needs neither pg_cron nor pg_background/pg_dbms_lock - only Python 3 and psycopg (v3).

Installing (puts the scripts into `/opt/ssdf` and the commands onto PATH):
```
sudo pacman -S python-psycopg
sudo bash install.sh
```
Uninstall: `sudo bash uninstall.sh` (removes `/opt/ssdf` and the PATH wrappers; the python-psycopg package stays).
Usage:
```
ssdf --mode strict
ssdf --mode full --procs Collection
ssdf-test             # test scenarios, resets df log tables between runs
ssdf-test --no-reset  # keep state between runs (for inspection; expectations assume a clean state)
```
Debugging from the project (no install): `python3 framework/python/ssdf.py --mode strict`, `python3 example/test/python/all_execs.py` - psycopg must be importable (the system package above, or a venv: `python3 -m venv .venv && .venv/bin/pip install "psycopg[binary]"`). The test script finds `ssdf.py` both in the repo layout and in `/opt/ssdf` (test in `/opt/ssdf/test`).

Parameters mirror `df.exec_flow`: `--procs`, `--start-ids`, `--dests name:tier`, `--excls name:tier`, `--no-rep-dttm`, plus connection options `--dbname` (default "demo"), `--host`, `--port`, `--user`. `--excls` is a true exclusion (the inverted condition is fixed in both the dispatcher and `df.exec_flow`). `--no-rep-dttm` does not stop rep-date recording (`exec_id` always writes `flow_rep_dttm`/`clc`) - it only excludes watermarks from the dependency calculation.



