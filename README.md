# ssdf
## super SQL data flow

tool/framework for realization of the SQL data flow, also based on SQL. It uses PostgreSQL.

### Requirements
postgres extensions:\
dblink\
[pg_dbms_lock](https://github.com/HexaCluster/pg_dbms_lock)\
[pg_cron](https://github.com/citusdata/pg_cron). pg_cron should be installed in postgres database.\
Your database (for now) should be named "demo".

### deploying
apply script.sql and script_dwh.sql at your "demo" database.

### short description
project includes SQL code of the tool, sample data flow, and sample DWH for sample data flow.\
Project have 2 main features:
1) describing points of SQL data flow in setting table, where your have source and destination and code generation for them.
2) describing dependencies between points of SQL data flow directly as is and calculating fact order of execution at the moment of execution.
3) also I hope that SQL realization of the tool quite tranparent (excluding calculating of the dependencies, hah) and it could be modifyed for various needs, for example, types of points of data flow.

how it looks:\
![ssdf](https://github.com/nrv9999/ssdf/blob/main/screenshot_dataflow.png)

### description
so, we have table for describing SQL data flow:\
```select * from df.flow```\
If you want to do changes in flow, you should dirictly insert/update to `df.flow`.\
\
Also, as you can see, tool have one mini feature - tier of the point of data flow. What is this?\
For example, you have big table, which builds for many stages. Tier of the point uses special type - `df.point` - with 2 fields - name of table and building stage of it. So, you can address each stage of building of the table for further using it as dependencie. If you want address final stage, use "f" (only in dependencies, in targets you only use numbers, because "f" calculates at moment (as last existing stage).\
\
In tool, presents one thing - rep date. Each point when executing, rep date calculating for. It uses for information, and, for calculating points of dataflow will executed (further will be about). See table `df.flow_rep_dttm`. Also presents table `df.flow_rep_dttm_clc`, for calculating rep date in cases when one stage of destination calculating with multiple points.\
\
and we have a view, calculating order of executing data flow:
```select * from df.vw_flow;```\
view has following calculated fields:
- `dest_tier_f` - replaces value of dest_tier by "f" if it last point(s) for building destination.
- `vw_ordr` - first number, at that point can be already executed.
- `vw_ordr_max` - last number, at that point can be executed.

So we can see, order of execution is undefined. On the idea, it resolves at moment of execution by the available resources. Also degree of parallelism defines by available resources - view have no limit for parallelism.\
Also we have `priority` field, that directly used for calculating order. By the resource limitation, we consider limited number of points at each stage of execution, and; at each stage we prefer more prior points (from available) for execution. 
\
So, we going to execution of data flow and appropriate procedures.\
\
`df.exec_id(in _id_batch bigint, in _id int, out _strt_dt timestamp)` - procedure, which executes given point of data flow. It called when executing a flow and it can be called when you test created point in development.
Speaking about this procedure, it necessary to tell about:
- `select * from df.exec_log el order by startdate desc;` - log of SQL code execution of this procedure
- types of points of data flow. Now realized several points: "V" - view, "P" - procedure, "StrtP" - procedure, starting the flow and generating first rep date. About types will be separate paragraph.

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
\


