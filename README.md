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
If you want to do changes in flow, you should dirictly insert/update to `df.flow`.
Also, as you can see, tool have one mini feature - tier of the point of data flow. What is this?\
For example, you have big table, which builds for many stages. Tier of the point uses special type - `df.point` - with 2 fields - name of table and building stage of it. So, you can address each stage of building of the table for further using it as dependencie. If you want address final stage, use "f" (only in dependencies, in targets you only use numbers, because "f" calculates at moment (as last existing stage).



