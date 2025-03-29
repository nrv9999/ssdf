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

### description
project includes SQL code of the tool, sample data flow, and sample DWH for sample data flow.\
Project have 2 main features:
1) describing points of SQL data flow in setting table, where your have source and destination and code generation for them.
2) describing dependencies between points of SQL data flow directly as is and calculating fact order of execution at the moment of execution.
3) also i hope that SQL realization of the tool quite tranparent (excluding calculating of the dependencies, hah) and it could be modifyed for various needs, for exmaple, types of points of data flow.



