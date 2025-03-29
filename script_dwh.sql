
drop procedure if exists dwh.usp_sa_wait_contact_collection_event;

create or replace procedure dwh.usp_sa_wait_contact_collection_event(in _id_batch bigint, in _id int, in _p df.point)
as $$
begin

insert into df.flow_rep_dttm_clc (id_batch, p, id, dttm, rep_id)
select _id_batch, _p, _id, CURRENT_TIMESTAMP, null;

end
$$ LANGUAGE plpgsql;

drop table if exists dwh.sa_collection_event;

create table dwh.sa_collection_event (id_batch bigint not null, LoadDate timestamp not null, REP_DATE timestamp not null
	, id bigint not null, dt timestamp not null, comment text, id_debt bigint not null);

drop table if exists dwh.h_collection_event;

create table dwh.h_collection_event (id_batch bigint not null, LoadDate timestamp not null, UPDATE_DTTM timestamp not null, REP_DATE timestamp not null
	, id bigint not null, dt timestamp not null, comment text, id_debt bigint not null);

drop table if exists dwh.sa_debt;

create table dwh.sa_debt (id_batch bigint not null, LoadDate timestamp not null, REP_DATE timestamp not null
	, id bigint not null, dt timestamp not null, comment text, amount numeric(18, 2) not null);

drop table if exists dwh.h_debt;

create table dwh.h_debt (id_batch bigint not null, LoadDate timestamp not null, REP_DATE timestamp not null
	, id bigint not null, dt timestamp not null, comment text, amount numeric(18, 2) not null);

drop table if exists dwh.srs_collection_event;

CREATE TABLE dwh.srs_collection_event (id_batch bigint not null, LoadDate timestamp not NULL, id bigint GENERATED ALWAYS AS IDENTITY NOT null, source_id bigint NOT null);

create or replace procedure dwh.usp_srs_collection_event(in _id_batch bigint)
as $$
begin

insert into dwh.srs_collection_event (id_batch, LoadDate, source_id)
select distinct _id_batch, CURRENT_TIMESTAMP, id
from dwh.sa_collection_event sa
where not exists (select 1 from dwh.srs_collection_event s where s.source_id = sa.id);

end
$$ LANGUAGE plpgsql;

CREATE TABLE dwh.glb_collection_event (id_batch bigint not null, LoadDate timestamp not NULL, RK bigint NOT null, source_id bigint NOT null, source_cd varchar(7) not null);

create or replace procedure dwh.usp_glb_collection_event(in _id_batch bigint)
as $$
declare mx bigint;
begin

select max(RK) into mx from dwh.glb_collection_event;

insert into dwh.glb_collection_event (id_batch, LoadDate, RK, source_id, source_cd)
select distinct _id_batch, CURRENT_TIMESTAMP, mx + row_number() over (), id, 'CONT'
from dwh.srs_collection_event s
where not exists (select 1 from dwh.glb_collection_event g where g.source_id = s.id and g.source_cd = 'CONT');

end
$$ LANGUAGE plpgsql;

drop table if exists dwh.srs_collection_event;

CREATE TABLE dwh.srs_debt (id_batch bigint not null, LoadDate timestamp not NULL, id bigint GENERATED ALWAYS AS IDENTITY NOT null, source_id bigint NOT null);

create or replace procedure dwh.usp_srs_debt(in _id_batch bigint)
as $$
begin

insert into dwh.srs_debt (id_batch, LoadDate, source_id)
select distinct _id_batch, CURRENT_TIMESTAMP, id
from dwh.sa_debt sa
where not exists (select 1 from dwh.srs_debt s where s.source_id = sa.id);

end
$$ LANGUAGE plpgsql;

CREATE TABLE dwh.glb_debt (id_batch bigint not null, LoadDate timestamp not NULL, RK bigint NOT null, source_id bigint NOT null, source_cd varchar(7) not null);

create or replace procedure dwh.usp_glb_debt(in _id_batch bigint)
as $$
declare mx bigint;
begin

select max(RK) into mx from dwh.glb_debt;

insert into dwh.glb_collection_event (id_batch, LoadDate, RK, source_id, source_cd)
select distinct _id_batch, CURRENT_TIMESTAMP, mx + row_number() over (), id, 'CONT'
from dwh.srs_debt s
where not exists (select 1 from dwh.glb_debt g where g.source_id = s.id and g.source_cd = 'CONT');

end
$$ LANGUAGE plpgsql;

CREATE TABLE dwh.dwh_collection_event(id_batch bigint not null, LoadDate timestamp not NULL, REP_DATE timestamp not null, RK bigint NOT null, DEBT_RK bigint not null
	, DTTM timestamp not null, comm text);

create view dwh.vw_dwh_collection_event as
select gc.RK, gd.RK DEBT_RK, sa.DT DTTM, sa.comment comm
from dwh.sa_collection_event sa
	join dwh.srs_debt sd
		on sd.source_id = sa.id_debt
	join dwh.glb_debt gd
		on gd.source_cd = 'CONT' and gd.source_id = sd.id
	join dwh.srs_collection_event sc
		on sc.source_id = sa.id
	join dwh.glb_collection_event gc
		on gc.source_cd = 'CONT' and gc.source_id = sc.id;




