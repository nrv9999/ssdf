#!/usr/bin/env python3
"""Client-thread dispatcher for the SSDF data flow.

Python replacement for ssdf.exec_flow / ssdf.exec_batch_from_id: the planning,
error-propagation and wave-selection SQL below is the original framework
logic, but points are launched as threads, each with its own client
connection, instead of pg_cron jobs. ssdf.exec_id is still called as SQL.
The original SQL procedures remain installed and usable.

Usage:
    python ssdf.py --mode strict
    python ssdf.py --mode full --procs Collection
"""

import argparse
import logging
import threading

import psycopg

log = logging.getLogger("ssdf")

LOCK_NAME = "ssdf_exec_batch_from_id_lock"
BATCH_LOCK_NAME = "ssdf_batch_id"

# excls is a true exclusion here; the original ssdf.exec_flow still has the inverted <@ condition
PLAN_START_SQL = """
create temp table tmp_plan_list
as
select *
from ssdf.vw_flow_1
where (%(procs)s::varchar[] is null or process in (select unnest(%(procs)s::varchar[]))) and ((deps is null and %(start_ids)s::int[] is null)
        or id in (select unnest(%(start_ids)s::int[])))
    and (%(excls)s::ssdf.point[] is null or not (array[(dest, dest_tier_f)::ssdf.point] <@ %(excls)s::ssdf.point[]))
"""

PLAN_NEXT_SQL = """
insert into tmp_plan_list
select f2.*
from
    (select array_agg((f.dest, f.dest_tier_f)::ssdf.point) dests
        from (select *, count(*) over (partition by dest, dest_tier_f) eff_cnt
            from tmp_plan_list) f
        where (%(dests)s::ssdf.point[] is null or not (array[(f.dest, f.dest_tier_f)::ssdf.point] <@ %(dests)s::ssdf.point[]))
            and (%(mode)s = 'full' or (%(mode)s = 'strict' and f.eff_cnt =
                (select cnt from (select *, count(*) over (partition by dest, dest_tier_f) cnt
                    from ssdf.vw_flow_1 fr) fr
                where fr.id = f.id))
            )) fa
    join ssdf.vw_flow_1 f2
        on (%(mode)s = 'strict' and (select array_agg(p) from (select unnest(fa.dests) p
                union all
                (select p from ssdf.vw_dest_rep_dttm rd where %(use_rep_dttm)s and rd.dttm
                    > (select dttm from ssdf.vw_dest_rep_dttm rd2 where rd2.p = (f2.dest, f2.dest_tier_f)::ssdf.point)))) @> f2.deps)
            or (%(mode)s = 'full' and exists ((SELECT unnest(fa.dests)
                        union all
                        (select p from ssdf.vw_dest_rep_dttm rd where %(use_rep_dttm)s and rd.dttm
                            > (select dttm from ssdf.vw_dest_rep_dttm rd2 where rd2.p = (f2.dest, f2.dest_tier_f)::ssdf.point)))
                    INTERSECT
                    SELECT unnest(f2.deps)))
where f2.id not in (select id from tmp_plan_list) and (%(procs)s::varchar[] is null or f2."process" in (select unnest(%(procs)s::varchar[])))
    and (%(excls)s::ssdf.point[] is null or not (array[(f2.dest, f2.dest_tier_f)::ssdf.point] <@ %(excls)s::ssdf.point[]))
"""

PLAN_ORDER_SQL = """
create temp table tmp_plan as
select tmp_plan_list.id, v.vw_ordr, v.vw_ordr_max, v.priority
from tmp_plan_list
    join ssdf.vw_flow v on v.id = tmp_plan_list.id
"""

BATCH_INSERT_SQL = """
insert into ssdf.batch_log (id_batch, id, vw_ordr, vw_ordr_max, priority, LoadDate)
select %(batch)s, id, vw_ordr, vw_ordr_max, priority, CURRENT_TIMESTAMP
from tmp_plan
"""

UPDATE_EXECUTED_SQL = """
UPDATE ssdf.batch_log as b
SET executed = l.executed
FROM ssdf.exec_log l
WHERE l.id_batch = %(batch)s and l.id = %(id)s and l.startdate = %(strt_dt)s
    AND b.id_batch = l.id_batch and b.id = l.id
"""

EXCLUDE_NEXT_SQL = """
insert into tmp_exclude
select f2.id from
    (select array_agg((f.dest, f.dest_tier_f)::ssdf.point) dests from
        (select distinct f.dest, f.dest_tier_f from ssdf.batch_log as b
            join ssdf.vw_flow f
                on f.id = b.id
        where b.executed = 'error' and b.id_batch = %(batch)s
        union
        select distinct f.dest, f.dest_tier_f from tmp_exclude
            join ssdf.vw_flow f
                on f.id = tmp_exclude.id) f) f
    join ssdf.vw_flow f2
        on f2.deps && f.dests
where f2.id in (select id from ssdf.batch_log where id_batch = %(batch)s and executed is null)
    and not exists (select 1 from tmp_exclude where tmp_exclude.id = f2.id)
    and ((%(mode)s = 'full' and (select array_agg(p) from (select unnest(f.dests) p
        union all
        (select p from ssdf.vw_dest_rep_dttm rd where %(use_rep_dttm)s and rd.dttm
            <= (select dttm from ssdf.vw_dest_rep_dttm rd2 where rd2.p = (f2.dest, f2.dest_tier_f)::ssdf.point)))) @> f2.deps)
    or (%(mode)s = 'strict'))
"""

WAVE_BUSY_SQL = """
select 1 from ssdf.batch_log b2
where b2.id_batch = %(batch)s and (b2.executed is null or b2.executed = 'running') and b2.enabled = true
    and vw_ordr = (select vw_ordr from ssdf.batch_log b where b.id_batch = %(batch)s and b.id = %(id)s)
"""

NEXT_ORDR_SQL = """
select min(vw_ordr)
from ssdf.batch_log b2
where b2.id_batch = %(batch)s and b2.executed is null and b2.enabled = true
    and vw_ordr > (select vw_ordr from ssdf.batch_log b where b.id_batch = %(batch)s and b.id = %(id)s)
"""

NEXT_WAVE_SQL = """
create temp table tmp_next as
select * from ssdf.batch_log b where b.id_batch = %(batch)s and b.executed is null and b.enabled = true and vw_ordr <= %(ordr)s
"""

NEXT_FILTER_SQL = """
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
            on tmp_next2.id <> tmp_next.id and tmp_next2.vw_ordr <= tmp_next.vw_ordr_max)
"""

NEXT_CANDIDATE_SQL = """
select id from tmp_next b order by priority, vw_ordr, id limit 1 offset %(offs)s
"""

CLAIM_SQL = """
update ssdf.batch_log
set executed = 'running', startdate = CURRENT_TIMESTAMP
where id_batch = %(batch)s and id = %(id)s
"""


def _connect(conninfo):
    return psycopg.connect(**conninfo, autocommit=True)


def _point_array(points):
    if points is None:
        return None
    elems = []
    for name, tier in points:
        comp = "(" + ",".join('"' + f.replace("\\", "\\\\").replace('"', '\\"') + '"' for f in (name, tier)) + ")"
        elems.append('"' + comp.replace("\\", "\\\\").replace('"', '\\"') + '"')
    return "{" + ",".join(elems) + "}"


def _dbg(cur, id_batch, id, msg):
    cur.execute("insert into ssdf.dbg select %s, %s, CURRENT_TIMESTAMP::text || %s", (id_batch, id, msg))


class _Workers:
    def __init__(self):
        self._cond = threading.Condition()
        self._active = 0

    def spawn(self, fn, *args):
        with self._cond:
            self._active += 1
        threading.Thread(target=self._run, args=(fn, args), daemon=True).start()

    def _run(self, fn, args):
        try:
            fn(*args)
        except Exception:
            log.exception("worker failed")
        finally:
            with self._cond:
                self._active -= 1
                self._cond.notify_all()

    def wait(self):
        with self._cond:
            while self._active:
                self._cond.wait()


def _launch(cur, conninfo, id_batch, id, mode, use_rep_dttm, workers):
    # claim before spawning: siblings check batch_log under the same lock
    cur.execute(CLAIM_SQL, {"batch": id_batch, "id": id})
    workers.spawn(exec_batch_from_id, conninfo, id_batch, id, mode, use_rep_dttm, workers)


def exec_batch_from_id(conninfo, id_batch, id, mode, use_rep_dttm, workers):
    params = {"batch": id_batch, "id": id, "mode": mode, "use_rep_dttm": use_rep_dttm}
    with _connect(conninfo) as conn:
        cur = conn.cursor()
        _dbg(cur, id_batch, id, " start")

        # outside the lock: this is what lets sibling points run in parallel
        cur.execute("CALL ssdf.exec_id(%s, %s, NULL)", (id_batch, id))
        strt_dt = cur.fetchone()[0]
        log.info("batch %s point %s executed", id_batch, id)

        cur.execute("select pg_advisory_lock(hashtext(%s)::bigint)", (LOCK_NAME,))
        _dbg(cur, id_batch, id, " lock")
        try:
            cur.execute(UPDATE_EXECUTED_SQL, {**params, "strt_dt": strt_dt})

            cur.execute("select executed from ssdf.batch_log where id_batch = %(batch)s and id = %(id)s", params)
            if cur.fetchone()[0] == "error":
                cur.execute("create temp table tmp_exclude (id int)")
                while True:
                    cur.execute(EXCLUDE_NEXT_SQL, params)
                    if cur.rowcount == 0:
                        break
                cur.execute("""
                    update ssdf.batch_log as b
                    set enabled = false
                    where id_batch = %(batch)s and id in (select id from tmp_exclude)
                """, params)
                cur.execute("drop table if exists tmp_exclude")

            cur.execute(WAVE_BUSY_SQL, params)
            if cur.fetchone() is None:
                cur.execute(NEXT_ORDR_SQL, params)
                ordr = cur.fetchone()[0]
                if ordr is None:
                    cur.execute("select vw_ordr from ssdf.batch_log where id_batch = %(batch)s and id = %(id)s", params)
                    ordr = cur.fetchone()[0]

                cur.execute(NEXT_WAVE_SQL, {**params, "ordr": ordr})
                cur.execute(NEXT_FILTER_SQL)
                cur.execute("delete from tmp_next where id not in (select id from tmp_next_filter)")

                offs = 0
                while True:
                    cur.execute(NEXT_CANDIDATE_SQL, {**params, "offs": offs})
                    row = cur.fetchone()
                    if row is None:
                        break
                    curid = row[0]
                    cur.execute("select ssdf.get_for_resource(%s)", (curid,))
                    if cur.fetchone()[0]:
                        _dbg(cur, id_batch, id, " first " + str(curid))
                        _launch(cur, conninfo, id_batch, curid, mode, use_rep_dttm, workers)
                    offs += 1

                cur.execute("select id from tmp_next b order by priority, vw_ordr, id limit 1")
                row = cur.fetchone()
                cur.execute("select 1 from ssdf.batch_log b where b.id_batch = %(batch)s and executed = 'running'", params)
                nothing_running = cur.fetchone() is None
                if row is not None and nothing_running:
                    curid = row[0]
                    cur.execute("call ssdf.wait_for_resource(%s)", (curid,))
                    _dbg(cur, id_batch, id, " second " + str(curid))
                    _launch(cur, conninfo, id_batch, curid, mode, use_rep_dttm, workers)

                cur.execute("drop table if exists tmp_next")
                cur.execute("drop table if exists tmp_next_filter")
        finally:
            cur.execute("select pg_advisory_unlock(hashtext(%s)::bigint)", (LOCK_NAME,))

        _dbg(cur, id_batch, id, " end")


def exec_flow(conninfo, mode, procs=None, start_ids=None, dests=None, excls=None, use_rep_dttm=True):
    if mode not in ("full", "strict"):
        raise ValueError("mode could be only full and strict")

    params = {
        "mode": mode,
        "procs": procs,
        "start_ids": start_ids,
        "dests": _point_array(dests),
        "excls": _point_array(excls),
        "use_rep_dttm": use_rep_dttm,
    }
    workers = _Workers()

    with _connect(conninfo) as conn:
        cur = conn.cursor()
        cur.execute("drop table if exists tmp_plan_list")
        cur.execute("drop table if exists tmp_plan")

        cur.execute(PLAN_START_SQL, params)
        while True:
            cur.execute(PLAN_NEXT_SQL, params)
            if cur.rowcount == 0:
                break

        cur.execute(PLAN_ORDER_SQL)

        # parallel dispatchers must not pick the same max+1; released right
        # after the plan insert, never held during point execution
        cur.execute("select pg_advisory_lock(hashtext(%s)::bigint)", (BATCH_LOCK_NAME,))
        try:
            cur.execute("select coalesce(max(id_batch), 0::bigint) + 1 from ssdf.batch_log")
            id_batch = cur.fetchone()[0]
            cur.execute(BATCH_INSERT_SQL, {**params, "batch": id_batch})
        finally:
            cur.execute("select pg_advisory_unlock(hashtext(%s)::bigint)", (BATCH_LOCK_NAME,))

        cur.execute("select id, vw_ordr from tmp_plan order by vw_ordr, priority")
        for pid, ordr in cur.fetchall():
            log.info("(id,vw_ordr): (%s,%s)", pid, ordr)

        cur.execute("""
            select id from tmp_plan
            where vw_ordr = (select min(vw_ordr) from tmp_plan)
            order by priority, id
        """)
        first = True
        for (pid,) in cur.fetchall():
            if first:
                cur.execute("call ssdf.wait_for_resource(%s)", (pid,))
                first = False
            else:
                cur.execute("select ssdf.get_for_resource(%s)", (pid,))
                if not cur.fetchone()[0]:
                    continue
            _launch(cur, conninfo, id_batch, pid, mode, use_rep_dttm, workers)

        cur.execute("drop table tmp_plan_list")
        cur.execute("drop table tmp_plan")

    workers.wait()

    with _connect(conninfo) as conn:
        cur = conn.cursor()
        cur.execute("""
            select b.id, f.dest, b.executed, b.enabled
            from ssdf.batch_log b
                join ssdf.vw_flow f on f.id = b.id
            where b.id_batch = %s
            order by b.vw_ordr, b.priority, b.id
        """, (id_batch,))
        for pid, dest, executed, enabled in cur.fetchall():
            log.info("batch %s result: id=%s dest=%s executed=%s enabled=%s", id_batch, pid, dest, executed, enabled)

    return id_batch


def _parse_points(values):
    if values is None:
        return None
    points = []
    for v in values:
        name, tier = v.rsplit(":", 1)
        points.append((name, tier))
    return points


def main():
    p = argparse.ArgumentParser(description="SSDF data flow dispatcher")
    p.add_argument("--mode", required=True, choices=("full", "strict"))
    p.add_argument("--procs", nargs="+", help="process names to limit the flow")
    p.add_argument("--start-ids", nargs="+", type=int, help="flow point ids to start from")
    p.add_argument("--dests", nargs="+", metavar="NAME:TIER", help="destinations not to generate the flow after")
    p.add_argument("--excls", nargs="+", metavar="NAME:TIER", help="points to exclude")
    p.add_argument("--no-rep-dttm", action="store_true", help="do not use rep dates for dependency calculation")
    p.add_argument("--dbname", required=True)
    p.add_argument("--host")
    p.add_argument("--port", type=int)
    p.add_argument("--user")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(threadName)s %(levelname)s %(message)s")

    conninfo = {"dbname": args.dbname}
    if args.host:
        conninfo["host"] = args.host
    if args.port:
        conninfo["port"] = args.port
    if args.user:
        conninfo["user"] = args.user

    exec_flow(conninfo, args.mode, args.procs, args.start_ids,
              _parse_points(args.dests), _parse_points(args.excls), not args.no_rep_dttm)


if __name__ == "__main__":
    main()
