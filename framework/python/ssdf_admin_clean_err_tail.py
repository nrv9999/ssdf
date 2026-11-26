#!/usr/bin/env python3
"""Админ-закрытие зависших running-точек после системного сбоя.

Пользовательские ошибки SQL-workload обрабатывает ssdf.exec_id сам (error + пропагация).
Системный сбой (обрыв сети, ребут сервера, сбой самого диспетчера) может оставить
точку в batch_log.executed='running' без живого потока — такие точки закрываются
вручную этим скриптом в error с _sqlerrm='system fault'.

Интерактивно: список running с номерами → выбрать закрываемые (0 — все, пусто — отмена).
SQL-аналог: ssdf.list_running() / ssdf.close_running(int[]).

Запуск: ssdf_admin_clean_err_tail (после install.sh) или python3 framework/python/ssdf_admin_clean_err_tail.py
"""

import argparse
import sys

import psycopg
from psycopg.rows import dict_row

LIST_SQL = """
select row_number() over (order by b.id_batch, b.id)::int as num,
    b.id_batch, b.id, f.source, f.dest, b.startdate
from ssdf.batch_log b
    join ssdf.flow f on f.id = b.id
where b.executed = 'running'
order by b.id_batch, b.id
"""

CLOSE_SQL = """
with numbered as (
    select row_number() over (order by b.id_batch, b.id) as num, b.id_batch, b.id
    from ssdf.batch_log b
        join ssdf.flow f on f.id = b.id
    where b.executed = 'running'
), picked as (
    select n.id_batch, n.id from numbered n
    where n.num = any(%(nums)s::bigint[]) or 0 = any(%(nums)s::bigint[])
), upd_exec as (
    update ssdf.exec_log l
    set executed = 'error', _sqlerrm = 'system fault', enddate = CURRENT_TIMESTAMP
    where l.executed = 'running' and (l.id_batch, l.id) in (select p.id_batch, p.id from picked p)
    returning l.id_batch, l.id
), upd_batch as (
    update ssdf.batch_log b
    set executed = 'error', _sqlerrm = 'system fault'
    where b.executed = 'running' and (b.id_batch, b.id) in (select p.id_batch, p.id from picked p)
    returning b.id_batch, b.id
)
select u.id_batch, u.id, f.source, f.dest
from upd_batch u
    join ssdf.flow f on f.id = u.id
"""


def list_running(conn):
    return conn.execute(LIST_SQL).fetchall()


def close_running(conn, nums):
    return conn.execute(CLOSE_SQL, {"nums": nums}).fetchall()


def main():
    p = argparse.ArgumentParser(description="Закрытие зависших running-точек (system fault)")
    p.add_argument("--dbname", default="demo")
    p.add_argument("--host")
    p.add_argument("--port", type=int)
    p.add_argument("--user")
    args = p.parse_args()

    conninfo = {"dbname": args.dbname}
    if args.host:
        conninfo["host"] = args.host
    if args.port:
        conninfo["port"] = args.port
    if args.user:
        conninfo["user"] = args.user

    with psycopg.connect(**conninfo, autocommit=True, row_factory=dict_row) as conn:
        rows = list_running(conn)
        if not rows:
            print("нет running-точек")
            return
        print("running-точки:")
        for r in rows:
            print(f"  {r['num']}) batch {r['id_batch']}: {r['source']} -> {r['dest']} (running с {r['startdate']})")

        raw = input("номера для закрытия (0 — все, пусто — отмена): ").strip()
        if not raw:
            print("отмена")
            return
        try:
            nums = [int(x) for x in raw.replace(",", " ").split()]
        except ValueError:
            print("ошибка: ожидались числа")
            sys.exit(1)
        if not set(nums) <= {r["num"] for r in rows} | {0}:
            print(f"ошибка: допустимы номера 1..{len(rows)} или 0")
            sys.exit(1)

        closed = close_running(conn, nums)
        for r in closed:
            print(f"закрыто (system fault): batch {r['id_batch']}: {r['source']} -> {r['dest']}")
        if not closed:
            print("нечего закрывать (точки уже не running)")


if __name__ == "__main__":
    main()
