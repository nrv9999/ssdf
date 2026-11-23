#!/usr/bin/env python3
"""Прогоны диспетчера ssdf.py на demo-легенде.

Запуск из корня репозитория:
    python3 example/test/python/all_execs.py [--no-reset]
или после установки в /opt/ssdf (см. install.sh в корне репозитория):
    python3 /opt/ssdf/test/example/all_execs.py [--no-reset]

По умолчанию каждый сценарий сбрасывает лог-таблицы df; --no-reset оставляет состояние
между прогонами (для инспекции/отладки — ожидания сценариев рассчитаны на чистое состояние,
с накопленными watermarks часть проверок может не совпасть).
Точки именуются source -> destination.
Ожидаемые error из-за дыр легенды: vw_main_dwh_payment -> dwh.dwh_payment (нет объектов)
и vw_dwh_debt -> dwh.dwh_debt (нет cond_col).
"""

import argparse
import logging
import sys
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

# repo layout: <root>/framework/python/ssdf.py; installed: /opt/ssdf/ssdf.py (test in /opt/ssdf/test/example)
_HERE = Path(__file__).resolve()
for _cand in (_HERE.parents[3] / "framework" / "python", _HERE.parents[2], _HERE.parents[1]):
    if (_cand / "ssdf.py").exists():
        sys.path.insert(0, str(_cand))
        break
else:
    raise SystemExit("ssdf.py не найден ни в layout репозитория, ни рядом с test/")
import ssdf
import ssdf_admin_clean_err_tail

CONNINFO = {"dbname": "demo"}
RESET = True

STRT_CE = "dwh.usp_sa_load_contact_collection_event"
STRT_DEBT = "dwh.usp_sa_load_contact_debt"
SA_CE = "dwh.sa_collection_event"
SA_DEBT = "dwh.sa_debt"
H_CE = "dwh.h_collection_event"
H_DEBT = "dwh.h_debt"
SRS_CE = "dwh.srs_collection_event"
GLB_CE = "dwh.glb_collection_event"
SRS_DEBT = "dwh.srs_debt"
GLB_DEBT = "dwh.glb_debt"
DWH_CE = "dwh.dwh_collection_event"
DWH_DEBT = "dwh.dwh_debt"
PAYMENT = "dwh.dwh_payment"
AMS = "dwh.ams_collection_event"
ALL = {STRT_CE, STRT_DEBT, SA_CE, SA_DEBT, H_CE, H_DEBT, SRS_CE, GLB_CE,
       SRS_DEBT, GLB_DEBT, DWH_CE, DWH_DEBT, PAYMENT, AMS}

STRT_CE_ID = 1    # uf_sa_wait_contact_collection_event -> usp_sa_load_contact_collection_event
STRT_DEBT_ID = 2  # uf_sa_wait_contact_debt -> usp_sa_load_contact_debt
SA_DEBT_ID = 4    # usp_sa_load_contact_debt -> sa_debt


def _conn():
    return psycopg.connect(**CONNINFO, autocommit=True, row_factory=dict_row)


def reset():
    if not RESET:
        return
    with _conn() as conn:
        conn.execute("truncate df.batch_log, df.exec_log, df.dbg, df.flow_rep_dttm, df.flow_rep_dttm_clc")


def run(mode, **kwargs):
    return ssdf.exec_flow(CONNINFO, mode, **kwargs)


def results(id_batch):
    with _conn() as conn:
        cur = conn.execute("""
            select f.source, f.dest, b.executed, b.enabled,
                (select count(*) from df.exec_log l where l.id_batch = b.id_batch and l.id = b.id) as log_rows
            from df.batch_log b
                join df.flow f on f.id = b.id
            where b.id_batch = %s
        """, (id_batch,))
        return {r["dest"]: r for r in cur.fetchall()}


def plan_is(d, dests):
    diff = set(d) ^ set(dests)
    assert not diff, f"расхождение плана: {sorted(diff)}"


def expect(d, dest, executed, enabled, log_rows=None):
    assert dest in d, f"{dest} нет в плане"
    r = d[dest]
    label = f"{r['source']} -> {dest}"
    assert r["executed"] == executed and r["enabled"] == enabled, \
        f"{label}: executed={r['executed']} enabled={r['enabled']}, ожидалось {executed}/{enabled}"
    if log_rows is not None:
        assert r["log_rows"] == log_rows, f"{label}: записей в exec_log {r['log_rows']}, ожидалось {log_rows}"


def strict_full():
    """strict: ошибка любого входа отключает зависимых до запуска.
    vw_ams_collection_event -> dwh.ams_collection_event отключён из-за ошибки payment:
    enabled=false, executed=null, записи в exec_log нет."""
    d = results(run("strict"))
    plan_is(d, ALL)
    expect(d, AMS, None, False, log_rows=0)
    expect(d, PAYMENT, "error", True)
    expect(d, DWH_DEBT, "error", True)
    expect(d, DWH_CE, "done", True)
    assert sum(r["executed"] == "done" for r in d.values()) == 11


def full_full():
    """full: зависимый отключается, только если «упали» ВСЕ входы.
    vw_ams_collection_event -> dwh.ams_collection_event остаётся enabled,
    доходит до последней волны и падает сам (запись в exec_log есть)."""
    d = results(run("full"))
    plan_is(d, ALL)
    expect(d, AMS, "error", True, log_rows=1)
    assert sum(r["executed"] == "done" for r in d.values()) == 11


def dests_stop():
    """--dests: не генерировать поток после указанных назначений.
    План останавливается после sa-загрузок: StrtF + usp_sa_load_* -> dwh.sa_*
    (+ payment, у него нет зависимостей — он всегда стартовый)."""
    d = results(run("strict", dests=[(SA_CE, "f"), (SA_DEBT, "f")]))
    plan_is(d, {STRT_CE, STRT_DEBT, PAYMENT, SA_CE, SA_DEBT})


def procs_filter():
    """--procs: только процесс Collection. payment (процесс Main) вне плана;
    ams выпадает сам: strict требует все входы в батче, а dwh_payment не в батче."""
    d = results(run("strict", procs=["Collection"]))
    plan_is(d, ALL - {PAYMENT, AMS})
    expect(d, DWH_DEBT, "error", True)
    assert sum(r["executed"] == "done" for r in d.values()) == 11


def start_ids_debt():
    """--start-ids: старт с указанных точек вместо deps-null стартов.
    Только debt-ветка от usp_sa_load_contact_debt -> dwh.sa_debt.
    dwh.sa_debt -> dwh.h_debt error: REP_DATE NOT NULL, а StrtF не запускался → rep-дата NULL."""
    d = results(run("strict", start_ids=[SA_DEBT_ID]))
    plan_is(d, {SA_DEBT, H_DEBT, SRS_DEBT, GLB_DEBT, DWH_DEBT})
    expect(d, H_DEBT, "error", True)
    expect(d, DWH_DEBT, "error", True)


def partial_refresh():
    """Разница full/strict в составе плана при частичном обновлении watermarks.
    После полного прогона обновляем только debt-ветку (старт с StrtF debt):
    full пересобирает vw_dwh_collection_event -> dwh.dwh_collection_event (обновился ЛЮБОЙ вход),
    strict — нет (не все входы свежие/в батче)."""
    run("strict")
    d = results(run("full", start_ids=[STRT_DEBT_ID]))
    assert DWH_CE in d, "full: dwh_collection_event должен быть в плане"
    d = results(run("strict", start_ids=[STRT_DEBT_ID]))
    plan_is(d, {STRT_DEBT, SA_DEBT, H_DEBT, SRS_DEBT, GLB_DEBT, DWH_DEBT})


def no_rep_dttm():
    """--no-rep-dttm: watermarks по-прежнему ЗАПИСЫВАЮТСЯ (exec_id всегда пишет
    flow_rep_dttm/clc), но не учитываются в расчёте зависимостей — только точки батча.
    После полного прогона + обновления debt-ветки: старт с StrtF collection
    без watermarks не тянет dwh_collection_event (debt-входы не в батче), с watermarks — тянет."""
    run("strict")
    run("strict", start_ids=[STRT_DEBT_ID])
    b = run("strict", start_ids=[STRT_CE_ID], use_rep_dttm=False)
    d = results(b)
    assert DWH_CE not in d, "no-rep-dttm: dwh_collection_event не должен быть в плане"
    with _conn() as conn:
        n = conn.execute("select count(*) as n from df.flow_rep_dttm_clc where id_batch = %s", (b,)).fetchone()["n"]
    assert n > 0, "журнал flow_rep_dttm_clc должен заполняться и при --no-rep-dttm"
    d = results(run("strict", start_ids=[STRT_CE_ID]))
    assert DWH_CE in d, "с watermarks dwh_collection_event должен быть в плане"


def excls_exclude():
    """--excls: исключение точек (условие пофикшено против инверсии в оригинальном df.exec_flow).
    Кейс 1: исключить vw_main_dwh_payment -> dwh.dwh_payment — ams выпадает сам (strict: нет входа).
    Кейс 2: исключить usp_srs_collection_event -> dwh.srs_collection_event — проверка фильтра и в замыкании:
    точка не возвращается через зависимости, glb/dwh_collection_event и ams выпадают следом."""
    d = results(run("strict", excls=[(PAYMENT, "f")]))
    plan_is(d, ALL - {PAYMENT, AMS})
    expect(d, DWH_DEBT, "error", True)
    reset()
    d = results(run("strict", excls=[(SRS_CE, "f")]))
    plan_is(d, ALL - {SRS_CE, GLB_CE, DWH_CE, AMS})
    expect(d, PAYMENT, "error", True)
    expect(d, DWH_DEBT, "error", True)


def admin_fault():
    """Админ-закрытие зависших running после системного сбоя (оба варианта).
    Имитация: usp_sa_load_* -> dwh.sa_* батча 999 остались running без живого потока.
    SQL: df.list_running() нумерует, df.close_running(array[num]) закрывает в error/'system fault';
    Python: ssdf_admin_clean_err_tail.close_running(conn, [0]) закрывает все оставшиеся.
    Фантомная running-запись exec_log тоже закрывается."""
    with _conn() as conn:
        conn.execute("""
            insert into df.batch_log (id_batch, id, vw_ordr, vw_ordr_max, priority, loaddate, executed, startdate)
            select 999, id, vw_ordr, vw_ordr_max, priority, CURRENT_TIMESTAMP, 'running', CURRENT_TIMESTAMP
            from df.vw_flow where dest in (%s, %s)
        """, (SA_CE, SA_DEBT))
        conn.execute("""
            insert into df.exec_log (id_batch, id, startdate, executed, command)
            select 999, id, CURRENT_TIMESTAMP, 'running', 'phantom'
            from df.flow where dest = %s
        """, (SA_CE,))
        rows = conn.execute("select * from df.list_running()").fetchall()
        assert [r["num"] for r in rows] == [1, 2]
        assert {r["dest"] for r in rows} == {SA_CE, SA_DEBT}
        closed = conn.execute("select * from df.close_running(array[%s])", (rows[0]["num"],)).fetchall()
        assert len(closed) == 1 and closed[0]["dest"] == rows[0]["dest"]
        r = conn.execute("select executed, _sqlerrm from df.batch_log where id_batch = 999 and id = %s",
                         (rows[0]["id"],)).fetchone()
        assert r["executed"] == "error" and r["_sqlerrm"] == "system fault"
        r = conn.execute("select executed, _sqlerrm, enddate from df.exec_log where id_batch = 999 and id = %s",
                         (rows[0]["id"],)).fetchone()
        assert r["executed"] == "error" and r["_sqlerrm"] == "system fault" and r["enddate"] is not None
        closed = ssdf_admin_clean_err_tail.close_running(conn, [0])
        assert len(closed) == 1 and closed[0]["dest"] == rows[1]["dest"]
        assert conn.execute("select count(*) as n from df.batch_log where id_batch = 999 and executed = 'running'").fetchone()["n"] == 0
        assert ssdf_admin_clean_err_tail.close_running(conn, [0]) == []
        conn.execute("delete from df.exec_log where id_batch = 999")
        conn.execute("delete from df.batch_log where id_batch = 999")


SCENARIOS = [strict_full, full_full, dests_stop, procs_filter,
             start_ids_debt, partial_refresh, no_rep_dttm, excls_exclude, admin_fault]


def main():
    global RESET
    p = argparse.ArgumentParser(description="Прогоны диспетчера ssdf.py на demo-легенде")
    p.add_argument("--no-reset", action="store_true",
                   help="не очищать лог-таблицы df между прогонами (ожидания рассчитаны на чистое состояние)")
    args = p.parse_args()
    RESET = not args.no_reset
    logging.basicConfig(level=logging.WARNING)
    failed = []
    for fn in SCENARIOS:
        reset()
        try:
            fn()
        except (AssertionError, KeyError) as e:
            failed.append(fn.__name__)
            print(f"FAIL {fn.__name__}: {e}")
        else:
            print(f"PASS {fn.__name__}")
    print(f"{len(SCENARIOS) - len(failed)}/{len(SCENARIOS)} PASS")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
