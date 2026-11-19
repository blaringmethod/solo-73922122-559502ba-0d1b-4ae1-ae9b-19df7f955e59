"""SQLite 持久化层。

- cells：导入的调查格值（导入后不可更换，无更新/删除接口）。
- publications：已获准公开的汇总记录（不可撤销，只插入，无更新/删除接口）。
- submissions：报送明细（按 ID+修订号去重；每 ID 仅一行 valid=1 的当前有效版）。
- submission_batches：每批上传的审计记录（拒收的批次不落库）。

所有写操作均在单事务中完成：失败即整体回滚，不会留下半次发布或半批报送。
连接按操作创建，启用 WAL，重启后数据保留在 DB_PATH 指定的库文件中。
"""

import json
import os
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from .submissions import SubmissionConflict, row_content

_DEFAULT_DB = Path(__file__).resolve().parent.parent / "data" / "app.db"
DB_PATH = os.environ.get("DB_PATH", str(_DEFAULT_DB))

_SCHEMA = """
CREATE TABLE IF NOT EXISTS cells (
    district   TEXT NOT NULL,
    age_group  TEXT NOT NULL,
    population INTEGER NOT NULL CHECK (population >= 5),
    detected   INTEGER NOT NULL CHECK (detected >= 0 AND detected <= population),
    PRIMARY KEY (district, age_group)
);
CREATE TABLE IF NOT EXISTS publications (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    districts  TEXT NOT NULL,          -- JSON 数组
    age_groups TEXT NOT NULL,          -- JSON 数组
    total      INTEGER NOT NULL CHECK (total >= 0),
    created_at TEXT NOT NULL           -- ISO8601 UTC
);

-- 报送明细：按（报送ID, 修订号）去重落库，作为“已提交记录”的唯一事实源。
-- valid 为 1 表示该版是本 ID 已接收的最高修订（有效版本）；
-- 更高修订到达后会把它置为 0（迟到旧版不会复活）。
CREATE TABLE IF NOT EXISTS submissions (
    submission_id TEXT NOT NULL,
    revision      INTEGER NOT NULL CHECK (revision > 0),
    district      TEXT NOT NULL,
    age_group     TEXT NOT NULL,
    population    INTEGER NOT NULL CHECK (population >= 0),
    detected      INTEGER NOT NULL CHECK (detected >= 0 AND detected <= population),
    voided        INTEGER NOT NULL CHECK (voided IN (0, 1)),
    valid         INTEGER NOT NULL CHECK (valid IN (0, 1)),
    batch_id      INTEGER NOT NULL,
    received_at   TEXT NOT NULL,       -- ISO8601 UTC
    PRIMARY KEY (submission_id, revision)
);
CREATE INDEX IF NOT EXISTS idx_submissions_valid
    ON submissions (valid, district, age_group);

-- 每批上传的审计记录：仅记录已接收的批次（拒收批次整体回滚，不落库）。
CREATE TABLE IF NOT EXISTS submission_batches (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    filename      TEXT NOT NULL,
    rows_accepted INTEGER NOT NULL,
    rows_duplicate INTEGER NOT NULL,
    created_at    TEXT NOT NULL
);
"""


def connect() -> sqlite3.Connection:
    path = DB_PATH
    if path != ":memory:":
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db() -> None:
    with closing(connect()) as conn, conn:
        conn.executescript(_SCHEMA)


def get_cells() -> list[dict]:
    with closing(connect()) as conn:
        rows = conn.execute(
            "SELECT district, age_group, population, detected FROM cells "
            "ORDER BY district, age_group"
        ).fetchall()
    return [dict(r) for r in rows]


def insert_cells(rows: list[dict]) -> None:
    """单事务写入全部格值；任一失败则整体回滚。组合重复抛 IntegrityError。"""
    with closing(connect()) as conn, conn:
        conn.executemany(
            "INSERT INTO cells (district, age_group, population, detected) "
            "VALUES (:district, :age_group, :population, :detected)",
            rows,
        )


def get_publications() -> list[dict]:
    with closing(connect()) as conn:
        rows = conn.execute(
            "SELECT id, districts, age_groups, total, created_at "
            "FROM publications ORDER BY id"
        ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["districts"] = json.loads(d["districts"])
        d["age_groups"] = json.loads(d["age_groups"])
        out.append(d)
    return out


def insert_publication(districts: list[str], age_groups: list[str], total: int) -> dict:
    """单事务插入一条公开记录；失败回滚，不会留下半次发布。"""
    created = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with closing(connect()) as conn, conn:
        cur = conn.execute(
            "INSERT INTO publications (districts, age_groups, total, created_at) "
            "VALUES (?, ?, ?, ?)",
            (
                json.dumps(list(districts), ensure_ascii=False),
                json.dumps(list(age_groups), ensure_ascii=False),
                int(total),
                created,
            ),
        )
        pid = cur.lastrowid
    return {
        "id": pid,
        "districts": list(districts),
        "age_groups": list(age_groups),
        "total": int(total),
        "created_at": created,
    }


# ---------------- 报送补正 / 作废 ----------------


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def accept_submissions(filename: str, rows: list[dict]) -> dict:
    """在一个事务内接收一批报送；失败整体回滚，旧结果不变。

    规则：
    - 同（ID, 修订号）重传且内容相同 → 幂等，不重复计数；
    - 同（ID, 修订号）重传但内容冲突 → 抛 SubmissionConflict，整批拒收；
    - 高于该 ID 现有最高修订 → 该版成为有效版本，旧有效版置为无效；
    - 低于现有最高修订（迟到旧版）→ 仅留档为无效历史，不改变汇总；
    - 作废版成为有效版本时撤去该 ID 的全部贡献。
    返回 {batch_id, rows_accepted, rows_duplicate, rows_superseded, effective_ids}。
    """
    received = _now()
    conn = connect()
    conn.isolation_level = None  # 手动控制事务边界
    committed = False
    try:
        conn.execute("BEGIN IMMEDIATE")  # 串行化同库并发上传
        cur = conn.execute(
            "INSERT INTO submission_batches "
            "(filename, rows_accepted, rows_duplicate, created_at) "
            "VALUES (?, 0, 0, ?)",
            (filename, received),
        )
        batch_id = cur.lastrowid

        existing = {
            (r["submission_id"], r["revision"]): r
            for r in conn.execute(
                "SELECT submission_id, revision, district, age_group, population, "
                "detected, voided FROM submissions"
            )
        }
        max_rev: dict[str, int] = {}
        for (sid, rev) in existing:
            max_rev[sid] = max(max_rev.get(sid, 0), rev)

        accepted = duplicate = superseded = 0
        served_ids: set[str] = set()
        seen: dict[tuple[str, int], dict] = dict(existing)
        for row in rows:
            key = (row["submission_id"], row["revision"])
            if key in seen:
                if row_content(seen[key]) != row_content(row):
                    raise SubmissionConflict(
                        f"报送 {row['submission_id']} 修订 {row['revision']} "
                        "与已提交记录内容冲突，整批拒收"
                    )
                duplicate += 1
                continue

            # 迟到旧版：留档但无效，绝不复活。
            is_highest = row["revision"] >= max_rev.get(row["submission_id"], 0)
            suppress_prior = is_highest and max_rev.get(row["submission_id"], 0) > 0
            if is_highest:
                max_rev[row["submission_id"]] = row["revision"]
                served_ids.add(row["submission_id"])
            else:
                superseded += 1

            conn.execute(
                "INSERT INTO submissions "
                "(submission_id, revision, district, age_group, population, "
                "detected, voided, valid, batch_id, received_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    row["submission_id"],
                    row["revision"],
                    row["district"],
                    row["age_group"],
                    int(row["population"]),
                    int(row["detected"]),
                    int(bool(row["voided"])),
                    1 if is_highest else 0,
                    batch_id,
                    received,
                ),
            )
            seen[key] = row
            if suppress_prior:
                conn.execute(
                    "UPDATE submissions SET valid = 0 "
                    "WHERE submission_id = ? AND revision < ? AND valid = 1",
                    (row["submission_id"], row["revision"]),
                )
            accepted += 1

        conn.execute(
            "UPDATE submission_batches "
            "SET rows_accepted = ?, rows_duplicate = ? WHERE id = ?",
            (accepted, duplicate, batch_id),
        )
        conn.execute("COMMIT")
        committed = True
    except BaseException:
        if not committed:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
        raise
    finally:
        conn.close()

    return {
        "batch_id": batch_id,
        "rows_accepted": accepted,
        "rows_duplicate": duplicate,
        "rows_superseded": superseded,
        "effective_ids": sorted(served_ids),
    }


def _effective_submission_rows(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT submission_id, revision, district, age_group, population, "
        "detected, voided, batch_id, received_at "
        "FROM submissions WHERE valid = 1 "
        "ORDER BY submission_id"
    ).fetchall()
    return [dict(r) for r in rows]


def get_effective_submissions() -> list[dict]:
    """当前每个报送ID 的最高修订（含作废版），即“已提交记录”的当前视图。"""
    with closing(connect()) as c:
        return _effective_submission_rows(c)


def _submission_batch_rows(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT id, filename, rows_accepted, rows_duplicate, created_at "
        "FROM submission_batches ORDER BY id"
    ).fetchall()
    return [dict(r) for r in rows]


def get_submission_batches(conn: sqlite3.Connection | None = None) -> list[dict]:
    if conn is not None:
        return _submission_batch_rows(conn)
    with closing(connect()) as c:
        return _submission_batch_rows(c)


def aggregate_submissions(conn: sqlite3.Connection, cells: list[dict]) -> dict:
    """按当前有效报送逐格汇总；只统计与本调查（分区,年龄组）对齐的格。

    所有数字在同一次读取中得出，保证逐格汇总与差额对应同一批已提交记录。
    返回 {
      cells: [{district, age_group, population, detected, report, diff}],
      report_total, survey_total, total_diff,
      extra_contributions: [...],  # 范围外组合单列，不并入调查格汇总
      voided_ids, effective, effective_count
    }
    """
    known = {(c["district"], c["age_group"]): c for c in cells}
    agg: dict[tuple[str, str], dict] = {}
    extras: list[dict] = []
    voided_ids: list[str] = []
    report_total = 0
    extra_total = 0
    effective_count = 0
    effective: list[dict] = []

    for s in _effective_submission_rows(conn):
        effective_count += 1
        effective.append(
            {
                "submission_id": s["submission_id"],
                "revision": s["revision"],
                "district": s["district"],
                "age_group": s["age_group"],
                "population": s["population"],
                "detected": s["detected"],
                "voided": bool(s["voided"]),
                "batch_id": s["batch_id"],
                "received_at": s["received_at"],
            }
        )
        key = (s["district"], s["age_group"])
        if s["voided"]:
            voided_ids.append(s["submission_id"])
            continue
        if key not in known:
            extra_total += int(s["detected"])
            extras.append(
                {
                    "submission_id": s["submission_id"],
                    "revision": s["revision"],
                    "district": s["district"],
                    "age_group": s["age_group"],
                    "detected": int(s["detected"]),
                    "population": int(s["population"]),
                }
            )
            continue
        report_total += int(s["detected"])
        bucket = agg.setdefault(
            key,
            {
                "district": s["district"],
                "age_group": s["age_group"],
                "population": int(known[key]["population"]),
                "report": 0,
                "reported_count": 0,
            },
        )
        bucket["report"] += int(s["detected"])
        bucket["reported_count"] += 1

    out_cells = []
    for c in cells:
        bucket = agg.get(
            (c["district"], c["age_group"]),
            {
                "district": c["district"],
                "age_group": c["age_group"],
                "population": int(c["population"]),
                "report": 0,
                "reported_count": 0,
            },
        )
        bucket["detected"] = int(c["detected"])
        bucket["diff"] = int(bucket["report"]) - int(c["detected"])
        out_cells.append(bucket)

    survey_total = sum(int(c["detected"]) for c in cells)
    return {
        "cells": out_cells,
        "report_total": report_total,
        "survey_total": survey_total,
        "total_diff": report_total - survey_total,
        "extra_contributions": extras,
        "extra_total": extra_total,
        "voided_ids": sorted(voided_ids),
        "effective": effective,
        "effective_count": effective_count,
    }
