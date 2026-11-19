"""报送补正 / 作废 CSV 的解析与校验。

每行字段：
- 报送ID：一条报送记录的逻辑标识（同一 ID 可有多版修订）
- 修订号：正整数；同一 ID 只以最高修订号贡献汇总
- 分区、年龄组、人数、检出数
- 作废标记：是/否（也接受 1/0、true/false，留空视为“否”）

有效记录必须满足 0 <= 检出数 <= 人数。
任一行非法时由调用方整批拒收，不做部分写入。
"""

import csv
import io
import re

# 表头别名（统一 strip BOM / 空白 / 小写后比较）
_HEADER_ALIASES = {
    "submission_id": {
        "报送id", "报送编号", "submission_id", "submissionid", "report_id",
        "reportid", "record_id", "recordid", "id",
    },
    "revision": {"修订号", "修订", "revision", "rev", "version"},
    "district": {"分区", "地区", "区域", "district"},
    "age_group": {"年龄组", "年龄", "age_group", "agegroup", "age"},
    "population": {"人数", "population", "pop"},
    "detected": {"检出数", "检出", "detected", "count"},
    "voided": {
        "作废标记", "作废", "voided", "void", "is_void", "cancelled", "canceled",
    },
}

_VOID_TRUE = {
    "1", "true", "yes", "y", "是", "作废", "已作废", "void", "voided",
    "cancel", "cancelled", "canceled",
}
_VOID_FALSE = {
    "", "0", "false", "no", "n", "否", "有效", "正常", "未作废", "active",
}

_INT_RE = re.compile(r"^[+-]?\d+$")


class SubmissionConflict(Exception):
    """同一（报送ID, 修订号）出现不同内容：整批拒收。"""


def _norm_header(value: str) -> str:
    return str(value).strip().lstrip("﻿").lower()


def _parse_int(raw: str, field: str, lineno: int) -> int:
    text = str(raw).strip()
    if not _INT_RE.match(text):
        raise ValueError(f"第 {lineno} 行{field}不是整数：{raw!r}")
    return int(text)


def _parse_void(raw: str, lineno: int) -> bool:
    text = str(raw).strip().lower()
    if text in _VOID_TRUE:
        return True
    if text in _VOID_FALSE:
        return False
    raise ValueError(
        f"第 {lineno} 行作废标记无法识别：{raw!r}"
        "（可用：是/否、1/0、true/false，留空视为否）"
    )


def parse_submission_csv(text: str) -> list[dict]:
    """解析并逐行校验报送 CSV；任一行非法即抛 ValueError。"""
    reader = csv.reader(io.StringIO(text))
    rows = [r for r in reader if any(str(cell).strip() for cell in r)]
    if not rows:
        raise ValueError("CSV 为空")

    header = [_norm_header(h) for h in rows[0]]
    col: dict[str, int] = {}
    for field, aliases in _HEADER_ALIASES.items():
        aliases = {_norm_header(a) for a in aliases}
        for idx, name in enumerate(header):
            if name in aliases:
                col[field] = idx
                break
        if field not in col:
            raise ValueError(f"缺少必需列：{field}（可用表头：{sorted(aliases)}）")

    out: list[dict] = []
    for lineno, row in enumerate(rows[1:], start=2):
        try:
            submission_id = row[col["submission_id"]].strip()
            revision_s = row[col["revision"]].strip()
            district = row[col["district"]].strip()
            age_group = row[col["age_group"]].strip()
            population_s = row[col["population"]].strip()
            detected_s = row[col["detected"]].strip()
            void_s = row[col["voided"]].strip()
        except IndexError:
            raise ValueError(f"第 {lineno} 行列数不足")

        if not submission_id:
            raise ValueError(f"第 {lineno} 行报送ID为空")
        if not district or not age_group:
            raise ValueError(f"第 {lineno} 行分区或年龄组为空")

        revision = _parse_int(revision_s, "修订号", lineno)
        if revision <= 0:
            raise ValueError(f"第 {lineno} 行修订号须为正整数：{revision}")

        population = _parse_int(population_s, "人数", lineno)
        detected = _parse_int(detected_s, "检出数", lineno)
        if population < 0:
            raise ValueError(f"第 {lineno} 行人数不能为负数：{population}")
        if detected < 0:
            raise ValueError(f"第 {lineno} 行检出数不能为负数：{detected}")
        if detected > population:
            raise ValueError(
                f"第 {lineno} 行检出数 {detected} 大于人数 {population}"
                "（有效记录须满足 0 ≤ 检出数 ≤ 人数）"
            )

        out.append(
            {
                "line": lineno,
                "submission_id": submission_id,
                "revision": revision,
                "district": district,
                "age_group": age_group,
                "population": population,
                "detected": detected,
                "voided": _parse_void(void_s, lineno),
            }
        )

    if not out:
        raise ValueError("没有数据行")
    return out


def row_content(row: dict) -> tuple:
    """用于判断“同 ID 同修订”是否内容一致的规范化字段元组。"""
    return (
        row["district"],
        row["age_group"],
        int(row["population"]),
        int(row["detected"]),
        int(bool(row["voided"])),
    )
