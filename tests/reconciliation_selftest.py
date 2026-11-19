"""报送补正 / 作废对账自测：以 TestClient 驱动应用，使用独立临时数据库。

运行：python tests/reconciliation_selftest.py

覆盖：
- CSV 逐行校验（正整数修订号、0≤检出数≤人数、作废标记、缺列）
- 任一行非法 / 同（ID,修订）内容冲突 → 整批拒收且旧结果不变
- 同 ID 同修订相同内容重传幂等；同批次内重复也不重复计数
- 每个 ID 只以最高修订贡献：改归属从旧格扣除、再计入新格
- 迟到旧版不复活；作废撤去贡献
- 逐格汇总、差额、额外组合出自同一批有效记录
- 交错 / 并发上传后汇总与有效版本一致
- 重启（新进程视角）后仍可查；原调查与已公开汇总不变
- 上报明细不进入公开页
"""

import os
import sqlite3
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

_TMP = tempfile.mkdtemp(prefix="cdc_recon_test_")
os.environ["DB_PATH"] = str(Path(_TMP) / "test.db")  # 须在导入 app 前设置

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402

SURVEY = (
    "分区,年龄组,人数,检出数\n"
    "城东,青年,10,2\n"
    "城东,老年,10,8\n"
    "城西,青年,10,8\n"
    "城西,老年,10,8\n"
)
HEADER = "报送ID,修订号,分区,年龄组,人数,检出数,作废标记\n"

results = []


def check(name, cond, extra=""):
    results.append(bool(cond))
    print(f"[{'PASS' if cond else 'FAIL'}] {name}")
    if not cond and extra:
        print("       " + extra.replace("\n", "\n       ")[:800])


def run():
    with TestClient(app) as client:
        def upload(text, name="subs.csv"):
            return client.post(
                "/api/submissions/import",
                files={"file": (name, text.encode("utf-8"), "text/csv")},
            )

        def recon():
            r = client.get("/api/reconciliation")
            assert r.status_code == 200, r.text
            return r.json()

        def cell(j, district, age):
            for c in j["cells"]:
                if c["district"] == district and c["age_group"] == age:
                    return c
            raise AssertionError(f"missing cell {district}/{age}")

        def totals(j):
            return (j["report_total"], j["survey_total"], j["total_diff"],
                    j["extra_total"], j["effective_count"])

        # ---- 0. 未导入调查时拒绝对账 / 上传 ----
        r = upload(HEADER + "A,1,城东,青年,10,1,否\n")
        check("未导入调查时上传被拒绝", r.status_code == 400, r.text)
        check("未导入调查时对账被拒绝", client.get("/api/reconciliation").status_code == 400)

        r = client.post(
            "/api/import", files={"file": ("s.csv", SURVEY.encode(), "text/csv")}
        )
        check("调查导入成功（固定基线）", r.status_code == 201, r.text)
        pubs_before = client.get("/api/public").json()["publications"]
        j = recon()
        check("空白对账：无报送、差额=-调查总数",
              totals(j) == (0, 26, -26, 0, 0), str(totals(j)))

        # ---- 1. 逐行非法 → 整批拒收 ----
        bad_rows = {
            "修订号=0": "A,1,城东,青年,10,1,否\nB,0,城东,老年,10,1,否\n",
            "修订号为负": "A,1,城东,青年,10,1,否\nB,-2,城东,老年,10,1,否\n",
            "修订号非整数": "A,1,城东,青年,10,1,否\nB,x,城东,老年,10,1,否\n",
            "检出数>人数": "A,1,城东,青年,10,1,否\nB,1,城东,老年,10,11,否\n",
            "检出数为负": "A,1,城东,青年,10,1,否\nB,1,城东,老年,10,-1,否\n",
            "人数为负": "A,1,城东,青年,10,1,否\nB,1,城东,老年,-1,0,否\n",
            "检出数非整数": "A,1,城东,青年,10,1.5,否\n",
            "作废标记无法识别": "A,1,城东,青年,10,1,也许\n",
            "报送ID为空": "A,1,城东,青年,10,1,否\n,1,城东,老年,10,1,否\n",
            "分区为空": "A,1,城东,青年,10,1,否\nB,1,,老年,10,1,否\n",
            "列数不足": "A,1,城东,青年,10,1,否\nB,1,城东,老年,10\n",
        }
        for label, rows in bad_rows.items():
            r = upload(HEADER + rows)
            check(f"非法行「{label}」整批拒收(400)", r.status_code == 400, r.text)
        r = upload("分区,年龄组,人数,检出数\n城东,青年,10,1\n")
        check("缺少报送ID等必需列被拒绝", r.status_code == 400, r.text)
        check("非法批次后旧结果不变",
              totals(recon()) == (0, 26, -26, 0, 0), str(totals(recon())))

        # ---- 2. 正常批次 ----
        r = upload(
            HEADER
            + "S1,1,城东,青年,10,3,否\n"
            + "S2,1,城东,老年,10,8,否\n"
        )
        check("首批上传成功", r.status_code == 201, r.text.split("\n")[0])
        check("首批元数据正确",
              r.json()["rows_accepted"] == 2 and r.json()["rows_duplicate"] == 0, r.text)
        j = recon()
        check("首批逐格汇总正确",
              cell(j, "城东", "青年")["report"] == 3
              and cell(j, "城东", "青年")["diff"] == 1
              and cell(j, "城东", "老年")["report"] == 8
              and cell(j, "城东", "老年")["diff"] == 0
              and cell(j, "城西", "青年")["report"] == 0
              and cell(j, "城西", "青年")["diff"] == -8, str(j["cells"]))
        check("首批合计与差额",
              (j["report_total"], j["total_diff"]) == (11, -15), str(totals(j)))
        check("逐格差额之和等于合计差额",
              sum(c["diff"] for c in j["cells"]) == j["total_diff"])

        # ---- 3. 幂等重传与内容冲突 ----
        r = upload(HEADER + "S1,1,城东,青年,10,3,否\n")
        check("同 ID 同修订同内容重传幂等",
              r.status_code == 201 and r.json()["rows_accepted"] == 0
              and r.json()["rows_duplicate"] == 1, r.text)
        check("幂等重传不重复计数",
              (recon()["report_total"], recon()["effective_count"]) == (11, 2))
        r = upload(HEADER + "S1,1,城东,青年,10,4,否\n")
        check("同 ID 同修订不同内容整批拒收(409)", r.status_code == 409, r.text)
        check("冲突后旧结果不变",
              (recon()["report_total"], recon()["effective_count"]) == (11, 2))

        # 同批次内：先出现新行，再出现相同内容重复 → 只计一次
        r = upload(
            HEADER + "S5,1,城西,青年,10,6,否\n" + "S5,1,城西,青年,10,6,否\n"
        )
        check("同批次内完全重复行只接收一次",
              r.status_code == 201 and r.json()["rows_accepted"] == 1
              and r.json()["rows_duplicate"] == 1, r.text)
        j = recon()
        check("同批次重复只计一次贡献",
              cell(j, "城西", "青年")["report"] == 6, str(j["cells"]))
        # 同批次内冲突 → 整批拒收，包括前面尚未提交的合法行
        r = upload(
            HEADER
            + "S6,1,城西,老年,10,2,否\n"
            + "S5,1,城西,青年,10,9,否\n"
        )
        check("同批次内同 ID 同修订冲突整批拒收", r.status_code == 409, r.text)
        j = recon()
        check("同批次冲突不留下部分写入",
              cell(j, "城西", "老年")["report"] == 0
              and cell(j, "城西", "青年")["report"] == 6, str(j["cells"]))

        # 同批次内乱序修订：先低后高，高修订最终有效；先高后低，低版留档无效
        r = upload(HEADER + "S8,1,城东,青年,10,3,否\nS8,2,城东,老年,10,4,否\n")
        j = recon()
        check("同批次内低→高：高修订有效",
              r.status_code == 201
              and cell(j, "城东", "老年")["report"] == 12
              and cell(j, "城东", "青年")["report"] == 3, str(j["cells"]))
        r = upload(HEADER + "S9,5,城东,青年,10,1,否\nS9,2,城西,老年,10,1,否\n")
        j = recon()
        check("同批次内高→低：低版留档不生效",
              r.status_code == 201 and r.json()["rows_superseded"] == 1
              and cell(j, "城东", "青年")["report"] == 4
              and cell(j, "城西", "老年")["report"] == 0, str(j["cells"]))
        # 撤销这两条测试数据，避免影响后续断言
        upload(HEADER + "S8,3,城东,老年,10,4,是\nS9,6,城东,青年,10,1,是\n")

        # ---- 4. 最高修订贡献；改归属扣旧格、计新格 ----
        r = upload(HEADER + "S1,2,城西,老年,10,5,否\n")
        check("更高修订接收", r.status_code == 201 and r.json()["rows_accepted"] == 1, r.text)
        j = recon()
        check("改归属：旧格城东/青年扣除 3",
              cell(j, "城东", "青年")["report"] == 0
              and cell(j, "城东", "青年")["diff"] == -2, str(j["cells"]))
        check("改归属：新格城西/老年计入 5",
              cell(j, "城西", "老年")["report"] == 5
              and cell(j, "城西", "老年")["diff"] == -3, str(j["cells"]))
        check("改归属后合计正确",
              (j["report_total"], j["total_diff"]) == (8 + 6 + 5, 19 - 26),
              str(totals(j)))

        # 迟到旧版：修订 1 已经存在（同内容幂等），换一个从未提交过的旧修订也没内容变化；
        # 这里用新增 ID 先提交高修订，再补低修订。
        r = upload(HEADER + "S7,5,城东,青年,10,4,否\n")
        check("先收到高修订", r.status_code == 201, r.text)
        r = upload(HEADER + "S7,2,城西,青年,10,7,否\n")
        check("迟到旧版被留档但不生效",
              r.status_code == 201 and r.json()["rows_accepted"] == 1
              and r.json()["rows_superseded"] == 1, r.text)
        j = recon()
        check("迟到旧版不复活、不改变汇总",
              cell(j, "城东", "青年")["report"] == 4
              and cell(j, "城西", "青年")["report"] == 6, str(j["cells"]))

        # ---- 5. 作废撤去贡献 ----
        r = upload(HEADER + "S2,2,城东,老年,10,8,是\n")
        check("作废版接收", r.status_code == 201, r.text)
        j = recon()
        check("作废撤去贡献",
              cell(j, "城东", "老年")["report"] == 0
              and "S2" in j["voided_ids"], str(j["cells"]))
        r = upload(HEADER + "S2,1,城东,老年,10,8,否\n")
        check("作废后重传旧版：按同 ID 同修订视为幂等，不复活",
              r.status_code == 201 and j["voided_ids"] == recon()["voided_ids"],
              r.text)
        r = upload(HEADER + "S2,3,城东,老年,10,8,否\n")
        check("作废后更高修订（非作废）可重新生效", r.status_code == 201, r.text)
        j = recon()
        check("更高修订恢复贡献",
              cell(j, "城东", "老年")["report"] == 8
              and j["voided_ids"] == ["S8", "S9"], str(j["cells"]))

        # ---- 6. 额外组合单列 ----
        r = upload(HEADER + "X1,1,城北,少儿,5,2,否\n")
        check("范围外组合接收", r.status_code == 201, r.text)
        j = recon()
        check("额外组合单列",
              j["extra_total"] == 2 and len(j["extra_contributions"]) == 1
              and j["extra_contributions"][0]["district"] == "城北", str(j))
        check("额外组合不并入调查格汇总",
              sum(c["report"] for c in j["cells"]) == j["report_total"]
              and j["report_total"] == 4 + 6 + 8 + 5, str(totals(j)))

        # ---- 7. 交错上传后的一致性与并发 ----
        def upload_pair(i):
            t = (
                HEADER
                + f"C{i},1,城东,青年,10,1,否\n"
                + f"D{i},1,城西,老年,10,2,否\n"
            )
            with TestClient(app) as c2:
                return c2.post(
                    "/api/submissions/import",
                    files={"file": (f"b{i}.csv", t.encode("utf-8"), "text/csv")},
                ).status_code

        with ThreadPoolExecutor(max_workers=6) as ex:
            codes = list(ex.map(upload_pair, range(6)))
        check("并发上传全部被接收", all(c == 201 for c in codes), str(codes))
        j = recon()
        conn = sqlite3.connect(os.environ["DB_PATH"])
        valid_total = conn.execute(
            "SELECT COALESCE(SUM(detected), 0) FROM submissions "
            "WHERE valid = 1 AND voided = 0 "
            "AND (district, age_group) IN "
            "(SELECT district, age_group FROM cells)"
        ).fetchone()[0]
        conn.close()
        check("并发后对账合计等于有效版本之和", j["report_total"] == valid_total,
              f"recon={j['report_total']} db={valid_total}")
        check("并发后每个 ID 只有一个有效版本",
              j["effective_count"] == sum(1 for _ in j["voided_ids"])
              + sum(c["reported_count"] for c in j["cells"])
              + len(j["extra_contributions"]))

        # ---- 8. 原调查 / 已公开汇总不变；公开页不含上报明细 ----
        admin = client.get("/api/admin/state").json()
        check("调查格值未被报送改动",
              {(c["district"], c["age_group"]): c["detected"] for c in admin["cells"]}
              == {("城东", "青年"): 2, ("城东", "老年"): 8,
                  ("城西", "青年"): 8, ("城西", "老年"): 8})
        check("已公开汇总未变",
              client.get("/api/public").json()["publications"] == pubs_before)
        pub = client.get("/api/public").json()
        check("公开 API 不含上报明细字段",
              all(k not in str(pub) for k in
                  ("submission_id", "revision", "voided", "report_total", "batch")))
        r = client.get("/public")
        check("公开页可访问且不含报送ID",
              r.status_code == 200 and "S1" not in r.text and "报送ID" not in r.text)
        check("内部对账页可访问",
              client.get("/").status_code == 200)

        # ---- 9. 重启后仍可查（同一库文件，全新连接与客户端） ----
        before = recon()
        with TestClient(app) as client2:
            after = client2.get("/api/reconciliation").json()
        check("重启后对账结果一致",
              before["report_total"] == after["report_total"]
              and before["cells"] == after["cells"]
              and len(before["batches"]) == len(after["batches"]),
              f"{before['report_total']} != {after['report_total']}")
        rows = client.get("/api/reconciliation").json()["batches"]
        check("批次审计记录落盘",
              len(rows) >= 9 and all(r["rows_accepted"] >= 0 for r in rows),
              str(rows))

        # ---- 10. 存储失败整批回滚 ----
        import app.main as mainmod
        from app.submissions import SubmissionConflict

        before = recon()
        orig = mainmod.accept_submissions

        def _boom(*a, **k):
            raise RuntimeError("模拟保存失败")

        mainmod.accept_submissions = _boom
        try:
            r = upload(HEADER + "FAIL,1,城东,青年,10,1,否\n")
            check("保存异常返回 500", r.status_code == 500, r.text)
        finally:
            mainmod.accept_submissions = orig
        after = recon()
        check("保存失败不改变旧结果",
              totals(after) == totals(before), f"{totals(before)} != {totals(after)}")
        check("保存失败不留批次审计", after["batches"] == before["batches"],
              f"{before['batches']} vs {after['batches']}")
        _ = SubmissionConflict


run()
print(f"\n{sum(results)}/{len(results)} 通过")
sys.exit(0 if all(results) else 1)
