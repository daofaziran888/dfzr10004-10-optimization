"""增量去重引擎的回归测试：分段、水位、复用、快照分页、并发与持久化。

运行方式（在 backend 目录下）：
    DEDUP_STATE_PATH=/tmp/dedup-test.json .venv/bin/python -m unittest discover -s tests -v
测试内部会为每个用例切换到独立的临时状态文件。
"""
from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

os.environ.setdefault("DEDUP_STATE_PATH", tempfile.mktemp(prefix="dedup-test-", suffix=".json"))

from app.services import dedup as dedup_module  # noqa: E402
from app.services.dedup import (  # noqa: E402
    BATCH_CLOSED,
    BATCH_INTERRUPTED,
    BATCH_WAITING,
    REVIEW_RETIRED,
    DedupService,
    _ensure_state,
)
from app.store import store  # noqa: E402


def policy(number: str) -> dict:
    return next(row for row in store.rows("dedup") if row["策略编号"] == number)


class DedupIncrementalTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
        self._tmp.close()
        Path(self._tmp.name).unlink(missing_ok=True)
        dedup_module.STATE.path = Path(self._tmp.name)
        dedup_module.STATE.data = {}
        self.service = DedupService()
        self.service.bootstrap_history()

    def tearDown(self) -> None:
        Path(self._tmp.name).unlink(missing_ok=True)

    # ------------------------------------------------------------------
    def test_window_is_split_by_time_boundary(self) -> None:
        batch, _ = self.service.start_batch(policy("DEDU-0001"))
        self.assertEqual(len(batch["segments"]), 4)
        self.assertEqual(
            [segment["区间"] for segment in batch["segments"]],
            [
                "2026-09-01 ~ 2026-09-08",
                "2026-09-09 ~ 2026-09-16",
                "2026-09-17 ~ 2026-09-24",
                "2026-09-25 ~ 2026-09-30",
            ],
        )
        self.assertEqual(sum(segment["记录数"] for segment in batch["segments"]), 20)

    def test_interrupt_only_affects_current_segment_watermark(self) -> None:
        batch, _ = self.service.start_batch(policy("DEDU-0001"))
        # 段 1、2 收口，段 3 比对到第 4 条时中断。
        self.service.advance_batch(batch["batch_id"], {"推进记录数": 13})
        self.service.interrupt_batch(batch["batch_id"])
        view = self.service._batch_view(self.service.get_batch(batch["batch_id"]))
        self.assertEqual(view["status"], BATCH_INTERRUPTED)
        self.assertEqual([segment["状态"] for segment in view["segments"]], ["已收口", "已收口", "处理中", "待处理"])
        self.assertEqual(view["segments"][2]["水位"]["已比对记录"], 3)
        self.assertEqual(view["segments"][2]["水位"]["最后记录"], "R-1010")
        # 已收口区间不被回退，水位与摘要保持。
        self.assertEqual(view["segments"][0]["水位"]["已比对记录"], 4)
        self.assertEqual(view["segments"][1]["水位"]["已比对记录"], 6)
        self.assertEqual(view["segments"][0]["摘要哈希"], view["segments"][0]["摘要哈希"])

        # 续跑只补段 3 剩下的记录，段 1、2 不重算。
        resumed, _ = self.service.advance_batch(batch["batch_id"])
        self.assertEqual(resumed["status"], BATCH_CLOSED)
        self.assertEqual([segment["水位"]["已比对记录"] for segment in resumed["segments"]], [4, 6, 6, 4])

    def test_closed_history_segments_are_reused_with_original_digest(self) -> None:
        history = self.service.get_batch("BATCH-0001")
        self.assertEqual(history["status"], BATCH_CLOSED)
        digests = {segment["序号"]: segment["摘要"]["摘要哈希"] for segment in history["segments"]}

        batch, _ = self.service.start_batch(policy("DEDU-0003"))
        self.service.advance_batch(batch["batch_id"])
        view = self.service._batch_view(batch)
        self.assertEqual(view["status"], BATCH_CLOSED)
        for segment in view["segments"]:
            self.assertTrue(segment["复用"])
            self.assertEqual(segment["复用批次"], "BATCH-0001")
            self.assertEqual(segment["摘要哈希"], digests[segment["序号"]])

        report = self.service.report_summary(policy="DEDU-0003")
        summary = report["summary"]
        self.assertEqual(summary["扫描记录数"], 0)
        self.assertEqual(summary["复用分段数"], 4)
        self.assertEqual(summary["重复量"], 2)
        self.assertEqual(summary["仍需人工确认"], 5)

    def test_new_records_only_invalidate_affected_windows(self) -> None:
        batch, _ = self.service.start_batch(policy("DEDU-0001"))
        self.service.advance_batch(batch["batch_id"])
        result = self.service.add_records(
            policy("DEDU-0001"),
            [{
                "rec_id": "R-9001",
                "商品编号": "SKU-1005",
                "标题": "儿童保温杯 蓝色 500ml",
                "采集时间": "2026-09-17 08:00:00",
                "来源站点": "新增",
            }],
        )
        self.assertEqual([item["序号"] for item in result["受影响区间"]], [3])
        # 段 3 与下游段 4 失效；段 1、2 保持。
        self.assertEqual([item["序号"] for item in result["失效并待重算区间"]], [3, 4])

        after, _ = self.service.start_batch(policy("DEDU-0001"))
        self.service.advance_batch(after["batch_id"])
        view = self.service._batch_view(after)
        self.assertTrue(view["segments"][0]["复用"])
        self.assertTrue(view["segments"][1]["复用"])
        self.assertFalse(view["segments"][2]["复用"])
        self.assertFalse(view["segments"][3]["复用"])

    def test_pagination_stays_on_same_snapshot(self) -> None:
        batch, _ = self.service.start_batch(policy("DEDU-0001"))
        self.service.advance_batch(batch["batch_id"])
        first, _ = self.service.list_results(batch["batch_id"], page=1, size=2)
        snapshot = first["snapshot_id"]
        full_total = first["total"]

        # 新增记录并重算后，旧快照仍按原结果分页，不会重复计数。
        self.service.add_records(
            policy("DEDU-0001"),
            [{
                "rec_id": "R-9002",
                "商品编号": "SKU-1001",
                "标题": "夏季连衣裙 红色 M码 长款",
                "采集时间": "2026-09-04 08:00:00",
            }],
        )
        second, _ = self.service.list_results(batch["batch_id"], snapshot_id=snapshot, page=2, size=2)
        self.assertEqual(second["snapshot_id"], snapshot)
        self.assertEqual(second["total"], full_total)
        pages = [
            self.service.list_results(batch["batch_id"], snapshot_id=snapshot, page=page, size=2)[0]
            for page in range(1, (full_total + 1) // 2 + 1)
        ]
        seen = [row["记录编号"] for page in pages for row in page["items"]]
        self.assertEqual(len(seen), len(set(seen)))
        self.assertEqual(len(seen), full_total)

    def test_second_batch_waits_for_same_interval(self) -> None:
        first, _ = self.service.start_batch(policy("DEDU-0001"))
        second, message = self.service.start_batch(policy("DEDU-0001"))
        self.assertEqual(second["status"], BATCH_WAITING)
        self.assertEqual(second["blocked_by"], first["batch_id"])
        self.assertIn("等待", message)
        blocked, _ = self.service.advance_batch(second["batch_id"])
        self.assertEqual(blocked["status"], BATCH_WAITING)
        self.assertEqual(blocked["segments"][0]["状态"], "待处理")

        # 前一批次推进完了以后，等待批次可以推进并复用已收口段。
        self.service.advance_batch(first["batch_id"])
        resumed, _ = self.service.advance_batch(second["batch_id"])
        self.assertEqual(resumed["status"], BATCH_CLOSED)
        self.assertTrue(all(segment["复用"] for segment in resumed["segments"]))
        self.assertTrue(all(segment["复用批次"] == first["batch_id"] for segment in resumed["segments"]))

    def test_state_survives_restart(self) -> None:
        batch, _ = self.service.start_batch(policy("DEDU-0001"))
        self.service.advance_batch(batch["batch_id"], {"推进记录数": 10})
        self.service.interrupt_batch(batch["batch_id"])
        before = self.service._batch_view(self.service.get_batch(batch["batch_id"]))

        # 模拟进程重启：重新读取同一个状态文件。
        dedup_module.STATE.reload()
        restarted = DedupService()
        restarted.bootstrap_history()
        recovered = restarted._batch_view(restarted.get_batch(batch["batch_id"]))
        self.assertEqual(recovered["status"], BATCH_INTERRUPTED)
        self.assertEqual(
            [segment["水位"]["已比对记录"] for segment in recovered["segments"]],
            [segment["水位"]["已比对记录"] for segment in before["segments"]],
        )
        resumed, _ = restarted.advance_batch(batch["batch_id"])
        self.assertEqual(resumed["status"], BATCH_CLOSED)

    def test_retention_policies_are_preserved(self) -> None:
        latest, _ = self.service.start_batch(policy("DEDU-0001"))
        self.service.advance_batch(latest["batch_id"])
        rows = self.service.list_results(latest["batch_id"])[0]["items"]
        kept = {row["业务键"]: row["保留记录"] for row in rows}
        # 保留最新：SKU-1001 的两条时间相同，按记录编号后者优先。
        self.assertEqual(kept["SKU-1001"], "R-1002")
        self.assertEqual(kept["SKU-1002"], "R-1004")

        history = self.service.get_batch("BATCH-0001")
        hist = self.service.list_results(history["batch_id"])[0]["items"]
        kept_hist = {row["业务键"]: row["保留记录"] for row in hist}
        # 保留最早：历史结论不能被重写。
        self.assertEqual(kept_hist["SKU-3002"], "R-3003")
        self.assertEqual(kept_hist["SKU-3006"], "R-3011")

    def test_reviews_are_synced_and_retired_on_recompute(self) -> None:
        first, _ = self.service.start_batch(policy("DEDU-0001"))
        self.service.advance_batch(first["batch_id"])
        before = {row["复核编号"] for row in self.service.list_reviews(batch_id=first["batch_id"])}
        self.assertTrue(before)

        self.service.add_records(
            policy("DEDU-0001"),
            [{
                "rec_id": "R-9003",
                "商品编号": "SKU-1011",
                "标题": "陶瓷马克杯 白色 陶瓷盖 加厚",
                "采集时间": "2026-09-13 09:00:00",
            }],
        )
        second, _ = self.service.start_batch(policy("DEDU-0001"))
        self.service.advance_batch(second["batch_id"])
        after = {row["复核编号"] for row in self.service.list_reviews(batch_id=second["batch_id"])}
        self.assertTrue(after)
        self.assertTrue(before.isdisjoint(after))
        retired = [
            row for row in _ensure_state()["reviews"].values() if row["复核状态"] == REVIEW_RETIRED
        ]
        self.assertTrue(retired)
        for review in self.service.list_reviews(batch_id=second["batch_id"]):
            self.assertEqual(review["批次收口状态"], "已收口")
            self.assertTrue(review["批次已收口"])

    def test_report_reports_scan_range_reuse_and_manual_confirm(self) -> None:
        batch, _ = self.service.start_batch(policy("DEDU-0001"))
        self.service.advance_batch(batch["batch_id"], {"推进记录数": 10})
        self.service.interrupt_batch(batch["batch_id"])
        report = self.service.report_summary(policy="DEDU-0001")
        item = report["batches"][0]
        self.assertEqual(item["扫描范围"]["已收口分段"], 2)
        self.assertEqual(item["扫描范围"]["本次扫描记录"], 10)
        self.assertEqual([segment["序号"] for segment in item["尚未收口区间"]], [3, 4])
        self.assertGreaterEqual(item["仍需人工确认"], 1)

        closed, _ = self.service.advance_batch(batch["batch_id"])
        report = self.service.report_summary(policy="DEDU-0001")
        item = report["batches"][0]
        self.assertEqual(item["尚未收口区间"], [])
        self.assertTrue(item["已收口"])
        self.assertEqual(item["重复量"], 6)


if __name__ == "__main__":
    unittest.main()
