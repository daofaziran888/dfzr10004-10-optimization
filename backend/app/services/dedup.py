"""采集去重比对业务规则与增量批次引擎。

设计要点
--------
1. 大窗口不一次性重扫：策略按「业务键列 + 时间边界」拆成若干增量区间（分段）。
   每段只在自己的记录里比对，并把段内保留记录压成一份「段摘要」交给下一段；
   后续段只和摘要里的幸存记录比对，不再遍历全量历史。
2. 每段持久化水位（记录序号、最后记录、已比对记录数、已比对对数），
   中断后从水位继续；已收口段保留原摘要，不会被回退。
3. 已收口段的摘要按「配置签名 + 记录指纹 + 上段摘要哈希」缓存；
   新增记录只让受影响的段及其下游摘要失效，其余段直接复用原摘要。
4. 批次收口时落一份不可变结果快照，分页始终引用同一个 snapshot_id，
   同一组重复记录不会被重复计数。
5. 同一策略同一窗口同时只允许一个批次推进，后来者进入等待，
   前一批次收口或中断后，等待批次可以推进并复用已收口段。
"""
from __future__ import annotations

import difflib
import hashlib
import json
import re
import threading
import unicodedata
from datetime import date, datetime, timedelta
from typing import Any

from app.dedup_records import DEDUP_RECORDS_BY_POLICY
from app.persistence import JsonState
from app.store import store

MODULE = "dedup"
REQUIRED_FIELDS = ["策略编号", "业务键列", "排序字段"]
STATUS_ORDER = ["草稿", "待校验", "已启用", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "启用策略": "已启用", "停用策略": "已停用"}
NEGATIVE_ACTIONS = ["停用策略"]

# 分段与批次状态
SEG_PENDING = "待处理"
SEG_RUNNING = "处理中"
SEG_CLOSED = "已收口"
SEG_DIRTY = "待重算"
BATCH_PENDING = "待处理"
BATCH_RUNNING = "处理中"
BATCH_WAITING = "等待中"
BATCH_INTERRUPTED = "已中断"
BATCH_CLOSED = "已收口"

RETAIN_LATEST = "保留最新"
RETAIN_EARLIEST = "保留最早"
KIND_AUTO = "自动去重"
KIND_REVIEW = "待人工确认"

REVIEW_PENDING = "待确认"
REVIEW_DUP = "已确认重复"
REVIEW_KEEP = "已确认保留"
REVIEW_RETIRED = "已被新批次替代"

ALGORITHM_VERSION = "v1"
DEFAULT_SEGMENT_DAYS = 7
DEFAULT_THRESHOLD = 0.85
REVIEW_MARGIN = 0.10
MAX_PAGE_SIZE = 200

LOCK = threading.RLock()
STATE = JsonState()


# ---------------------------------------------------------------------------
# 基础工具
# ---------------------------------------------------------------------------
def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _digest(text: str, length: int = 10) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:length]


def _normalize(text: Any) -> str:
    normalized = unicodedata.normalize("NFKC", str(text or ""))
    normalized = re.sub(r"\s+", " ", normalized).strip().casefold()
    return normalized


def _similarity(left: Any, right: Any) -> float:
    a, b = _normalize(left), _normalize(right)
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    return round(difflib.SequenceMatcher(None, a, b).ratio(), 4)


def _parse_day(value: Any) -> date | None:
    text = str(value or "").strip()[:10]
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def _parse_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _as_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _iso_day(value: date) -> str:
    return value.isoformat()


def _record_id(record: dict[str, Any]) -> str:
    return str(record.get("rec_id") or record.get("记录编号") or "")


def _record_time(record: dict[str, Any], sort_field: str) -> datetime:
    return _parse_time(record.get(sort_field)) or datetime.min


def _sort_key(record: dict[str, Any], sort_field: str) -> tuple[datetime, str]:
    return (_record_time(record, sort_field), _record_id(record))


def _record_fingerprint(records: list[dict[str, Any]]) -> str:
    lines = [
        "|".join([
            _record_id(record),
            str(record.get("采集时间") or ""),
            str(record.get("标题") or ""),
            str(record.get("来源站点") or ""),
        ])
        for record in sorted(records, key=lambda item: _record_id(item))
    ]
    return _digest("\n".join(lines), 16)


def _digest_hash(digest: dict[str, Any]) -> str:
    if not digest:
        return "empty"
    keys = {
        key: sorted(
            [
                [_record_id(record), record.get("采集时间"), record.get("标题")]
                for record in value.get("survivors", [])
            ]
        )
        for key, value in (digest.get("keys") or {}).items()
    }
    return _digest(json.dumps(keys, ensure_ascii=False, sort_keys=True), 16)


def _policy_number(policy: dict[str, Any]) -> str:
    return str(policy.get("策略编号") or f"DEDU-{policy.get('id', '')}")


def _policy_window(policy: dict[str, Any]) -> tuple[date, date]:
    start = _parse_day(policy.get("窗口开始"))
    end = _parse_day(policy.get("窗口结束"))
    if start and end:
        return start, end
    match = re.findall(r"\d{4}-\d{2}-\d{2}", str(policy.get("时间窗口") or ""))
    if len(match) >= 2:
        start = _parse_day(match[0])
        end = _parse_day(match[1])
        if start and end:
            return start, end
    today = date.today()
    return today, today


def _config(policy: dict[str, Any]) -> dict[str, Any]:
    start, end = _policy_window(policy)
    segment_days = _as_int(policy.get("分段天数"), DEFAULT_SEGMENT_DAYS)
    segment_days = max(1, min(segment_days, 31))
    threshold = _as_float(policy.get("相似阈值"), DEFAULT_THRESHOLD)
    threshold = min(max(threshold, 0.0), 1.0)
    business_key = str(policy.get("业务键列") or "业务键列")
    compare_field = str(policy.get("比对字段") or "标题")
    sort_field = str(policy.get("排序字段") or "采集时间")
    retention = str(policy.get("保留策略") or RETAIN_LATEST)
    if retention not in (RETAIN_LATEST, RETAIN_EARLIEST):
        retention = RETAIN_LATEST
    return {
        "策略编号": _policy_number(policy),
        "窗口开始": start,
        "窗口结束": end,
        "分段天数": segment_days,
        "相似阈值": threshold,
        "复核下限": round(max(0.0, threshold - REVIEW_MARGIN), 4),
        "业务键列": business_key,
        "比对字段": compare_field,
        "排序字段": sort_field,
        "保留策略": retention,
    }


def _config_hash(config: dict[str, Any]) -> str:
    payload = {
        "策略编号": config["策略编号"],
        "窗口开始": _iso_day(config["窗口开始"]),
        "窗口结束": _iso_day(config["窗口结束"]),
        "分段天数": config["分段天数"],
        "相似阈值": config["相似阈值"],
        "复核下限": config["复核下限"],
        "业务键列": config["业务键列"],
        "比对字段": config["比对字段"],
        "排序字段": config["排序字段"],
        "保留策略": config["保留策略"],
        "算法版本": ALGORITHM_VERSION,
    }
    return _digest(json.dumps(payload, ensure_ascii=False, sort_keys=True), 12)


def _segments(config: dict[str, Any]) -> list[dict[str, Any]]:
    segments: list[dict[str, Any]] = []
    cursor = config["窗口开始"]
    index = 1
    while cursor <= config["窗口结束"]:
        seg_end = min(cursor + timedelta(days=config["分段天数"] - 1), config["窗口结束"])
        segments.append({
            "序号": index,
            "开始": _iso_day(cursor),
            "结束": _iso_day(seg_end),
            "区间": f"{_iso_day(cursor)} ~ {_iso_day(seg_end)}",
        })
        cursor = seg_end + timedelta(days=1)
        index += 1
    return segments


def _cache_key(config: dict[str, Any], segment: dict[str, Any]) -> str:
    return "|".join([
        config["策略编号"],
        str(segment["序号"]),
        segment["开始"],
        segment["结束"],
        _config_hash(config),
    ])


def _retention_prefers_new(new: dict[str, Any], old: dict[str, Any], config: dict[str, Any]) -> bool:
    new_rank = _sort_key(new, config["排序字段"])
    old_rank = _sort_key(old, config["排序字段"])
    if config["保留策略"] == RETAIN_EARLIEST:
        return new_rank < old_rank
    return new_rank >= old_rank


# ---------------------------------------------------------------------------
# 状态仓库
# ---------------------------------------------------------------------------
def _default_state() -> dict[str, Any]:
    return {
        "version": 1,
        "counters": {"batch": 0, "snapshot": 0, "review": 0},
        "batches": {},
        "segment_cache": {},
        "snapshots": {},
        "reviews": {},
        "extra_records": {},
        "bootstrapped": False,
    }


def _ensure_state() -> dict[str, Any]:
    if not STATE.data.get("version"):
        # 内存镜像为空时先回读磁盘，避免把已有的水位/批次状态覆盖成默认值。
        loaded = STATE.reload()
        if loaded.get("version"):
            STATE.data = loaded
        else:
            STATE.data.update(_default_state())
            STATE.save()
    for key, value in _default_state().items():
        STATE.data.setdefault(key, value)
    STATE.data.setdefault("counters", {})
    for counter in ("batch", "snapshot", "review"):
        STATE.data["counters"].setdefault(counter, 0)
    return STATE.data


def _next_id(kind: str, prefix: str) -> str:
    data = _ensure_state()
    data["counters"][kind] = int(data["counters"].get(kind, 0)) + 1
    return f"{prefix}-{data['counters'][kind]:04d}"


# ---------------------------------------------------------------------------
# 引擎
# ---------------------------------------------------------------------------
class DedupService:
    # 兼容原有列表/明细/登记/流转接口 ---------------------------------------
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("策略编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"去重策略 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于采集去重比对可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"去重策略已{action}"

    # 记录集 ---------------------------------------------------------------
    def policy_records(self, policy_number: str) -> list[dict[str, Any]]:
        data = _ensure_state()
        base = [dict(row) for row in DEDUP_RECORDS_BY_POLICY.get(policy_number, [])]
        extra = [dict(row) for row in data["extra_records"].get(policy_number, [])]
        return base + extra

    def add_records(
        self, policy: dict[str, Any], records: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """追加采集记录并返回受影响区间；窗口外的记录会被明确标记为忽略。"""
        with LOCK:
            data = _ensure_state()
            config = _config(policy)
            policy_number = config["策略编号"]
            business_key = config["业务键列"]
            normalized: list[dict[str, Any]] = []
            ignored: list[dict[str, Any]] = []
            for index, record in enumerate(records):
                key = str(record.get(business_key) or record.get("商品编号") or "").strip()
                collected_at = record.get(config["排序字段"]) or record.get("采集时间")
                parsed = _parse_time(collected_at)
                rec_id = str(record.get("rec_id") or record.get("记录编号") or "").strip()
                if not key or not parsed or not rec_id:
                    ignored.append({
                        "记录编号": rec_id or f"新记录-{index + 1}",
                        "原因": f"缺少{config['业务键列']}、{config['排序字段']} 或 rec_id",
                    })
                    continue
                item = {
                    "rec_id": rec_id,
                    business_key: key,
                    config["比对字段"]: record.get(config["比对字段"]) or record.get("标题") or "",
                    config["排序字段"]: str(collected_at),
                    "来源站点": record.get("来源站点") or "新增",
                    "保留优先级": _as_int(record.get("保留优先级"), 0),
                }
                start, end = config["窗口开始"], config["窗口结束"]
                if not (start <= parsed.date() <= end):
                    ignored.append({
                        "记录编号": rec_id,
                        "原因": f"采集时间 {collected_at} 不在窗口 {_iso_day(start)} 至 {_iso_day(end)}",
                    })
                    continue
                normalized.append(item)
            data["extra_records"].setdefault(policy_number, []).extend(normalized)
            segments = _segments(config)
            affected = self._affected_segments(config, normalized)
            invalidated = self._invalidate_after(config, segments, affected)
            reopened = self._reopen_active_segments(config, affected)
            STATE.save()
            return {
                "策略编号": policy_number,
                "新增记录数": len(normalized),
                "忽略记录": ignored,
                "受影响区间": affected,
                "失效并待重算区间": invalidated,
                "未收口批次重开区间": reopened,
                "说明": "仅受影响的区间会重算；其余已收口区间继续复用原摘要。",
            }

    def _affected_segments(
        self, config: dict[str, Any], records: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        affected: list[dict[str, Any]] = []
        for segment in _segments(config):
            for record in records:
                parsed = _parse_time(record.get(config["排序字段"]))
                if parsed and segment["开始"] <= parsed.date().isoformat() <= segment["结束"]:
                    affected.append({
                        "序号": segment["序号"],
                        "区间": segment["区间"],
                        "新增记录": _record_id(record),
                    })
                    break
        return affected

    def _invalidate_after(
        self,
        config: dict[str, Any],
        segments: list[dict[str, Any]],
        affected: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """记录变化会让该段及其下游的摘要失效，上游段保持可复用。"""
        data = _ensure_state()
        if not affected:
            return []
        first = min(int(item["序号"]) for item in affected)
        removed: list[dict[str, Any]] = []
        for segment in segments:
            if int(segment["序号"]) < first:
                continue
            key = _cache_key(config, segment)
            if key in data["segment_cache"]:
                removed.append({
                    "序号": segment["序号"],
                    "区间": segment["区间"],
                    "原摘要": data["segment_cache"][key].get("摘要", {}).get("摘要哈希"),
                    "状态": "已失效，等待重算",
                })
                data["segment_cache"].pop(key, None)
        return removed

    def _reopen_active_segments(
        self, config: dict[str, Any], affected: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """新增记录落入未收口批次时，把受影响的已收口段重新打开，只重算这些段及下游。"""
        data = _ensure_state()
        if not affected:
            return []
        first = min(int(item["序号"]) for item in affected)
        reopened: list[dict[str, Any]] = []
        for batch in data["batches"].values():
            if batch.get("策略编号") != config["策略编号"]:
                continue
            if batch.get("status") not in (
                BATCH_PENDING, BATCH_RUNNING, BATCH_WAITING, BATCH_INTERRUPTED
            ):
                continue
            if (batch.get("窗口开始"), batch.get("窗口结束")) != (
                _iso_day(config["窗口开始"]), _iso_day(config["窗口结束"]),
            ):
                continue
            if batch.get("分段天数") != config["分段天数"]:
                continue
            for segment in batch["segments"]:
                if int(segment["序号"]) < first:
                    continue
                if segment["状态"] == SEG_CLOSED:
                    reopened.append({
                        "batch_id": batch["batch_id"],
                        "序号": segment["序号"],
                        "区间": segment["区间"],
                        "原摘要": (segment.get("摘要") or {}).get("摘要哈希"),
                    })
                    segment["原摘要"] = segment.get("摘要")
                    segment["摘要"] = None
                    segment["状态"] = SEG_DIRTY
                    segment["复用"] = False
                    segment["工作区"] = None
                    segment["重复行"] = []
                    segment["复核编号"] = []
                    segment["水位"] = {
                        "记录序号": 0,
                        "已比对记录": 0,
                        "已比对对数": 0,
                        "最后记录": None,
                        "最后采集时间": None,
                    }
                    batch["updated_at"] = _now()
                    for review in data["reviews"].values():
                        if review.get("segment_key") != _cache_key(config, segment):
                            continue
                        if review.get("复核状态") == REVIEW_RETIRED:
                            continue
                        review["复核状态"] = REVIEW_RETIRED
                        review["处理时间"] = _now()
                        review["说明"] = "区间因新增记录重开，原复核项作废"
                elif segment["状态"] in (SEG_PENDING, SEG_RUNNING):
                    # 下游段尚未收口，保持原样；其摘要缓存已随上游失效。
                    continue
        return reopened

    # 批次 ---------------------------------------------------------------
    def start_batch(
        self,
        policy: dict[str, Any],
        values: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        values = values or {}
        with LOCK:
            data = _ensure_state()
            config = _config(policy)
            start = _parse_day(values.get("窗口开始")) or config["窗口开始"]
            end = _parse_day(values.get("窗口结束")) or config["窗口结束"]
            if end < start:
                return None, "窗口结束不能早于窗口开始"
            if (start, end) != (config["窗口开始"], config["窗口结束"]):
                config = dict(config)
                config["窗口开始"] = start
                config["窗口结束"] = end
            segment_days = _as_int(values.get("分段天数"), config["分段天数"])
            segment_days = max(1, min(segment_days, 31))
            if segment_days != config["分段天数"]:
                config = dict(config)
                config["分段天数"] = segment_days
            config = dict(config)
            config["配置哈希"] = _config_hash(config)
            interval_key = self._interval_key(config)
            active = self._active_batch(interval_key)
            batch_id = _next_id("batch", "BATCH")
            batch = {
                "id": int(data["counters"]["batch"]),
                "batch_id": batch_id,
                "策略编号": config["策略编号"],
                "policy_id": policy.get("id"),
                "时间窗口": f"{_iso_day(config['窗口开始'])} 至 {_iso_day(config['窗口结束'])}",
                "窗口开始": _iso_day(config["窗口开始"]),
                "窗口结束": _iso_day(config["窗口结束"]),
                "分段天数": config["分段天数"],
                "相似阈值": config["相似阈值"],
                "复核下限": config["复核下限"],
                "保留策略": config["保留策略"],
                "业务键列": config["业务键列"],
                "比对字段": config["比对字段"],
                "排序字段": config["排序字段"],
                "配置哈希": config["配置哈希"],
                "区间键": interval_key,
                "status": BATCH_PENDING,
                "blocked_by": None,
                "历史批次": False,
                "created_at": _now(),
                "updated_at": _now(),
                "segments": self._init_segments(config),
                "snapshot_ids": [],
                "snapshot_id": None,
                "推进预算": None,
            }
            if active is not None:
                batch["status"] = BATCH_WAITING
                batch["blocked_by"] = active["batch_id"]
                data["batches"][batch_id] = batch
                STATE.save()
                return batch, (
                    f"区间「{batch['时间窗口']}」已有批次 {active['batch_id']} 在推进，"
                    f"批次 {batch_id} 进入等待"
                )
            data["batches"][batch_id] = batch
            budget = _as_int(values.get("推进记录数"), 0)
            STATE.save()
            if budget > 0:
                self._advance_batch(batch, budget)
            return batch, f"去重批次 {batch_id} 已发起，窗口共 {len(batch['segments'])} 段"

    def _init_segments(self, config: dict[str, Any]) -> list[dict[str, Any]]:
        records = self.policy_records(config["策略编号"])
        result: list[dict[str, Any]] = []
        for segment in _segments(config):
            local = self._segment_records(config, records, segment)
            result.append({
                "序号": segment["序号"],
                "开始": segment["开始"],
                "结束": segment["结束"],
                "区间": segment["区间"],
                "状态": SEG_PENDING,
                "复用": False,
                "复用批次": None,
                "原摘要": None,
                "记录指纹": _record_fingerprint(local),
                "记录数": len(local),
                "工作区": None,
                "工作区指纹": None,
                "摘要": None,
                "重复行": [],
                "复核编号": [],
                "水位": {
                    "记录序号": 0,
                    "已比对记录": 0,
                    "已比对对数": 0,
                    "最后记录": None,
                    "最后采集时间": None,
                },
            })
        return result

    def _segment_records(
        self, config: dict[str, Any], records: list[dict[str, Any]], segment: dict[str, Any]
    ) -> list[dict[str, Any]]:
        local: list[dict[str, Any]] = []
        for record in records:
            parsed = _parse_time(record.get(config["排序字段"]))
            if parsed is None:
                continue
            if segment["开始"] <= parsed.date().isoformat() <= segment["结束"]:
                local.append(record)
        local.sort(key=lambda item: _sort_key(item, config["排序字段"]))
        return local

    def _interval_key(self, config: dict[str, Any]) -> str:
        return f"{config['策略编号']}@{_iso_day(config['窗口开始'])}~{_iso_day(config['窗口结束'])}@{config['分段天数']}@{_config_hash(config)}"

    def _active_batch(self, interval_key: str) -> dict[str, Any] | None:
        data = _ensure_state()
        for batch in sorted(data["batches"].values(), key=lambda item: int(item["id"])):
            if batch.get("区间键") != interval_key:
                continue
            if batch.get("status") in (BATCH_RUNNING, BATCH_WAITING, BATCH_PENDING):
                return batch
        return None

    def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        data = _ensure_state()
        return data["batches"].get(batch_id)

    def batch_view(self, batch_id: str) -> dict[str, Any] | None:
        """对外读模型：不含工作区，只暴露水位、分段进度与最近结果。"""
        batch = self.get_batch(batch_id)
        return None if batch is None else self._batch_view(batch)

    def list_batches(
        self, *, policy: str | None = None, status: str | None = None
    ) -> list[dict[str, Any]]:
        data = _ensure_state()
        rows = sorted(data["batches"].values(), key=lambda item: int(item["id"]), reverse=True)
        if policy:
            rows = [row for row in rows if row.get("策略编号") == policy]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        return [self._batch_view(row) for row in rows]

    def advance_batch(
        self, batch_id: str, values: dict[str, Any] | None = None
    ) -> tuple[dict[str, Any] | None, str]:
        values = values or {}
        with LOCK:
            batch = self.get_batch(batch_id)
            if batch is None:
                return None, f"去重批次 {batch_id} 不存在"
            if batch["status"] == BATCH_CLOSED:
                return batch, f"去重批次 {batch_id} 已收口，结果按快照 {batch.get('snapshot_id')} 复用"
            if batch["status"] == BATCH_WAITING:
                blocker_id = batch.get("blocked_by")
                blocker = self.get_batch(blocker_id) if blocker_id else None
                if blocker is not None and blocker.get("status") in (BATCH_RUNNING, BATCH_PENDING):
                    return batch, (
                        f"批次 {batch_id} 仍在等待 {blocker_id}（{blocker.get('status')}），"
                        "待前一批次收口或中断后再次推进"
                    )
                batch["status"] = BATCH_RUNNING
                batch["blocked_by"] = None
                batch["updated_at"] = _now()
            if batch["status"] in (BATCH_PENDING, BATCH_INTERRUPTED):
                batch["status"] = BATCH_RUNNING
                batch["updated_at"] = _now()
            budget = values.get("推进记录数")
            budget = None if budget in (None, "") else max(0, _as_int(budget, 0))
            processed = self._advance_batch(batch, budget)
            watermark = self._batch_watermark(batch)
            return batch, (
                f"批次 {batch_id} 已推进 {processed} 条记录，"
                f"当前状态 {batch['status']}，水位 {watermark['已比对记录']}"
            )

    def interrupt_batch(self, batch_id: str) -> tuple[dict[str, Any] | None, str]:
        with LOCK:
            batch = self.get_batch(batch_id)
            if batch is None:
                return None, f"去重批次 {batch_id} 不存在"
            if batch["status"] == BATCH_CLOSED:
                return batch, f"去重批次 {batch_id} 已收口，无需中断"
            batch["status"] = BATCH_INTERRUPTED
            batch["updated_at"] = _now()
            if batch.get("snapshot_id"):
                self._refresh_snapshot(batch)
            STATE.save()
            open_segments = [seg for seg in batch["segments"] if seg["状态"] != SEG_CLOSED]
            return batch, (
                f"批次 {batch_id} 已中断；已收口 {len(batch['segments']) - len(open_segments)} 段，"
                f"未收口 {len(open_segments)} 段保留水位等待续跑"
            )

    def _advance_batch(self, batch: dict[str, Any], budget: int | None) -> int:
        config = self._batch_config(batch)
        records = self.policy_records(config["策略编号"])
        processed = 0
        for segment in batch["segments"]:
            if budget is not None and processed >= budget:
                break
            state = segment
            if state["状态"] == SEG_CLOSED:
                continue
            local = self._segment_records(config, records, state)
            state["记录指纹"] = _record_fingerprint(local)
            state["记录数"] = len(local)
            cache = self._reusable_cache(batch, config, state, local)
            if cache is not None:
                self._apply_cache(state, cache)
                batch["updated_at"] = _now()
                continue
            self._begin_segment(batch, config, state, local)
            while state["水位"]["记录序号"] < len(local):
                if budget is not None and processed >= budget:
                    break
                index = int(state["水位"]["记录序号"])
                self._process_record(batch, config, state, local[index])
                state["水位"]["记录序号"] = index + 1
                state["水位"]["已比对记录"] = index + 1
                state["水位"]["最后记录"] = _record_id(local[index])
                state["水位"]["最后采集时间"] = str(local[index].get(config["排序字段"]) or "")
                processed += 1
            if state["水位"]["记录序号"] >= len(local):
                self._close_segment(batch, config, state)
                STATE.save()
            else:
                STATE.save()
                break
        self._finalize_if_closed(batch)
        STATE.save()
        return processed

    def _batch_config(self, batch: dict[str, Any]) -> dict[str, Any]:
        return {
            "策略编号": batch["策略编号"],
            "窗口开始": _parse_day(batch["窗口开始"]),
            "窗口结束": _parse_day(batch["窗口结束"]),
            "分段天数": int(batch["分段天数"]),
            "相似阈值": float(batch["相似阈值"]),
            "复核下限": float(batch["复核下限"]),
            "业务键列": batch["业务键列"],
            "比对字段": batch["比对字段"],
            "排序字段": batch["排序字段"],
            "保留策略": batch["保留策略"],
        }

    def _reusable_cache(
        self,
        batch: dict[str, Any],
        config: dict[str, Any],
        segment: dict[str, Any],
        local: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        data = _ensure_state()
        key = _cache_key(config, segment)
        cache = data["segment_cache"].get(key)
        if cache is None:
            return None
        if cache.get("记录指纹") != _record_fingerprint(local):
            return None
        previous = batch["segments"][int(segment["序号"]) - 2] if int(segment["序号"]) > 1 else None
        carry_hash = _digest_hash(previous["摘要"]) if previous is not None else "empty"
        if cache.get("上游摘要哈希") != carry_hash:
            return None
        return cache

    def _apply_cache(self, segment: dict[str, Any], cache: dict[str, Any]) -> None:
        segment["状态"] = SEG_CLOSED
        segment["复用"] = True
        segment["复用批次"] = cache.get("来源批次")
        segment["原摘要"] = cache.get("摘要")
        segment["摘要"] = cache.get("摘要")
        segment["重复行"] = [dict(row) for row in cache.get("重复行", [])]
        segment["复核编号"] = list(cache.get("复核编号", []))
        segment["记录数"] = int(cache.get("摘要", {}).get("记录数", segment["记录数"]))
        segment["水位"] = {
            "记录序号": segment["记录数"],
            "已比对记录": int(cache.get("摘要", {}).get("已比对记录", segment["记录数"])),
            "已比对对数": int(cache.get("摘要", {}).get("比对对数", 0)),
            "最后记录": cache.get("摘要", {}).get("最后记录"),
            "最后采集时间": cache.get("摘要", {}).get("最后采集时间"),
        }
        segment["工作区"] = None

    def _begin_segment(
        self,
        batch: dict[str, Any],
        config: dict[str, Any],
        segment: dict[str, Any],
        local: list[dict[str, Any]],
    ) -> None:
        if (
            segment.get("工作区") is not None
            and segment["状态"] in (SEG_RUNNING, SEG_DIRTY)
            and segment.get("工作区指纹") == segment.get("记录指纹")
        ):
            segment["状态"] = SEG_RUNNING
            return
        segment["状态"] = SEG_RUNNING
        segment["复用"] = False
        segment["复用批次"] = None
        segment["原摘要"] = None
        segment["重复行"] = []
        segment["复核编号"] = []
        segment["水位"] = {
            "记录序号": 0,
            "已比对记录": 0,
            "已比对对数": 0,
            "最后记录": None,
            "最后采集时间": None,
        }
        carry: dict[str, dict[str, Any]] = {}
        previous = batch["segments"][int(segment["序号"]) - 2] if int(segment["序号"]) > 1 else None
        if previous is not None and previous.get("摘要"):
            for key, value in previous["摘要"].get("keys", {}).items():
                carry[key] = {"survivors": [dict(record) for record in value.get("survivors", [])]}
        segment["工作区"] = {"keys": carry}
        segment["工作区指纹"] = segment.get("记录指纹")

    def _process_record(
        self,
        batch: dict[str, Any],
        config: dict[str, Any],
        segment: dict[str, Any],
        record: dict[str, Any],
    ) -> None:
        work = segment["工作区"]
        business_key = str(record.get(config["业务键列"]) or "")
        bucket = work["keys"].setdefault(business_key, {"survivors": []})
        survivors: list[dict[str, Any]] = bucket["survivors"]
        best_value = 0.0
        best_record: dict[str, Any] | None = None
        comparisons = 0
        for survivor in survivors:
            candidate = _similarity(record.get(config["比对字段"]), survivor.get(config["比对字段"]))
            comparisons += 1
            if candidate > best_value:
                best_value = candidate
                best_record = survivor
        segment["水位"]["已比对对数"] = int(segment["水位"]["已比对对数"]) + comparisons
        if best_record is not None and best_value >= config["相似阈值"]:
            self._resolve_duplicate(segment, config, record, best_record, bucket, best_value)
            return
        survivors.append(dict(record))
        if best_record is not None and best_value >= config["复核下限"]:
            review_id = self._create_review(batch, segment, config, record, best_record, best_value)
            segment["复核编号"].append(review_id)

    def _resolve_duplicate(
        self,
        segment: dict[str, Any],
        config: dict[str, Any],
        record: dict[str, Any],
        survivor: dict[str, Any],
        bucket: dict[str, Any],
        similarity: float,
    ) -> None:
        survivors: list[dict[str, Any]] = bucket["survivors"]
        if _retention_prefers_new(record, survivor, config):
            kept, dropped = record, survivor
            survivors[survivors.index(survivor)] = dict(record)
        else:
            kept, dropped = survivor, record
        segment["重复行"].append({
            "记录编号": _record_id(dropped),
            "业务键": str(dropped.get(config["业务键列"]) or ""),
            "保留记录": _record_id(kept),
            "保留标题": str(kept.get(config["比对字段"]) or ""),
            "重复标题": str(dropped.get(config["比对字段"]) or ""),
            "相似度": similarity,
            "处理类型": KIND_AUTO,
            "分段": segment["序号"],
            "区间": segment["区间"],
            "保留策略": config["保留策略"],
            "采集时间": str(dropped.get(config["排序字段"]) or ""),
        })

    def _create_review(
        self,
        batch: dict[str, Any],
        segment: dict[str, Any],
        config: dict[str, Any],
        record: dict[str, Any],
        survivor: dict[str, Any],
        similarity: float,
    ) -> str:
        data = _ensure_state()
        review_id = _next_id("review", "REV")
        review = {
            "复核编号": review_id,
            "策略编号": config["策略编号"],
            "批次编号": batch["batch_id"],
            "分段": segment["序号"],
            "区间": segment["区间"],
            "segment_key": _cache_key(config, segment),
            "业务键": str(record.get(config["业务键列"]) or ""),
            "记录编号": _record_id(record),
            "标题": str(record.get(config["比对字段"]) or ""),
            "保留记录": _record_id(survivor),
            "保留标题": str(survivor.get(config["比对字段"]) or ""),
            "相似度": similarity,
            "复核状态": REVIEW_PENDING,
            "创建时间": _now(),
            "处理时间": None,
        }
        data["reviews"][review_id] = review
        return review_id

    def _segment_summary(
        self, segment: dict[str, Any], config: dict[str, Any]
    ) -> dict[str, Any]:
        work = segment.get("工作区") or {"keys": {}}
        keys: dict[str, Any] = {}
        survivor_count = 0
        for key, value in work.get("keys", {}).items():
            survivors = value.get("survivors", [])
            survivor_count += len(survivors)
            keys[key] = {
                "survivors": [
                    {
                        "rec_id": _record_id(record),
                        config["业务键列"]: key,
                        config["比对字段"]: record.get(config["比对字段"]),
                        config["排序字段"]: record.get(config["排序字段"]),
                        "来源站点": record.get("来源站点"),
                    }
                    for record in survivors
                ]
            }
        summary = {
            "分段": segment["序号"],
            "区间": segment["区间"],
            "记录数": int(segment["水位"].get("已比对记录") or segment.get("记录数") or 0),
            "已比对记录": int(segment["水位"].get("已比对记录") or 0),
            "比对对数": int(segment["水位"].get("已比对对数") or 0),
            "重复量": len(segment.get("重复行") or []),
            "待确认": len(segment.get("复核编号") or []),
            "保留摘要": survivor_count,
            "保留策略": config["保留策略"],
            "最后记录": segment["水位"].get("最后记录"),
            "最后采集时间": segment["水位"].get("最后采集时间"),
            "keys": keys,
        }
        summary["摘要哈希"] = _digest_hash(summary)
        return summary

    def _close_segment(
        self, batch: dict[str, Any], config: dict[str, Any], segment: dict[str, Any]
    ) -> None:
        data = _ensure_state()
        summary = self._segment_summary(segment, config)
        segment["摘要"] = summary
        segment["状态"] = SEG_CLOSED
        segment["工作区"] = None
        previous = batch["segments"][int(segment["序号"]) - 2] if int(segment["序号"]) > 1 else None
        upstream_hash = _digest_hash(previous["摘要"]) if previous is not None else "empty"
        data["segment_cache"][_cache_key(config, segment)] = {
            "摘要": summary,
            "记录指纹": segment["记录指纹"],
            "上游摘要哈希": upstream_hash,
            "来源批次": batch["batch_id"],
            "重复行": [dict(row) for row in segment.get("重复行", [])],
            "复核编号": list(segment.get("复核编号", [])),
            "收口时间": _now(),
        }
        # 同一区间被重算后，旧批次留下的复核项标记为已被替代，避免复核清单重复。
        self._retire_reviews(
            config, segment, keep_batch=batch["batch_id"], reason="已被新批次重算替代"
        )
        batch["updated_at"] = _now()
        self._refresh_snapshot(batch)

    def _retire_reviews(
        self, config: dict[str, Any], segment: dict[str, Any], *, keep_batch: str, reason: str
    ) -> None:
        data = _ensure_state()
        segment_key = _cache_key(config, segment)
        for review in data["reviews"].values():
            if review.get("segment_key") != segment_key:
                continue
            if review.get("批次编号") == keep_batch:
                continue
            if review.get("复核状态") == REVIEW_RETIRED:
                continue
            review["复核状态"] = REVIEW_RETIRED
            review["替代批次"] = keep_batch
            review["处理时间"] = _now()
            review["说明"] = reason

    def _refresh_snapshot(self, batch: dict[str, Any]) -> None:
        data = _ensure_state()
        snapshot_id = _next_id("snapshot", "SNAP")
        closed = [seg for seg in batch["segments"] if seg["状态"] == SEG_CLOSED]
        rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        for segment in closed:
            for row in segment.get("重复行", []):
                rec_id = str(row.get("记录编号"))
                if rec_id in seen:
                    continue
                seen.add(rec_id)
                rows.append(dict(row))
        reviews = [
            review_id
            for segment in closed
            for review_id in segment.get("复核编号", [])
            if data["reviews"].get(review_id, {}).get("复核状态") != REVIEW_RETIRED
        ]
        records_scanned = sum(int(seg["水位"].get("已比对记录") or 0) for seg in closed)
        pairs = sum(int(seg["水位"].get("已比对对数") or 0) for seg in closed)
        reused = sum(1 for seg in closed if seg.get("复用"))
        snapshot = {
            "snapshot_id": snapshot_id,
            "batch_id": batch["batch_id"],
            "策略编号": batch["策略编号"],
            "创建时间": _now(),
            "扫描范围": {
                "窗口开始": batch["窗口开始"],
                "窗口结束": batch["窗口结束"],
                "分段天数": batch["分段天数"],
                "分段总数": len(batch["segments"]),
                "已收口分段": len(closed),
                "尚未收口区间": [
                    {
                        "序号": seg["序号"],
                        "区间": seg["区间"],
                        "状态": seg["状态"],
                        "水位": seg["水位"],
                    }
                    for seg in batch["segments"]
                    if seg["状态"] != SEG_CLOSED
                ],
                "分段": [
                    {
                        "序号": seg["序号"],
                        "区间": seg["区间"],
                        "状态": seg["状态"],
                        "复用": bool(seg.get("复用")),
                        "复用批次": seg.get("复用批次"),
                        "重复量": len(seg.get("重复行", [])),
                        "待确认": len(seg.get("复核编号", [])),
                        "摘要哈希": (seg.get("摘要") or {}).get("摘要哈希"),
                    }
                    for seg in batch["segments"]
                ],
            },
            "统计": {
                "记录数": records_scanned,
                "比对对数": pairs,
                "重复量": len(rows),
                "待确认": len(reviews),
                "复用分段数": reused,
            },
            "重复行": rows,
            "复核编号": reviews,
        }
        data["snapshots"][snapshot_id] = snapshot
        batch["snapshot_id"] = snapshot_id
        batch["snapshot_ids"].append(snapshot_id)

    def _finalize_if_closed(self, batch: dict[str, Any]) -> None:
        if batch["status"] in (BATCH_INTERRUPTED,):
            return
        if all(seg["状态"] == SEG_CLOSED for seg in batch["segments"]):
            # 收口时再落一份完整快照：全部已收口区间（含复用段）都进同一份结果。
            self._refresh_snapshot(batch)
            batch["status"] = BATCH_CLOSED
            batch["updated_at"] = _now()
            batch["snapshot_id"] = batch["snapshot_ids"][-1] if batch["snapshot_ids"] else None
            waiting = [
                item for item in _ensure_state()["batches"].values()
                if item.get("区间键") == batch.get("区间键") and item.get("status") == BATCH_WAITING
            ]
            for item in waiting:
                item["说明"] = f"前序批次 {batch['batch_id']} 已收口，可以推进并复用已收口分段"

    def _batch_view(self, batch: dict[str, Any]) -> dict[str, Any]:
        segments = [
            {
                key: value
                for key, value in seg.items()
                if key not in ("工作区", "摘要", "原摘要", "工作区指纹")
            }
            for seg in batch["segments"]
        ]
        for seg in segments:
            seg["摘要哈希"] = (batch["segments"][int(seg["序号"]) - 1].get("摘要") or {}).get("摘要哈希")
            seg["原摘要哈希"] = (seg.get("原摘要") or {}).get("摘要哈希") if seg.get("原摘要") else None
            seg["工作区"] = None
        pending = [seg for seg in segments if seg["状态"] != SEG_CLOSED]
        return {
            **{key: value for key, value in batch.items() if key != "segments"},
            "segments": segments,
            "水位": self._batch_watermark(batch),
            "尚未收口区间": pending,
            "已收口区间数": len(segments) - len(pending),
            "分段总数": len(segments),
            "最近结果": self._snapshot_view(batch.get("snapshot_id")),
        }

    def _batch_watermark(self, batch: dict[str, Any]) -> dict[str, Any]:
        closed = [seg for seg in batch["segments"] if seg["状态"] == SEG_CLOSED]
        current = next(
            (seg for seg in batch["segments"] if seg["状态"] != SEG_CLOSED),
            batch["segments"][-1] if batch["segments"] else None,
        )
        return {
            "已收口区间": [seg["序号"] for seg in closed],
            # 水位包含处理中分段的已比对部分，便于中断后核对续跑起点。
            "已比对记录": sum(int(seg["水位"].get("已比对记录") or 0) for seg in batch["segments"]),
            "已比对对数": sum(int(seg["水位"].get("已比对对数") or 0) for seg in batch["segments"]),
            "当前分段": None if current is None else current["序号"],
            "最后记录": None if current is None else current["水位"].get("最后记录"),
            "最后采集时间": None if current is None else current["水位"].get("最后采集时间"),
            "尚未收口": [seg["序号"] for seg in batch["segments"] if seg["状态"] != SEG_CLOSED],
        }

    def _snapshot_view(self, snapshot_id: str | None) -> dict[str, Any] | None:
        if not snapshot_id:
            return None
        data = _ensure_state()
        snapshot = data["snapshots"].get(snapshot_id)
        if snapshot is None:
            return None
        return {
            "snapshot_id": snapshot["snapshot_id"],
            "batch_id": snapshot["batch_id"],
            "创建时间": snapshot["创建时间"],
            "扫描范围": snapshot["扫描范围"],
            "统计": snapshot["统计"],
        }

    # 快照分页 -----------------------------------------------------------
    def list_results(
        self,
        batch_id: str,
        *,
        snapshot_id: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[dict[str, Any] | None, str]:
        with LOCK:
            data = _ensure_state()
            batch = self.get_batch(batch_id)
            if batch is None:
                return None, f"去重批次 {batch_id} 不存在"
            snapshot_id = snapshot_id or batch.get("snapshot_id")
            if not snapshot_id:
                return None, "该批次尚未产生结果快照，请先推进批次"
            snapshot = data["snapshots"].get(snapshot_id)
            if snapshot is None:
                return None, f"结果快照 {snapshot_id} 不存在或已丢失"
            rows = snapshot["重复行"]
            total = len(rows)
            start = max(page - 1, 0) * size
            return {
                "batch_id": batch_id,
                "snapshot_id": snapshot_id,
                "status": batch["status"],
                "closed": batch["status"] == BATCH_CLOSED,
                "total": total,
                "page": page,
                "size": size,
                "items": [dict(row) for row in rows[start:start + size]],
                "扫描范围": snapshot["扫描范围"],
                "统计": snapshot["统计"],
            }, ""

    def list_snapshots(self, batch_id: str) -> list[dict[str, Any]]:
        data = _ensure_state()
        batch = self.get_batch(batch_id)
        if batch is None:
            return []
        return [
            view for view in (
                self._snapshot_view(snapshot_id) for snapshot_id in batch.get("snapshot_ids", [])
            )
            if view is not None
        ]

    # 复核清单 -----------------------------------------------------------
    def list_reviews(
        self,
        *,
        policy: str | None = None,
        batch_id: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        data = _ensure_state()
        rows = sorted(data["reviews"].values(), key=lambda item: item["复核编号"])
        if policy:
            rows = [row for row in rows if row.get("策略编号") == policy]
        if status:
            rows = [row for row in rows if row.get("复核状态") == status]
        selected_keys: set[str] | None = None
        if batch_id:
            batch = self.get_batch(batch_id)
            if batch is None:
                return []
            selected_keys = {
                _cache_key(self._batch_config(batch), seg) for seg in batch["segments"]
            }
            rows = [
                row for row in rows
                if row.get("segment_key") in selected_keys
                and row.get("复核状态") != REVIEW_RETIRED
            ]
        return [self._review_view(row) for row in rows]

    def _review_view(self, review: dict[str, Any]) -> dict[str, Any]:
        data = _ensure_state()
        batch = data["batches"].get(str(review.get("批次编号")))
        batch_status = batch.get("status") if batch else None
        closed = batch_status == BATCH_CLOSED
        return {
            **review,
            "批次收口状态": "已收口" if closed else (batch_status or "未知"),
            "批次已收口": closed,
        }

    def act_review(self, review_id: str, action: str) -> tuple[dict[str, Any] | None, str]:
        with LOCK:
            data = _ensure_state()
            review = data["reviews"].get(review_id)
            if review is None:
                return None, f"复核项 {review_id} 不存在"
            if action not in ("确认重复", "确认保留"):
                return None, f"动作「{action}」不属于去重复核可执行范围"
            review["复核状态"] = REVIEW_DUP if action == "确认重复" else REVIEW_KEEP
            review["处理时间"] = _now()
            STATE.save()
            return self._review_view(review), f"复核项 {review_id} 已{action}"

    # 报表口径 -----------------------------------------------------------
    def report_summary(
        self, *, policy: str | None = None, batch_id: str | None = None
    ) -> dict[str, Any]:
        data = _ensure_state()
        batches: list[dict[str, Any]] = []
        if batch_id:
            selected = [self.get_batch(batch_id)] if self.get_batch(batch_id) else []
        else:
            latest: dict[str, dict[str, Any]] = {}
            for batch in sorted(data["batches"].values(), key=lambda item: int(item["id"])):
                latest[batch["策略编号"]] = batch
            selected = sorted(latest.values(), key=lambda item: item["策略编号"])
            if policy:
                selected = [item for item in selected if item["策略编号"] == policy]
        summaries: list[dict[str, Any]] = []
        segment_rows: list[dict[str, Any]] = []
        for batch in selected:
            snapshot = data["snapshots"].get(batch.get("snapshot_id") or "")
            closed = [seg for seg in batch["segments"] if seg["状态"] == SEG_CLOSED]
            pending = [seg for seg in batch["segments"] if seg["状态"] != SEG_CLOSED]
            review_ids = [
                review_id
                for seg in batch["segments"]
                for review_id in seg.get("复核编号", [])
                if data["reviews"].get(review_id, {}).get("复核状态") != REVIEW_RETIRED
            ]
            pending_reviews = [
                data["reviews"].get(review_id)
                for review_id in review_ids
                if data["reviews"].get(review_id, {}).get("复核状态") == REVIEW_PENDING
            ]
            reused_segments = [seg for seg in closed if seg.get("复用")]
            reused_duplicates = sum(len(seg.get("重复行", [])) for seg in reused_segments)
            scanned = sum(
                int(seg["水位"].get("已比对记录") or 0) for seg in closed if not seg.get("复用")
            )
            reused_scanned = sum(
                int(seg["水位"].get("已比对记录") or 0) for seg in reused_segments
            )
            summary = {
                "batch_id": batch["batch_id"],
                "策略编号": batch["策略编号"],
                "status": batch["status"],
                "已收口": batch["status"] == BATCH_CLOSED,
                "扫描范围": {
                    "窗口开始": batch["窗口开始"],
                    "窗口结束": batch["窗口结束"],
                    "分段天数": batch["分段天数"],
                    "分段总数": len(batch["segments"]),
                    "已收口分段": len(closed),
                    "本次扫描记录": scanned,
                    "本次扫描比对对数": sum(
                        int(seg["水位"].get("已比对对数") or 0)
                        for seg in closed if not seg.get("复用")
                    ),
                },
                "被复用结果": {
                    "复用分段数": len(reused_segments),
                    "复用记录数": reused_scanned,
                    "复用重复量": reused_duplicates,
                    "复用原摘要": [
                        {
                            "序号": seg["序号"],
                            "区间": seg["区间"],
                            "摘要哈希": seg.get("原摘要", (seg.get("摘要") or {})).get("摘要哈希")
                            if seg.get("原摘要") else (seg.get("摘要") or {}).get("摘要哈希"),
                            "来源批次": seg.get("复用批次"),
                        }
                        for seg in reused_segments
                    ],
                },
                "重复量": int((snapshot or {}).get("统计", {}).get("重复量", 0)),
                "仍需人工确认": len(pending_reviews),
                "已确认重复": sum(
                    1
                    for review_id in review_ids
                    if data["reviews"].get(review_id, {}).get("复核状态") == REVIEW_DUP
                ),
                "已确认保留": sum(
                    1
                    for review_id in review_ids
                    if data["reviews"].get(review_id, {}).get("复核状态") == REVIEW_KEEP
                ),
                "尚未收口区间": [
                    {
                        "序号": seg["序号"],
                        "区间": seg["区间"],
                        "状态": seg["状态"],
                        "水位": seg["水位"],
                    }
                    for seg in pending
                ],
                "snapshot_id": batch.get("snapshot_id"),
            }
            summaries.append(summary)
            for seg in batch["segments"]:
                segment_rows.append({
                    "batch_id": batch["batch_id"],
                    "策略编号": batch["策略编号"],
                    "序号": seg["序号"],
                    "区间": seg["区间"],
                    "状态": seg["状态"],
                    "复用": bool(seg.get("复用")),
                    "复用批次": seg.get("复用批次"),
                    "水位": seg["水位"],
                    "重复量": len(seg.get("重复行", [])),
                    "待确认": len(seg.get("复核编号", [])),
                    "摘要哈希": (seg.get("摘要") or {}).get("摘要哈希"),
                })
        totals = {
            "批次数量": len(summaries),
            "未收口批次数": sum(1 for item in summaries if not item["已收口"]),
            "扫描记录数": sum(item["扫描范围"]["本次扫描记录"] for item in summaries),
            "重复量": sum(item["重复量"] for item in summaries),
            "仍需人工确认": sum(item["仍需人工确认"] for item in summaries),
            "复用分段数": sum(item["被复用结果"]["复用分段数"] for item in summaries),
            "复用重复量": sum(item["被复用结果"]["复用重复量"] for item in summaries),
        }
        return {"summary": totals, "batches": summaries, "segments": segment_rows}

    # 历史批次 -----------------------------------------------------------
    def bootstrap_history(self) -> None:
        """首次启动时把 DEDU-0003 的历史窗口按原摘要收口，作为可复用基线。"""
        with LOCK:
            data = _ensure_state()
            if data.get("bootstrapped"):
                return
            policy = next(
                (row for row in store.rows(MODULE) if _policy_number(row) == "DEDU-0003"),
                None,
            )
            if policy is not None:
                config = _config(policy)
                config["配置哈希"] = _config_hash(config)
                batch_id = _next_id("batch", "BATCH")
                batch = {
                    "id": int(data["counters"]["batch"]),
                    "batch_id": batch_id,
                    "策略编号": config["策略编号"],
                    "policy_id": policy.get("id"),
                    "时间窗口": f"{_iso_day(config['窗口开始'])} 至 {_iso_day(config['窗口结束'])}",
                    "窗口开始": _iso_day(config["窗口开始"]),
                    "窗口结束": _iso_day(config["窗口结束"]),
                    "分段天数": config["分段天数"],
                    "相似阈值": config["相似阈值"],
                    "复核下限": config["复核下限"],
                    "保留策略": config["保留策略"],
                    "业务键列": config["业务键列"],
                    "比对字段": config["比对字段"],
                    "排序字段": config["排序字段"],
                    "配置哈希": config["配置哈希"],
                    "区间键": self._interval_key(config),
                    "status": BATCH_PENDING,
                    "blocked_by": None,
                    "历史批次": True,
                    "created_at": "2026-08-29 09:00:00",
                    "updated_at": "2026-08-29 09:00:00",
                    "segments": self._init_segments(config),
                    "snapshot_ids": [],
                    "snapshot_id": None,
                    "推进预算": None,
                }
                data["batches"][batch_id] = batch
                self._advance_batch(batch, None)
                if batch["snapshot_id"]:
                    data["snapshots"][batch["snapshot_id"]]["创建时间"] = "2026-08-29 09:05:00"
                    data["snapshots"][batch["snapshot_id"]]["历史快照"] = True
                data["bootstrapped"] = True
                STATE.save()


def bootstrap_dedup_state() -> None:
    """服务启动时调用：恢复持久化状态，并补齐历史批次基线。"""
    with LOCK:
        _ensure_state()
        DedupService().bootstrap_history()
