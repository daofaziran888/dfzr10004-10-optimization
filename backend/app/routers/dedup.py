"""采集去重比对接口。

既有接口保持不变：策略列表、明细、登记、启用/停用、导出。
增量去重新增三组接口：
- /batches        发起、推进、中断、查询批次（水位/分段进度/最近结果）
- /results        按快照分页读取去重结果（翻页沿用同一 snapshot_id）
- /reviews        复核清单读写（与去重批次收口状态同步）
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.dedup import MAX_PAGE_SIZE, DedupService

router = APIRouter(prefix="/api/dedup", tags=["采集去重比对"])

service = DedupService()

LIST_FIELDS = ["策略编号", "业务键列", "排序字段", "保留策略", "时间窗口", "相似阈值", "比对字段", "是否启用"]
STATUSES = ["草稿", "待校验", "已启用", "已停用"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按策略编号检索"),
    status: str | None = Query(default=None, description="草稿、待校验、已启用、已停用"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按策略编号与状态过滤采集去重比对列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


# ---------------------------------------------------------------------------
# 增量批次
# ---------------------------------------------------------------------------
def _require_policy(entry_id: int) -> dict[str, Any]:
    policy = service.get_entry(entry_id)
    if policy is None:
        raise HTTPException(status_code=404, detail=f"去重策略 {entry_id} 不存在或已归档")
    return policy


@router.post("/{entry_id}/batches", response_model=ActionResult)
def start_batch(entry_id: int, payload: EntryPayload) -> ActionResult:
    """为策略发起一次增量去重批次；同窗口已有批次推进时，新批次进入等待。"""
    policy = _require_policy(entry_id)
    batch, message = service.start_batch(policy, payload.values)
    if batch is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=service.batch_view(batch["batch_id"]))


@router.get("/batches/{batch_id}/reviews", response_model=dict)
def batch_reviews(batch_id: str) -> dict[str, Any]:
    """批次内的复核清单：与批次收口状态同步，供复核侧直接读取。"""
    if service.get_batch(batch_id) is None:
        raise HTTPException(status_code=404, detail=f"去重批次 {batch_id} 不存在")
    items = service.list_reviews(batch_id=batch_id)
    pending = sum(1 for item in items if item["复核状态"] == "待确认")
    return {"items": items, "total": len(items), "pending": pending}


@router.get("/batches", response_model=dict)
def list_batches(
    policy: str | None = Query(default=None, description="按策略编号过滤"),
    status: str | None = Query(default=None, description="待处理、处理中、等待中、已中断、已收口"),
) -> dict[str, Any]:
    """列出批次进度：包含水位、已收口/未收口区间与最近结果快照。"""
    items = service.list_batches(policy=policy, status=status)
    return {"items": items, "total": len(items)}


@router.get("/batches/{batch_id}", response_model=dict)
def get_batch(batch_id: str) -> dict:
    """读取单个批次：水位、分段进度、最近结果快照与收口状态。"""
    batch = service.batch_view(batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail=f"去重批次 {batch_id} 不存在")
    return batch


@router.post("/batches/{batch_id}/advance", response_model=ActionResult)
def advance_batch(batch_id: str, payload: EntryPayload) -> ActionResult:
    """推进批次；可带推进记录数做分批推进，中断后再次推进会从上次水位续跑。"""
    batch, message = service.advance_batch(batch_id, payload.values)
    if batch is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=service.batch_view(batch["batch_id"]))


@router.post("/batches/{batch_id}/interrupt", response_model=ActionResult)
def interrupt_batch(batch_id: str) -> ActionResult:
    """中断批次：已收口区间不被回退，未收口区间保留水位等待续跑。"""
    batch, message = service.interrupt_batch(batch_id)
    if batch is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=service.batch_view(batch["batch_id"]))


@router.get("/batches/{batch_id}/snapshots", response_model=dict)
def list_snapshots(batch_id: str) -> dict[str, Any]:
    """列出批次产生过的结果快照；分页读取时按 snapshot_id 固定同一份结果。"""
    if service.get_batch(batch_id) is None:
        raise HTTPException(status_code=404, detail=f"去重批次 {batch_id} 不存在")
    items = service.list_snapshots(batch_id)
    return {"items": items, "total": len(items)}


@router.get("/batches/{batch_id}/results", response_model=dict)
def list_results(
    batch_id: str,
    snapshot_id: str | None = Query(default=None, description="沿用同一快照翻页；缺省取批次当前快照"),
    page: int = 1,
    size: int = 20,
) -> dict[str, Any]:
    """按快照分页读取去重结果；同一组重复记录不会因翻页重复计数。"""
    if size > MAX_PAGE_SIZE:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    result, message = service.list_results(
        batch_id, snapshot_id=snapshot_id, page=max(page, 1), size=max(size, 1)
    )
    if result is None:
        raise HTTPException(status_code=404, detail=message)
    return result


# ---------------------------------------------------------------------------
# 复核清单
# ---------------------------------------------------------------------------
@router.get("/reviews", response_model=dict)
def list_reviews(
    policy: str | None = Query(default=None, description="按策略编号过滤"),
    batch_id: str | None = Query(default=None, description="按批次读取其区间内的复核项"),
    status: str | None = Query(default=None, description="待确认、已确认重复、已确认保留"),
) -> dict[str, Any]:
    """复核清单：去重结果同步到这里，并带出所属批次是否已收口。"""
    items = service.list_reviews(policy=policy, batch_id=batch_id, status=status)
    return {"items": items, "total": len(items)}


@router.post("/reviews/{review_id}/actions", response_model=ActionResult)
def act_review(review_id: str, payload: EntryPayload) -> ActionResult:
    """处理复核项：确认重复或确认保留。"""
    action = str(payload.values.get("action") or "").strip()
    review, message = service.act_review(review_id, action)
    if review is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=review)


# ---------------------------------------------------------------------------
# 报表口径
# ---------------------------------------------------------------------------
@router.get("/report", response_model=dict)
def dedup_report(
    policy: str | None = Query(default=None, description="按策略编号过滤"),
    batch_id: str | None = Query(default=None, description="只看某个批次"),
) -> dict[str, Any]:
    """采集统计口径：本次扫描范围、被复用结果、仍需人工确认数量与未收口区间。"""
    return service.report_summary(policy=policy, batch_id=batch_id)


# ---------------------------------------------------------------------------
# 采集记录与导出
# ---------------------------------------------------------------------------
@router.post("/{entry_id}/records", response_model=ActionResult)
def add_records(entry_id: int, payload: EntryPayload) -> ActionResult:
    """追加采集记录：只让受影响的区间及下游失效，其余段继续复用。"""
    policy = _require_policy(entry_id)
    records = payload.values.get("records")
    if not isinstance(records, list) or not records:
        return ActionResult(ok=False, message="records 需为非空数组")
    result = service.add_records(policy, records)
    return ActionResult(ok=True, message=f"已接收 {result['新增记录数']} 条记录", entry=result)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出采集去重比对清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "dedup", "total": total, "items": items}
@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条去重策略明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"去重策略 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条去重策略，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="去重策略已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条去重策略执行提交校验、启用策略、停用策略；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


