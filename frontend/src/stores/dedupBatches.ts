/**
 * 去重批次的共享数据层：批次列表、快照分页结果、复核清单都从这里取，
 * 页面只负责渲染。选中批次 / 快照 / 页码落在 localStorage，保证刷新、
 * 重新进入页面或从复核清单返回时仍沿用同一快照。
 */
import { computed, ref } from 'vue'

import { request } from '@/api/client'
import {
  clearConsoleState,
  loadConsoleState,
  saveConsoleState,
  type DedupConsoleState,
} from '@/stores/dedupConsole'

export type SegmentRow = {
  序号: number
  区间: string
  开始: string
  结束: string
  状态: string
  复用: boolean
  复用批次: string | null
  记录数: number
  重复行: Record<string, string | number | null>[]
  复核编号: string[]
  水位: {
    记录序号: number
    已比对记录: number
    已比对对数: number
    最后记录: string | null
    最后采集时间: string | null
  }
  摘要哈希: string | null
  原摘要哈希: string | null
  记录指纹: string | null
}

export type BatchView = {
  id: number
  batch_id: string
  policy_id: number | null
  策略编号: string
  status: string
  时间窗口: string
  窗口开始: string
  窗口结束: string
  分段天数: number
  相似阈值: number
  复核下限: number
  保留策略: string
  业务键列: string
  比对字段: string
  排序字段: string
  blocked_by: string | null
  历史批次: boolean
  created_at: string
  updated_at: string
  snapshot_id: string | null
  snapshot_ids: string[]
  segments: SegmentRow[]
  水位: {
    已收口区间: number[]
    已比对记录: number
    已比对对数: number
    当前分段: number | null
    最后记录: string | null
    最后采集时间: string | null
    尚未收口: number[]
  }
  尚未收口区间: SegmentRow[]
  已收口区间数: number
  分段总数: number
  最近结果: {
    snapshot_id: string
    batch_id: string
    创建时间: string
    扫描范围: Record<string, unknown>
    统计: {
      记录数: number
      比对对数: number
      重复量: number
      待确认: number
      复用分段数: number
    }
  } | null
}

export type ResultRow = Record<string, string | number | null>

export type ResultPage = {
  batch_id: string
  snapshot_id: string
  status: string
  closed: boolean
  total: number
  page: number
  size: number
  items: ResultRow[]
  扫描范围: Record<string, unknown>
  统计: {
    记录数: number
    比对对数: number
    重复量: number
    待确认: number
    复用分段数: number
  }
}

export type ReviewRow = Record<string, string | number | boolean | null>

export type DedupSummary = {
  summary: Record<string, number>
  batches: Record<string, unknown>[]
  segments: Record<string, unknown>[]
}

const state = ref<DedupConsoleState>(loadConsoleState())
const batches = ref<BatchView[]>([])
const batch = ref<BatchView | null>(null)
const results = ref<ResultPage | null>(null)
const reviews = ref<ReviewRow[]>([])
const summary = ref<DedupSummary | null>(null)
const loading = ref(false)
const message = ref('')
const error = ref('')

async function callJson(path: string, init?: RequestInit): Promise<unknown> {
  const response = await request(path, init)
  if (!response.ok) {
    let detail = `接口返回 ${response.status}`
    try {
      const payload = (await response.json()) as { detail?: unknown; message?: unknown }
      if (typeof payload.detail === 'string') detail = payload.detail
      else if (typeof payload.message === 'string') detail = payload.message
    } catch {
      // 响应不是 JSON 时保留状态码说明。
    }
    throw new Error(detail)
  }
  return response.json()
}

function persist(patch: Partial<DedupConsoleState>) {
  state.value = saveConsoleState(patch)
}

export function useDedupConsole() {
  const activeBatchId = computed(() => state.value.batchId)
  const isClosed = computed(() => batch.value?.status === '已收口')
  const closedSegments = computed(() =>
    (batch.value?.segments ?? []).filter((segment) => segment.状态 === '已收口'),
  )
  const openSegments = computed(() =>
    (batch.value?.segments ?? []).filter((segment) => segment.状态 !== '已收口'),
  )

  async function loadBatches(policy = state.value.policy) {
    loading.value = true
    error.value = ''
    try {
      const query = new URLSearchParams()
      if (policy) query.set('policy', policy)
      const payload = (await callJson(`/api/dedup/batches?${query.toString()}`)) as {
        items: BatchView[]
      }
      batches.value = payload.items ?? []
      return batches.value
    } catch (cause) {
      error.value = cause instanceof Error ? cause.message : '批次列表读取失败'
      return []
    } finally {
      loading.value = false
    }
  }

  async function selectBatch(batchId: string, options: { resetPage?: boolean } = {}) {
    batch.value = null
    results.value = null
    reviews.value = []
    loading.value = true
    error.value = ''
    try {
      const view = (await callJson(`/api/dedup/batches/${batchId}`)) as BatchView
      batch.value = view
      const closed = view.status === '已收口'
      const previous = state.value
      persist({
        batchId,
        snapshotId: previous.batchId === batchId ? previous.snapshotId : view.snapshot_id,
        page: previous.batchId === batchId && !options.resetPage ? previous.page : 1,
        policy: view.策略编号,
        lastBatchStatus: view.status,
      })
      await Promise.all([loadResults(!closed), loadReviews()])
      return view
    } catch (cause) {
      error.value = cause instanceof Error ? cause.message : '批次读取失败'
      return null
    } finally {
      loading.value = false
    }
  }

  async function loadResults(followLatest = false) {
    const batchId = state.value.batchId
    if (!batchId) return null
    error.value = ''
    try {
      const query = new URLSearchParams({
        page: String(state.value.page),
        size: String(state.value.size),
      })
      const view = batch.value
      const closed = view?.status === '已收口'
      const snapshotId = closed ? state.value.snapshotId : null
      if (snapshotId) query.set('snapshot_id', snapshotId)
      const payload = (await callJson(
        `/api/dedup/batches/${batchId}/results?${query.toString()}`,
      )) as ResultPage
      results.value = payload
      if (closed && payload.snapshot_id) {
        persist({ snapshotId: payload.snapshot_id, page: payload.page, size: payload.size })
      }
      return payload
    } catch (cause) {
      const detail = cause instanceof Error ? cause.message : '结果读取失败'
      // 快照尚未产生是正常状态（批次还没推进），不当作错误提示。
      results.value = null
      if (!detail.includes('尚未产生结果快照')) error.value = detail
      return null
    }
  }

  async function loadReviews() {
    const batchId = state.value.batchId
    const policy = state.value.policy || batch.value?.策略编号 || ''
    if (!batchId && !policy) {
      reviews.value = []
      return []
    }
    error.value = ''
    try {
      const query = new URLSearchParams()
      if (batchId) query.set('batch_id', batchId)
      else if (policy) query.set('policy', policy)
      const payload = (await callJson(`/api/dedup/reviews?${query.toString()}`)) as {
        items: ReviewRow[]
      }
      reviews.value = payload.items ?? []
      return reviews.value
    } catch (cause) {
      error.value = cause instanceof Error ? cause.message : '复核清单读取失败'
      return []
    }
  }

  async function loadSummary(policy = '') {
    const query = new URLSearchParams()
    if (policy) query.set('policy', policy)
    try {
      summary.value = (await callJson(`/api/dedup/report?${query.toString()}`)) as DedupSummary
      return summary.value
    } catch (cause) {
      error.value = cause instanceof Error ? cause.message : '统计口径读取失败'
      return null
    }
  }

  async function goToPage(page: number) {
    persist({ page: Math.max(1, page) })
    return loadResults()
  }

  async function changeSize(size: number) {
    persist({ size: Math.max(1, size), page: 1 })
    return loadResults()
  }

  async function startBatch(policyId: number, body: Record<string, unknown> = {}) {
    error.value = ''
    const payload = (await callJson(`/api/dedup/${policyId}/batches`, {
      method: 'POST',
      body: JSON.stringify(body),
    })) as { ok: boolean; message: string; entry: BatchView }
    message.value = payload.message
    if (!payload.ok || !payload.entry) throw new Error(payload.message)
    batch.value = payload.entry
    persist({
      batchId: payload.entry.batch_id,
      snapshotId: payload.entry.snapshot_id,
      page: 1,
      policy: payload.entry.策略编号,
      lastBatchStatus: payload.entry.status,
    })
    await Promise.all([loadBatches(), loadResults(), loadReviews()])
    return payload.entry
  }

  async function advanceBatch(batchId: string, budget?: number) {
    error.value = ''
    const body = budget && budget > 0 ? { 推进记录数: budget } : {}
    const payload = (await callJson(`/api/dedup/batches/${batchId}/advance`, {
      method: 'POST',
      body: JSON.stringify(body),
    })) as { ok: boolean; message: string; entry: BatchView }
    message.value = payload.message
    if (!payload.ok || !payload.entry) throw new Error(payload.message)
    batch.value = payload.entry
    persist({
      snapshotId: payload.entry.snapshot_id,
      lastBatchStatus: payload.entry.status,
    })
    await Promise.all([loadBatches(), loadResults(), loadReviews()])
    return payload.entry
  }

  async function interruptBatch(batchId: string) {
    error.value = ''
    const payload = (await callJson(`/api/dedup/batches/${batchId}/interrupt`, {
      method: 'POST',
      body: JSON.stringify({}),
    })) as { ok: boolean; message: string; entry: BatchView }
    message.value = payload.message
    if (!payload.ok || !payload.entry) throw new Error(payload.message)
    batch.value = payload.entry
    persist({
      snapshotId: payload.entry.snapshot_id,
      lastBatchStatus: payload.entry.status,
    })
    await Promise.all([loadBatches(), loadResults(), loadReviews()])
    return payload.entry
  }

  async function actReview(reviewId: string, action: string) {
    error.value = ''
    const payload = (await callJson(`/api/dedup/reviews/${reviewId}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })) as { ok: boolean; message: string }
    message.value = payload.message
    if (!payload.ok) throw new Error(payload.message)
    await Promise.all([loadReviews(), loadSummary(state.value.policy)])
    return payload
  }

  function resetConsole() {
    state.value = clearConsoleState()
    batch.value = null
    results.value = null
    reviews.value = []
    summary.value = null
  }

  return {
    state,
    batches,
    batch,
    results,
    reviews,
    summary,
    loading,
    message,
    error,
    activeBatchId,
    isClosed,
    closedSegments,
    openSegments,
    loadBatches,
    selectBatch,
    loadResults,
    loadReviews,
    loadSummary,
    goToPage,
    changeSize,
    startBatch,
    advanceBatch,
    interruptBatch,
    actReview,
    resetConsole,
  }
}
