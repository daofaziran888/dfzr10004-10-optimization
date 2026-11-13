<template>
  <section class="page" data-module="report">
    <header class="page-head">
      <div>
        <h2>采集统计报表管理</h2>
        <p class="page-desc">
          维护采集统计的维度配置、周期设置与分发策略，为采集效能评估提供可验证的数据工程流程。
          下方的去重口径直接取自批次快照，说明本次扫描范围、被复用结果与仍需人工确认的数量。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记统计报表</button>
        <button class="btn" type="button" @click="exportRows">导出采集统计报表清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无采集统计报表数据，可先登记统计报表</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条采集统计报表记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>

    <section class="dedup-report">
      <div class="report-head">
        <div>
          <h3>去重批次统计口径</h3>
          <p class="report-desc">
            数据取自去重批次的不可变结果快照：本次扫描范围、被复用结果、重复量与仍需人工确认数量。
            刷新页面或从复核清单返回时沿用同一批次与同一快照。
          </p>
        </div>
        <div class="report-actions">
          <label class="filter-item">
            <span>策略编号</span>
            <input v-model="policyFilter" placeholder="全部策略" @change="refreshSummary" />
          </label>
          <button class="btn" type="button" @click="refreshSummary">刷新口径</button>
        </div>
      </div>

      <p v-if="summaryError" class="error-text">{{ summaryError }}</p>
      <p v-if="message" class="console-message">{{ message }}</p>

      <div v-if="summary" class="stat-row">
        <article class="stat-card">
          <span class="stat-label">本次扫描记录</span>
          <strong class="stat-value">{{ summary.summary.扫描记录数 ?? 0 }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">重复量（快照口径）</span>
          <strong class="stat-value">{{ summary.summary.重复量 ?? 0 }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">被复用分段</span>
          <strong class="stat-value">{{ summary.summary.复用分段数 ?? 0 }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">被复用重复量</span>
          <strong class="stat-value">{{ summary.summary.复用重复量 ?? 0 }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">仍需人工确认</span>
          <strong class="stat-value">{{ summary.summary.仍需人工确认 ?? 0 }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">未收口批次</span>
          <strong class="stat-value">{{ summary.summary.未收口批次数 ?? 0 }}</strong>
        </article>
      </div>

      <table v-if="summaryBatches.length" class="data-table compact">
        <thead>
          <tr>
            <th>批次</th><th>策略</th><th>状态</th><th>扫描范围</th><th>本次扫描</th>
            <th>被复用结果</th><th>重复量</th><th>待人工确认</th><th>尚未收口区间</th><th>快照</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in summaryBatches" :key="String(item.batch_id)">
            <td>{{ item.batch_id }}</td>
            <td>{{ item.策略编号 }}</td>
            <td>
              <em :class="statusClass(String(item.status))">{{ item.status }}</em>
            </td>
            <td>{{ rangeText(item) }}</td>
            <td>{{ scanText(item) }}</td>
            <td>{{ reuseText(item) }}</td>
            <td>{{ item.重复量 }}</td>
            <td>{{ item.仍需人工确认 }}</td>
            <td>{{ openText(item) }}</td>
            <td class="mono">{{ item.snapshot_id ?? '—' }}</td>
          </tr>
        </tbody>
      </table>
      <p v-else-if="summary" class="empty-state">还没有可展示的去重批次；先在采集去重比对页发起批次。</p>

      <div class="report-foot">
        <button class="btn" type="button" @click="goDedup">前往采集去重比对</button>
        <button class="btn" type="button" @click="syncSnapshot">沿用当前快照刷新口径</button>
        <button class="btn ghost" type="button" @click="openReviewList">查看复核清单</button>
      </div>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'

import { request } from '@/api/client'
import { useDedupConsole } from '@/stores/dedupBatches'
import { loadConsoleState } from '@/stores/dedupConsole'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/report'
const columns = ["报表编号", "统计维度", "统计周期", "指标集合", "数据来源", "生成时间", "报表格式", "分发方式"]
const actions = ["生成报表", "归档报表", "删除报表"]
const statuses = ["草稿", "生成中", "已生成", "已归档"]
const stats = [{"label": "统计报表总数", "value": 0}, {"label": "已生成报表", "value": 0}, {"label": "已归档报表", "value": 0}]

const router = useRouter()

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)

const { loadSummary, loadBatches, message } = useDedupConsole()
const policyFilter = ref(loadConsoleState().policy)
const summaryError = ref('')
const summary = ref<Awaited<ReturnType<typeof loadSummary>> | null>(null)

const summaryBatches = computed(() => {
  const items = summary.value?.batches
  return Array.isArray(items) ? items : []
})

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '统计报表登记入口尚未接入审批流'
}

function rangeText(item: Record<string, unknown>) {
  const range = item.扫描范围 as Record<string, unknown> | undefined
  if (!range) return '—'
  return `${range.窗口开始} ~ ${range.窗口结束}（${range.分段总数} 段，每段 ${range.分段天数} 天）`
}

function scanText(item: Record<string, unknown>) {
  const range = item.扫描范围 as Record<string, unknown> | undefined
  if (!range) return '—'
  return `${range.本次扫描记录} 条 / ${range.本次扫描比对对数} 对`
}

function reuseText(item: Record<string, unknown>) {
  const reuse = item.被复用结果 as Record<string, unknown> | undefined
  if (!reuse) return '—'
  const digests = (reuse.复用原摘要 as Record<string, unknown>[]) ?? []
  const hashes = digests.map((entry) => `${entry.序号}段:${entry.摘要哈希}`).join('，')
  return `${reuse.复用分段数} 段 / ${reuse.复用重复量} 条重复${hashes ? `（${hashes}）` : ''}`
}

function openText(item: Record<string, unknown>) {
  const pending = (item.尚未收口区间 as Record<string, unknown>[]) ?? []
  if (!pending.length) return '无'
  return pending
    .map((segment) => {
      const watermark = segment.水位 as Record<string, unknown>
      return `${segment.序号}段 ${segment.状态}（已比对 ${watermark.已比对记录}）`
    })
    .join('；')
}

function statusClass(status: string) {
  if (status === '已收口') return 'tag tag-ok'
  if (status === '已中断') return 'tag tag-warn'
  if (status === '等待中') return 'tag tag-wait'
  return 'tag'
}

function goDedup() {
  void router.push('/dedup')
}

function openReviewList() {
  const state = loadConsoleState()
  if (state.batchId) {
    window.localStorage.setItem(
      'dedup-review-focus',
      JSON.stringify({ batchId: state.batchId, snapshotId: state.snapshotId }),
    )
  }
  void router.push('/dedup')
}

async function syncSnapshot() {
  const state = loadConsoleState()
  summaryError.value = ''
  try {
    const query = new URLSearchParams()
    if (state.policy) query.set('policy', state.policy)
    if (state.batchId) query.set('batch_id', state.batchId)
    const response = await request(`${ENDPOINT}/dedup-summary?${query.toString()}`)
    if (!response.ok) throw new Error('去重口径刷新失败')
    summary.value = (await response.json()) as Awaited<ReturnType<typeof loadSummary>>
  } catch (error) {
    summaryError.value = error instanceof Error ? error.message : '去重口径刷新失败'
  }
}

async function refreshSummary() {
  summaryError.value = ''
  try {
    summary.value = await loadSummary(policyFilter.value.trim())
  } catch (error) {
    summaryError.value = error instanceof Error ? error.message : '去重口径读取失败'
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('采集统计报表动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集统计报表操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('统计报表列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集统计报表列表读取失败'
  }
}

onMounted(async () => {
  await Promise.all([reload(), loadBatches(), refreshSummary()])
})
</script>

<style scoped>
.dedup-report { margin-top: 28px; border-top: 1px solid var(--border); padding-top: 16px; }
.report-head { display: flex; justify-content: space-between; gap: 16px; align-items: flex-start; }
.report-head h3 { margin: 0 0 4px; }
.report-desc { color: var(--muted); font-size: 12px; margin: 4px 0 12px; }
.report-actions { display: flex; gap: 8px; align-items: flex-end; }
.data-table.compact th, .data-table.compact td { padding: 6px 8px; font-size: 12px; }
.report-foot { display: flex; gap: 8px; margin-top: 12px; }
.console-message { color: #0f7b46; font-size: 13px; margin: 6px 0; }
.mono { font-family: Menlo, Consolas, monospace; }
.tag { display: inline-block; padding: 0 6px; border-radius: 4px; background: #eef2f7; color: #334155; font-style: normal; font-size: 11px; }
.tag-ok { background: #e7f6ed; color: #0f7b46; }
.tag-warn { background: #fef3e2; color: #b45309; }
.tag-wait { background: #f1edfb; color: #6d28d9; }
</style>
