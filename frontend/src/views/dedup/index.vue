<template>
  <section class="page" data-module="dedup">
    <header class="page-head">
      <div>
        <h2>采集去重比对管理</h2>
        <p class="page-desc">
          维护采集去重的业务键定义、相似度算法与保留策略；大窗口按业务键与时间边界拆成增量区间，
          按水位推进，已收口区间复用原摘要。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记去重策略</button>
        <button class="btn" type="button" @click="exportRows">导出采集去重比对清单</button>
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
          <th>增量批次</th>
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
          <td>
            <button class="link" type="button" :disabled="busy" @click="createBatch(row)">
              发起增量批次
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 2" class="empty-state">暂无采集去重比对数据，可先登记去重策略</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条采集去重比对记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>

    <section class="console">
      <div class="console-head">
        <div>
          <h3>增量去重批次</h3>
          <p class="console-desc">
            窗口按业务键列与时间边界拆段；中断后从上次水位续跑，已收口区间不回退。
            翻页固定同一快照，刷新或返回列表仍沿用原快照与原摘要。
          </p>
        </div>
        <div class="console-actions">
          <label class="filter-item">
            <span>策略编号</span>
            <input v-model="policyFilter" placeholder="如 DEDU-0001" @change="refreshBatches" />
          </label>
          <button class="btn" type="button" :disabled="busy" @click="refreshBatches">刷新批次</button>
          <button class="btn ghost" type="button" @click="resetView">重置视图</button>
        </div>
      </div>

      <p v-if="consoleMessage" class="console-message">{{ consoleMessage }}</p>
      <p v-if="consoleError" class="error-text">{{ consoleError }}</p>

      <div v-if="!batches.length" class="empty-state console-empty">
        暂无去重批次；在策略行点击「发起增量批次」后会按窗口拆段并生成水位。
      </div>

      <div v-else class="console-grid">
        <aside class="batch-list">
          <button
            v-for="item in batches"
            :key="item.batch_id"
            class="batch-item"
            type="button"
            :class="{ active: item.batch_id === selectedBatchId }"
            @click="selectBatch(item.batch_id)"
          >
            <strong>{{ item.batch_id }}</strong>
            <span>{{ item.策略编号 }} · {{ item.时间窗口 }}</span>
            <span>
              <em :class="statusClass(item.status)">{{ item.status }}</em>
              · {{ item.已收口区间数 }}/{{ item.分段总数 }} 段已收口
              <template v-if="item.blocked_by"> · 等待 {{ item.blocked_by }}</template>
            </span>
          </button>
        </aside>

        <div v-if="selectedBatch" class="batch-detail">
          <div class="batch-summary">
            <div class="batch-summary-head">
              <h4>{{ selectedBatch.batch_id }} · {{ selectedBatch.策略编号 }}</h4>
              <em :class="statusClass(selectedBatch.status)">{{ selectedBatch.status }}</em>
            </div>
            <dl class="kv-grid">
              <div><dt>窗口</dt><dd>{{ selectedBatch.时间窗口 }}</dd></div>
              <div><dt>分段</dt><dd>{{ selectedBatch.分段总数 }} 段 / 每段 {{ selectedBatch.分段天数 }} 天</dd></div>
              <div><dt>保留策略</dt><dd>{{ selectedBatch.保留策略 }}</dd></div>
              <div><dt>相似阈值</dt><dd>{{ selectedBatch.相似阈值 }}（复核下限 {{ selectedBatch.复核下限 }}）</dd></div>
              <div><dt>业务键 / 比对字段</dt><dd>{{ selectedBatch.业务键列 }} / {{ selectedBatch.比对字段 }}</dd></div>
              <div><dt>最近快照</dt><dd>{{ selectedBatch.snapshot_id ?? '尚未产生' }}</dd></div>
            </dl>
            <div class="watermark">
              <span>水位：已比对 {{ selectedBatch.水位.已比对记录 }} 条 / {{ selectedBatch.水位.已比对对数 }} 对</span>
              <span>已收口区间：{{ selectedBatch.水位.已收口区间.join('、') || '无' }}</span>
              <span>尚未收口：{{ selectedBatch.水位.尚未收口.join('、') || '无' }}</span>
              <span v-if="selectedBatch.水位.最后记录">
                最后记录 {{ selectedBatch.水位.最后记录 }}（{{ selectedBatch.水位.最后采集时间 }}）
              </span>
            </div>
            <div class="console-actions">
              <label class="filter-item">
                <span>本次推进记录数（留空则跑完）</span>
                <input v-model.number="advanceBudget" type="number" min="1" placeholder="如 5" />
              </label>
              <button class="btn primary" type="button" :disabled="busy || selectedBatch.status === '已收口'" @click="advance">
                推进批次
              </button>
              <button class="btn" type="button" :disabled="busy || selectedBatch.status === '已收口'" @click="interrupt">
                中断批次
              </button>
              <button class="btn ghost" type="button" :disabled="busy" @click="refreshSelected">刷新进度</button>
            </div>
            <p v-if="selectedBatch.status === '等待中'" class="notice">
              同一区间已有批次在推进，本批次进入等待；前一批次收口或中断后再次推进会复用已收口区间。
            </p>
          </div>

          <div class="panel">
            <h4>分段进度与水位</h4>
            <table class="data-table compact">
              <thead>
                <tr>
                  <th>段</th><th>区间</th><th>状态</th><th>已比对</th><th>比对对数</th><th>重复量</th>
                  <th>待确认</th><th>复用</th><th>最后记录</th><th>摘要哈希</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="segment in selectedBatch.segments" :key="segment.序号">
                  <td>{{ segment.序号 }}</td>
                  <td>{{ segment.区间 }}</td>
                  <td><em :class="segmentClass(segment.状态)">{{ segment.状态 }}</em></td>
                  <td>{{ segment.水位.已比对记录 }}/{{ segment.记录数 }}</td>
                  <td>{{ segment.水位.已比对对数 }}</td>
                  <td>{{ segment.重复行.length }}</td>
                  <td>{{ segment.复核编号.length }}</td>
                  <td>
                    <template v-if="segment.复用">复用 {{ segment.复用批次 }}</template>
                    <template v-else>—</template>
                  </td>
                  <td>{{ segment.水位.最后记录 ?? '—' }}</td>
                  <td class="mono">
                    {{ segment.摘要哈希 ?? '—' }}
                    <small v-if="segment.原摘要哈希 && segment.摘要哈希 !== segment.原摘要哈希" class="error-text">
                      （原 {{ segment.原摘要哈希 }}）
                    </small>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>

          <div class="panel">
            <div class="panel-head">
              <h4>去重结果（快照 {{ results?.snapshot_id ?? '尚未产生' }}）</h4>
              <div class="console-actions">
                <label class="filter-item">
                  <span>每页</span>
                  <select :value="pageState.size" @change="onSizeChange($event)">
                    <option :value="5">5</option>
                    <option :value="10">10</option>
                    <option :value="20">20</option>
                    <option :value="50">50</option>
                  </select>
                </label>
              </div>
            </div>
            <p class="console-desc">
              当前页固定引用快照 {{ results?.snapshot_id ?? '—' }}；翻页不会因为新增记录而改变已看结果，
              同一组重复记录只计一次。
            </p>
            <table v-if="results?.items?.length" class="data-table compact">
              <thead>
                <tr>
                  <th>分段</th><th>区间</th><th>业务键</th><th>记录编号</th><th>重复标题</th>
                  <th>保留记录</th><th>保留标题</th><th>相似度</th><th>处理类型</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="row in results.items" :key="String(row.记录编号)">
                  <td>{{ row.分段 }}</td>
                  <td>{{ row.区间 }}</td>
                  <td>{{ row.业务键 }}</td>
                  <td>{{ row.记录编号 }}</td>
                  <td>{{ row.重复标题 }}</td>
                  <td>{{ row.保留记录 }}</td>
                  <td>{{ row.保留标题 }}</td>
                  <td>{{ row.相似度 }}</td>
                  <td>{{ row.处理类型 }}</td>
                </tr>
              </tbody>
            </table>
            <p v-else class="empty-state">当前快照还没有去重结果；推进批次后会按已收口区间生成。</p>
            <div class="pager">
              <button class="btn ghost" type="button" :disabled="pageState.page <= 1" @click="changePage(-1)">上一页</button>
              <span>
                第 {{ pageState.page }} 页 · 共 {{ results?.total ?? 0 }} 条
                <template v-if="selectedBatch.status !== '已收口'"> · 结果仍随批次推进追加</template>
              </span>
              <button
                class="btn ghost"
                type="button"
                :disabled="!results || pageState.page * pageState.size >= (results?.total ?? 0)"
                @click="changePage(1)"
              >
                下一页
              </button>
            </div>
            <p v-if="selectedBatch.status !== '已收口'" class="notice">
              批次尚未收口：当前快照只包含已收口区间，继续推进后快照会更新；翻页始终按同一快照计数。
            </p>
          </div>

          <div class="panel">
            <div class="panel-head">
              <h4>复核清单</h4>
              <span class="console-desc">
                待确认 {{ pendingReviews }} 项 · 批次{{ selectedBatch.status === '已收口' ? '已收口' : '尚未收口' }}
                · 同步自快照 {{ results?.snapshot_id ?? selectedBatch.snapshot_id ?? '—' }}
              </span>
            </div>
            <table v-if="reviews.length" class="data-table compact">
              <thead>
                <tr>
                  <th>复核编号</th><th>分段</th><th>业务键</th><th>记录编号</th><th>标题</th>
                  <th>保留记录</th><th>相似度</th><th>复核状态</th><th>批次收口</th><th>动作</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="review in reviews" :key="String(review.复核编号)">
                  <td>{{ review.复核编号 }}</td>
                  <td>{{ review.分段 }}</td>
                  <td>{{ review.业务键 }}</td>
                  <td>{{ review.记录编号 }}</td>
                  <td>{{ review.标题 }}</td>
                  <td>{{ review.保留记录 }}</td>
                  <td>{{ review.相似度 }}</td>
                  <td>{{ review.复核状态 }}</td>
                  <td>{{ review.批次收口状态 }}</td>
                  <td class="row-actions">
                    <button class="link" type="button" :disabled="busy" @click="reviewAction(review, '确认重复')">确认重复</button>
                    <button class="link" type="button" :disabled="busy" @click="reviewAction(review, '确认保留')">确认保留</button>
                  </td>
                </tr>
              </tbody>
            </table>
            <p v-else class="empty-state">该批次还没有需要人工确认的记录。</p>
          </div>
        </div>
      </div>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'
import { useDedupConsole } from '@/stores/dedupBatches'
import type { ReviewRow } from '@/stores/dedupBatches'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/dedup'
const columns = ["策略编号", "业务键列", "排序字段", "保留策略", "时间窗口", "相似阈值", "比对字段", "是否启用"]
const actions = ["提交校验", "启用策略", "停用策略"]
const statuses = ["草稿", "待校验", "已启用", "已停用"]
const stats = computed(() => [
  { label: '去重策略总数', value: total.value },
  { label: '已启用策略', value: rows.value.filter((row) => row.status === '已启用').length },
  { label: '批次总数', value: batches.value.length },
  { label: '未收口批次', value: batches.value.filter((item) => item.status !== '已收口').length },
])

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const busy = ref(false)

const {
  batches,
  batch: selectedBatch,
  results,
  reviews,
  loading,
  message: consoleMessage,
  error: consoleError,
  state: pageState,
  activeBatchId,
  loadBatches,
  selectBatch: selectBatchState,
  loadResults,
  loadReviews,
  goToPage,
  changeSize,
  startBatch,
  advanceBatch,
  interruptBatch,
  actReview,
  resetConsole,
} = useDedupConsole()

const policyFilter = ref(pageState.value.policy)
const advanceBudget = ref<number | null>(null)
const selectedBatchId = computed(() => activeBatchId.value)
const pendingReviews = computed(
  () => reviews.value.filter((item) => item.复核状态 === '待确认').length,
)

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '去重策略登记入口尚未接入审批流'
}

function statusClass(status: string) {
  if (status === '已收口') return 'tag tag-ok'
  if (status === '已中断') return 'tag tag-warn'
  if (status === '等待中') return 'tag tag-wait'
  return 'tag'
}

function segmentClass(status: string) {
  if (status === '已收口') return 'tag tag-ok'
  if (status === '处理中') return 'tag tag-run'
  if (status === '待重算') return 'tag tag-warn'
  return 'tag'
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('采集去重比对动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集去重比对操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('去重策略列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集去重比对列表读取失败'
  }
}

async function createBatch(row: Row) {
  busy.value = true
  errorMessage.value = ''
  try {
    const created = await startBatch(Number(row.id))
    policyFilter.value = created.策略编号
    await selectBatchState(created.batch_id)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '发起增量批次失败'
  } finally {
    busy.value = false
  }
}

async function selectBatch(batchId: string) {
  busy.value = true
  try {
    await selectBatchState(batchId)
    policyFilter.value = pageState.value.policy
  } finally {
    busy.value = false
  }
}

async function refreshBatches() {
  busy.value = true
  try {
    await loadBatches(policyFilter.value.trim())
    const target = pageState.value.batchId
    if (target) await selectBatchState(target)
  } finally {
    busy.value = false
  }
}

async function refreshSelected() {
  if (!selectedBatchId.value) return
  busy.value = true
  try {
    await selectBatchState(selectedBatchId.value)
  } finally {
    busy.value = false
  }
}

async function advance() {
  if (!selectedBatchId.value) return
  busy.value = true
  try {
    await advanceBatch(
      selectedBatchId.value,
      advanceBudget.value ? Number(advanceBudget.value) : undefined,
    )
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批次推进失败'
  } finally {
    busy.value = false
  }
}

async function interrupt() {
  if (!selectedBatchId.value) return
  busy.value = true
  try {
    await interruptBatch(selectedBatchId.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批次中断失败'
  } finally {
    busy.value = false
  }
}

async function reviewAction(review: ReviewRow, action: string) {
  busy.value = true
  try {
    await actReview(String(review.复核编号), action)
    await loadResults()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '复核处理失败'
  } finally {
    busy.value = false
  }
}

async function changePage(delta: number) {
  await goToPage(pageState.value.page + delta)
}

async function onSizeChange(event: Event) {
  const value = Number((event.target as HTMLSelectElement).value)
  await changeSize(value)
}

function resetView() {
  resetConsole()
  policyFilter.value = ''
  errorMessage.value = ''
}

onMounted(async () => {
  await loadBatches()
  if (pageState.value.batchId) {
    await selectBatchState(pageState.value.batchId)
  } else if (batches.value.length) {
    await selectBatchState(batches.value[0].batch_id)
  }
  await loadReviews().catch(() => undefined)
})
</script>

<style scoped>
.console {
  margin-top: 28px;
  border-top: 1px solid var(--border);
  padding-top: 16px;
}
.console-head {
  display: flex;
  justify-content: space-between;
  gap: 16px;
  align-items: flex-start;
}
.console-head h3 { margin: 0 0 4px; }
.console-desc { color: var(--muted); font-size: 12px; margin: 4px 0 12px; }
.console-actions { display: flex; flex-wrap: wrap; gap: 8px; align-items: flex-end; }
.console-message { color: #0f7b46; font-size: 13px; margin: 6px 0; }
.console-empty { border: 1px dashed var(--border); padding: 24px; background: #fff; }
.console-grid { display: flex; gap: 16px; align-items: flex-start; }
.batch-list { width: 260px; display: flex; flex-direction: column; gap: 8px; }
.batch-item {
  text-align: left;
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 10px;
  cursor: pointer;
  display: flex;
  flex-direction: column;
  gap: 2px;
  font-size: 12px;
}
.batch-item.active { border-color: var(--brand); box-shadow: 0 0 0 1px var(--brand) inset; }
.batch-item strong { font-size: 13px; }
.batch-detail { flex: 1; min-width: 0; display: flex; flex-direction: column; gap: 16px; }
.batch-summary { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px; }
.batch-summary-head { display: flex; gap: 8px; align-items: center; margin-bottom: 8px; }
.batch-summary-head h4 { margin: 0; }
.kv-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 8px; margin: 0 0 10px; }
.kv-grid dt { color: var(--muted); font-size: 12px; }
.kv-grid dd { margin: 2px 0 0; font-size: 13px; }
.watermark { display: flex; flex-wrap: wrap; gap: 12px; font-size: 12px; color: var(--muted); margin-bottom: 10px; }
.panel { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px; }
.panel-head { display: flex; justify-content: space-between; gap: 12px; align-items: center; }
.panel h4 { margin: 0 0 8px; }
.data-table.compact th, .data-table.compact td { padding: 6px 8px; font-size: 12px; }
.pager { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-top: 8px; font-size: 12px; color: var(--muted); }
.tag { display: inline-block; padding: 0 6px; border-radius: 4px; background: #eef2f7; color: #334155; font-style: normal; font-size: 11px; }
.tag-ok { background: #e7f6ed; color: #0f7b46; }
.tag-warn { background: #fef3e2; color: #b45309; }
.tag-run { background: #e8f0fe; color: #1f6feb; }
.tag-wait { background: #f1edfb; color: #6d28d9; }
.mono { font-family: Menlo, Consolas, monospace; }
.notice { background: #f8fafc; border-left: 3px solid var(--brand); padding: 6px 10px; font-size: 12px; color: #334155; margin: 8px 0 0; }
@media (max-width: 960px) {
  .console-grid { flex-direction: column; }
  .batch-list { width: 100%; }
}
</style>
