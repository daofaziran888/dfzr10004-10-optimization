/**
 * 去重批次的浏览器端持久化。
 *
 * 后端把水位、分批进度和最近结果落在磁盘状态文件里；前端还需要记住
 * 「当前在看的批次 / 快照 / 页码 / 筛选」，刷新页面或从复核清单返回时
 * 才能继续沿用同一份快照，而不是重新按默认值取第一页。
 */
const STORAGE_KEY = 'dedup-console-v1'

export type DedupConsoleState = {
  batchId: string | null
  snapshotId: string | null
  page: number
  size: number
  policy: string
  lastBatchStatus: string
  updatedAt: string
}

const EMPTY_STATE: DedupConsoleState = {
  batchId: null,
  snapshotId: null,
  page: 1,
  size: 10,
  policy: '',
  lastBatchStatus: '',
  updatedAt: '',
}

function readState(): DedupConsoleState {
  if (typeof window === 'undefined') return { ...EMPTY_STATE }
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY)
    if (!raw) return { ...EMPTY_STATE }
    const parsed = JSON.parse(raw) as Partial<DedupConsoleState>
    return {
      batchId: parsed.batchId ?? null,
      snapshotId: parsed.snapshotId ?? null,
      page: Number(parsed.page) > 0 ? Number(parsed.page) : 1,
      size: Number(parsed.size) > 0 ? Number(parsed.size) : 10,
      policy: String(parsed.policy ?? ''),
      lastBatchStatus: String(parsed.lastBatchStatus ?? ''),
      updatedAt: String(parsed.updatedAt ?? ''),
    }
  } catch {
    return { ...EMPTY_STATE }
  }
}

export function loadConsoleState(): DedupConsoleState {
  return readState()
}

export function saveConsoleState(patch: Partial<DedupConsoleState>): DedupConsoleState {
  const next = { ...readState(), ...patch, updatedAt: new Date().toISOString() }
  if (typeof window !== 'undefined') {
    try {
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify(next))
    } catch {
      // 隐私模式或配额不足时不影响页面使用，只丢失跨刷新记忆。
    }
  }
  return next
}

export function clearConsoleState(): DedupConsoleState {
  if (typeof window !== 'undefined') {
    try {
      window.localStorage.removeItem(STORAGE_KEY)
    } catch {
      // 同上，忽略存储异常。
    }
  }
  return { ...EMPTY_STATE }
}
