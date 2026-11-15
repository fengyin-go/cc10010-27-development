import type { OrchestrationState } from './orchestration-types'

// 编排引擎的本地持久化：与清单数据分开存放，带版本号，旧版本数据迁移到当前结构。
export const ORCHESTRATION_STORAGE_KEY = 'lake-govern-config:orchestration'
export const ORCHESTRATION_VERSION = 2

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T
}

// 示例数据：SCHE-0003 自带一个活动批次，用来演示「同一计划不得出现两个活动实例」。
const SEED_STATE: OrchestrationState = {
  version: ORCHESTRATION_VERSION,
  batchSeq: 1,
  recordSeq: 1,
  queueSeq: 1,
  retrySeq: 0,
  batches: [
    {
      id: 1,
      batchNo: 'SCHE-0003-B1',
      planId: 3,
      planCode: 'SCHE-0003',
      status: '运行中',
      attempt: 1,
      currentWave: 1,
      totalWaves: 1,
      checkpointJob: '',
      completedJobs: [],
      failedJobs: [],
      skippedJobs: [],
      startedAt: '2026-11-01T02:00:00.000Z',
      finishedAt: '',
      note: '示例活动批次：启动同计划会被拒绝，可停止或等待其结论',
    },
  ],
  records: [
    {
      id: 1,
      batchNo: 'SCHE-0003-B1',
      planId: 3,
      planCode: 'SCHE-0003',
      jobCode: 'BATC-0003',
      waveNo: 1,
      slotNo: 1,
      attempt: 1,
      status: '运行中',
      startedAt: '2026-11-01T02:00:00.000Z',
      finishedAt: '',
      durationSec: 0,
      timeoutSec: 300,
      conclusion: '已获得并发名额，执行中',
    },
  ],
  dispatchQueue: [
    {
      id: 1,
      queueNo: 'QUE-0001',
      planId: 3,
      planCode: 'SCHE-0003',
      batchNo: 'SCHE-0003-B1',
      jobCode: 'BATC-0003',
      waveNo: 1,
      targetModule: 'batch',
      targetKey: 'BATC-0003',
      status: '已准入',
      updatedAt: '2026-11-01T02:00:00.000Z',
      note: '第1波 名额1，已下发批量转换',
    },
  ],
  retryQueue: [],
}

/** 历史版本（v1 及以前）只有计划状态、没有批次与队列：补齐空结构，沿用历史口径。 */
export function migrateOrchestrationState(raw: Partial<OrchestrationState> | null | undefined): OrchestrationState {
  const base = clone(SEED_STATE)
  if (!raw || typeof raw !== 'object') {
    return base
  }
  const merged: OrchestrationState = {
    version: ORCHESTRATION_VERSION,
    batchSeq: Number(raw.batchSeq) || base.batchSeq,
    recordSeq: Number(raw.recordSeq) || base.recordSeq,
    queueSeq: Number(raw.queueSeq) || base.queueSeq,
    retrySeq: Number(raw.retrySeq) || 0,
    batches: Array.isArray(raw.batches) ? raw.batches : base.batches,
    records: Array.isArray(raw.records) ? raw.records : base.records,
    dispatchQueue: Array.isArray(raw.dispatchQueue) ? raw.dispatchQueue : base.dispatchQueue,
    retryQueue: Array.isArray(raw.retryQueue) ? raw.retryQueue : [],
  }
  return merged
}

function readStorage(): OrchestrationState {
  if (typeof window === 'undefined' || !window.localStorage) {
    return clone(SEED_STATE)
  }
  const raw = window.localStorage.getItem(ORCHESTRATION_STORAGE_KEY)
  if (!raw) {
    const seeded = clone(SEED_STATE)
    window.localStorage.setItem(ORCHESTRATION_STORAGE_KEY, JSON.stringify(seeded))
    return seeded
  }
  try {
    return migrateOrchestrationState(JSON.parse(raw) as Partial<OrchestrationState>)
  } catch {
    const seeded = clone(SEED_STATE)
    window.localStorage.setItem(ORCHESTRATION_STORAGE_KEY, JSON.stringify(seeded))
    return seeded
  }
}

let cache: OrchestrationState | null = null

export function loadOrchestrationState(): OrchestrationState {
  if (cache === null) {
    cache = readStorage()
  }
  return cache
}

/** 每个名额批次落盘一次：写入中断时已保存的断点不会丢。写失败会抛错，由引擎走事务失败路径。 */
export function saveOrchestrationState(state: OrchestrationState): void {
  cache = state
  if (typeof window !== 'undefined' && window.localStorage) {
    window.localStorage.setItem(ORCHESTRATION_STORAGE_KEY, JSON.stringify(state))
  }
}

export function resetOrchestrationState(): OrchestrationState {
  const seeded = clone(SEED_STATE)
  saveOrchestrationState(seeded)
  return seeded
}
