import { listRows, saveRows } from '../data/local-store'
import { MODULE_BY_KEY } from '../data/modules'
import {
  ORCHESTRATION_VERSION,
  loadOrchestrationState,
  saveOrchestrationState,
} from '../data/orchestration-store'
import type {
  BatchInstance,
  DispatchQueueEntry,
  ExecutionRecord,
  OrchestrationBundle,
  OrchestrationState,
  PlanDag,
  PlanRunConfig,
  RetryQueueEntry,
} from '../data/orchestration-types'
import type { ActionResult, EntryRow } from '../data/types'

// 编排调度引擎：依赖图分波 → 并发名额准入 → 超时判失败 → 失败策略 → 断点续跑，
// 结论同步到调度执行队列、待重试任务队列与批量转换任务，不只改计划标记。

const SCHEDULE_KEY = 'schedule'
const BATCH_KEY = 'batch'
const SCHEDULE_FIELDS = MODULE_BY_KEY.get(SCHEDULE_KEY)?.fields ?? []
/** 运行中与已中断都算活动实例：重复启动同一计划不得出现两个活动实例。 */
const ACTIVE_BATCH_STATUSES = ['运行中', '已中断']
/** 通知策略里带这个标记时，第一个名额批次落盘后抛出写中断，用于演练断点保留。 */
const INTERRUPT_TOKEN = '模拟写中断'

const TASK_SPLIT = /[,，、;；|\s]+/
const EDGE_SPLIT = /[;；\n]+/

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T
}

function now(): string {
  return new Date().toISOString()
}

function pad(seq: number): string {
  return String(seq).padStart(4, '0')
}

// ---------------------------------------------------------------------------
// 依赖图解析与分波
// ---------------------------------------------------------------------------

/** 解析计划的依赖图定义并分波：没有前置关系的作业编入同一执行波次。 */
export function parsePlanDag(plan: EntryRow): { ok: boolean; dag: PlanDag; errors: string[] } {
  const errors: string[] = []
  const jobs: string[] = []
  for (const token of String(plan['任务列表'] ?? '').split(TASK_SPLIT)) {
    const name = token.trim()
    if (name && !jobs.includes(name)) {
      jobs.push(name)
    }
  }
  const dag: PlanDag = { jobs, deps: {}, waves: [] }
  if (jobs.length === 0) {
    errors.push('任务列表为空，无法编排')
    return { ok: false, dag, errors }
  }
  for (const job of jobs) {
    dag.deps[job] = []
  }
  // 依赖图定义支持两种写法：「下游:上游1,上游2」分段，或「上游->下游」链式；其余片段按历史口径忽略。
  const edges: [string, string][] = []
  for (const segment of String(plan['依赖图定义'] ?? '').split(EDGE_SPLIT)) {
    const seg = segment.trim()
    if (!seg) {
      continue
    }
    if (seg.includes('->')) {
      const chain = seg.split('->').map((item) => item.trim()).filter(Boolean)
      for (let i = 0; i + 1 < chain.length; i += 1) {
        edges.push([chain[i + 1], chain[i]])
      }
    } else {
      const sepIndex = seg.search(/[:：]/)
      if (sepIndex > 0) {
        const down = seg.slice(0, sepIndex).trim()
        const ups = seg.slice(sepIndex + 1).split(/[,，、]+/).map((item) => item.trim()).filter(Boolean)
        for (const up of ups) {
          edges.push([down, up])
        }
      }
    }
  }
  for (const [down, up] of edges) {
    if (!jobs.includes(down)) {
      errors.push(`依赖图引用了任务列表之外的作业 ${down}`)
    }
    if (!jobs.includes(up)) {
      errors.push(`依赖图引用了任务列表之外的作业 ${up}`)
    }
    if (down === up) {
      errors.push(`作业 ${down} 不能依赖自身`)
    }
    if (jobs.includes(down) && jobs.includes(up) && down !== up && !dag.deps[down].includes(up)) {
      dag.deps[down].push(up)
    }
  }
  if (errors.length > 0) {
    return { ok: false, dag, errors }
  }
  // Kahn 分层：同一层互不依赖的作业编入同一波次，层内保持任务列表顺序。
  const placed = new Set<string>()
  let remaining = [...jobs]
  let waveNo = 0
  while (remaining.length > 0) {
    const layer = remaining.filter((job) => dag.deps[job].every((up) => placed.has(up)))
    if (layer.length === 0) {
      errors.push('依赖图存在循环依赖，无法分波')
      return { ok: false, dag, errors }
    }
    waveNo += 1
    dag.waves.push({ waveNo, jobs: layer })
    for (const job of layer) {
      placed.add(job)
    }
    remaining = remaining.filter((job) => !placed.has(job))
  }
  return { ok: true, dag, errors: [] }
}

function parsePositiveInt(raw: unknown, fallback: number): number {
  const value = Number.parseInt(String(raw ?? ''), 10)
  return Number.isFinite(value) && value > 0 ? value : fallback
}

/** 运行参数：并发上限、超时秒数、失败处理；历史数据缺省时沿用历史口径（串行、300s、停止后续）。 */
export function planRunConfig(plan: EntryRow): PlanRunConfig {
  return {
    concurrency: parsePositiveInt(plan['并发上限'], 1),
    timeoutSec: parsePositiveInt(plan['超时秒数'], 300),
    policy: String(plan['失败处理'] ?? '').includes('继续') ? '继续后续节点' : '停止后续节点',
  }
}

/** 确定性模拟耗时：同一作业同一次尝试结果稳定，换尝试次数会变化，便于演示超时与重试。 */
export function simulatedDurationSec(jobCode: string, attempt: number): number {
  let hash = 0
  for (let i = 0; i < jobCode.length; i += 1) {
    hash = (hash + jobCode.charCodeAt(i) * (i + 7)) % 100000
  }
  return 30 + ((hash + attempt * 97) % 150)
}

export function previewPlan(planId: number):
  | { ok: true; dag: PlanDag; config: PlanRunConfig }
  | { ok: false; errors: string[]; config: PlanRunConfig } {
  const plan = listRows(SCHEDULE_KEY).find((row) => Number(row.id) === planId)
  if (!plan) {
    return { ok: false, errors: [`没有找到编号为 ${planId} 的编排计划`], config: { concurrency: 1, timeoutSec: 300, policy: '停止后续节点' } }
  }
  const config = planRunConfig(plan)
  const parsed = parsePlanDag(plan)
  if (!parsed.ok) {
    return { ok: false, errors: parsed.errors, config }
  }
  return { ok: true, dag: parsed.dag, config }
}

// ---------------------------------------------------------------------------
// 跨模块写回
// ---------------------------------------------------------------------------

function setPlanStatus(planId: number, status: string, abnormal: boolean): void {
  const rows = listRows(SCHEDULE_KEY)
  const index = rows.findIndex((row) => Number(row.id) === planId)
  if (index < 0) {
    return
  }
  const next = [...rows]
  next[index] = { ...rows[index], status, pending: status !== '已停止', abnormal }
  saveRows(SCHEDULE_KEY, next)
}

/** 批次结果写回批量转换任务：作业成功→已完成，失败→已失败。 */
function writeBackBatchTasks(batch: BatchInstance): void {
  const rows = listRows(BATCH_KEY)
  let changed = false
  const next = rows.map((row) => {
    const code = String(row['任务编号'] ?? '')
    if (batch.completedJobs.includes(code)) {
      changed = true
      return { ...row, status: '已完成', pending: false, abnormal: false }
    }
    if (batch.failedJobs.includes(code)) {
      changed = true
      return { ...row, status: '已失败', pending: true, abnormal: true }
    }
    return row
  })
  if (changed) {
    saveRows(BATCH_KEY, next)
  }
}

function updateDispatch(
  state: OrchestrationState,
  batch: BatchInstance,
  jobCode: string,
  status: DispatchQueueEntry['status'],
  note: string,
): void {
  const entry = state.dispatchQueue.find((item) => item.batchNo === batch.batchNo && item.jobCode === jobCode)
  if (entry) {
    entry.status = status
    entry.note = note
    entry.updatedAt = now()
  }
}

function upsertRetryEntry(state: OrchestrationState, batch: BatchInstance, jobCode: string, reason: string): void {
  const stamp = now()
  const existing = state.retryQueue.find((item) => item.batchNo === batch.batchNo && item.jobCode === jobCode)
  if (existing) {
    existing.status = '待重试'
    existing.reason = reason
    existing.checkpointJob = batch.checkpointJob
    existing.updatedAt = stamp
    return
  }
  state.retrySeq += 1
  state.retryQueue.push({
    id: state.retrySeq,
    retryNo: `RETRY-${pad(state.retrySeq)}`,
    planId: batch.planId,
    planCode: batch.planCode,
    batchNo: batch.batchNo,
    jobCode,
    reason,
    checkpointJob: batch.checkpointJob,
    status: '待重试',
    createdAt: stamp,
    updatedAt: stamp,
  })
}

/** 已被其他活动批次占用的作业：同一作业不能跨两个批次。重试时排除本批次自己的排队记录。 */
function findJobConflicts(
  state: OrchestrationState,
  jobs: string[],
  excludeBatchNo = '',
): { job: string; batchNo: string }[] {
  const activeBatchNos = new Set(
    state.batches
      .filter((batch) => ACTIVE_BATCH_STATUSES.includes(batch.status) && batch.batchNo !== excludeBatchNo)
      .map((batch) => batch.batchNo),
  )
  const occupied = new Map<string, string>()
  for (const entry of state.dispatchQueue) {
    if (activeBatchNos.has(entry.batchNo) && (entry.status === '待准入' || entry.status === '已准入')) {
      occupied.set(entry.jobCode, entry.batchNo)
    }
  }
  return jobs
    .filter((job) => occupied.has(job))
    .map((job) => ({ job, batchNo: occupied.get(job) ?? '' }))
}

function maybeThrowInterruption(plan: EntryRow, batch: BatchInstance): void {
  if (batch.attempt === 1 && String(plan['通知策略'] ?? '').includes(INTERRUPT_TOKEN)) {
    throw new Error('模拟写中断：名额批次落盘后持久化失败')
  }
}

// ---------------------------------------------------------------------------
// 执行核心：分波准入，只有拿到并发名额且上游已完成的作业才产生执行记录
// ---------------------------------------------------------------------------

function executeWaves(
  state: OrchestrationState,
  plan: EntryRow,
  batch: BatchInstance,
  dag: PlanDag,
  config: PlanRunConfig,
): void {
  const completed = new Set(batch.completedJobs)
  const failed = batch.failedJobs
  for (const wave of dag.waves) {
    batch.currentWave = wave.waveNo
    // 准入条件：依赖结果已完成；未满足的作业留到扫尾统一标记跳过
    const admitted = wave.jobs.filter(
      (job) => !completed.has(job) && dag.deps[job].every((up) => completed.has(up)),
    )
    for (let i = 0; i < admitted.length; i += config.concurrency) {
      const burst = admitted.slice(i, i + config.concurrency)
      burst.forEach((job, index) => {
        updateDispatch(state, batch, job, '已准入', `第${wave.waveNo}波 名额${index + 1}，已下发批量转换`)
      })
      burst.forEach((job, index) => {
        const duration = simulatedDurationSec(job, batch.attempt)
        const success = duration <= config.timeoutSec
        const stamp = now()
        state.recordSeq += 1
        state.records.push({
          id: state.recordSeq,
          batchNo: batch.batchNo,
          planId: batch.planId,
          planCode: batch.planCode,
          jobCode: job,
          waveNo: wave.waveNo,
          slotNo: index + 1,
          attempt: batch.attempt,
          status: success ? '已完成' : '已失败',
          startedAt: stamp,
          finishedAt: stamp,
          durationSec: duration,
          timeoutSec: config.timeoutSec,
          conclusion: success ? '执行成功' : `超过超时秒数 ${config.timeoutSec}s，标记失败`,
        })
        if (success) {
          completed.add(job)
          batch.completedJobs.push(job)
          batch.checkpointJob = job
          updateDispatch(state, batch, job, '已完成', `第${wave.waveNo}波完成，耗时${duration}s`)
        } else {
          failed.push(job)
          updateDispatch(state, batch, job, '已失败', `超过超时秒数 ${config.timeoutSec}s`)
        }
      })
      // 每个名额批次落盘一次：写入中断时已保存的断点不会丢
      saveOrchestrationState(state)
      maybeThrowInterruption(plan, batch)
      if (failed.length > 0 && config.policy === '停止后续节点') {
        break
      }
    }
    if (failed.length > 0 && config.policy === '停止后续节点') {
      break
    }
  }
  // 扫尾：仍未准入的作业（未拿到名额或上游未完成）统一标记跳过，不产生执行记录
  for (const entry of state.dispatchQueue) {
    if (entry.batchNo === batch.batchNo && entry.status === '待准入') {
      entry.status = '已跳过'
      entry.updatedAt = now()
      entry.note = '未获得并发名额或上游未完成，未准入'
      if (!batch.skippedJobs.includes(entry.jobCode)) {
        batch.skippedJobs.push(entry.jobCode)
      }
    }
  }
  batch.status = failed.length > 0 ? '已失败' : '已完成'
  batch.finishedAt = now()
  saveOrchestrationState(state)
}

/** 批次结论落库：待重试队列跟着结论变化，结果写回批量转换任务，计划状态反映结论。 */
function concludeBatch(
  state: OrchestrationState,
  batch: BatchInstance,
  config: PlanRunConfig,
): void {
  for (const job of batch.failedJobs) {
    const record = [...state.records].reverse().find((item) => item.batchNo === batch.batchNo && item.jobCode === job)
    upsertRetryEntry(state, batch, job, record?.conclusion ?? `超过超时秒数 ${config.timeoutSec}s，标记失败`)
  }
  for (const entry of state.retryQueue) {
    if (entry.planId === batch.planId && entry.status === '待重试' && batch.completedJobs.includes(entry.jobCode)) {
      entry.status = '已重试'
      entry.updatedAt = now()
    }
  }
  writeBackBatchTasks(batch)
  if (batch.status === '已完成') {
    setPlanStatus(batch.planId, '待校验', false)
  } else {
    setPlanStatus(batch.planId, '已停止', true)
  }
  saveOrchestrationState(state)
}

function runBatch(
  state: OrchestrationState,
  plan: EntryRow,
  batch: BatchInstance,
  dag: PlanDag,
  config: PlanRunConfig,
): ActionResult {
  try {
    executeWaves(state, plan, batch, dag, config)
    concludeBatch(state, batch, config)
  } catch (error) {
    // 事务失败：断点保留，计划回到待校验
    batch.status = '已中断'
    batch.finishedAt = now()
    batch.note = `写入中断，断点保留在 ${batch.checkpointJob || '起点'}`
    try {
      saveOrchestrationState(state)
      setPlanStatus(batch.planId, '待校验', true)
    } catch {
      // 存储不可用时至少保留内存态，界面仍能展示断点
    }
    const reason = error instanceof Error ? error.message : '未知写入异常'
    return {
      ok: false,
      message: `批次写入中断（${reason}）：已保留断点，最后完成作业 ${batch.checkpointJob || '无'}，计划回到待校验，可在批次列表重试续跑`,
    }
  }
  if (batch.status === '已完成') {
    return {
      ok: true,
      message: `批次 ${batch.batchNo} 已完成：${batch.completedJobs.length} 个作业全部成功，结果已写回批量转换任务与调度执行队列`,
    }
  }
  return {
    ok: false,
    message: `批次 ${batch.batchNo} 已失败：成功 ${batch.completedJobs.length}、失败 ${batch.failedJobs.length}、跳过 ${batch.skippedJobs.length}，失败作业已进入待重试任务队列`,
  }
}

// ---------------------------------------------------------------------------
// 对外动作
// ---------------------------------------------------------------------------

export function startOrchestrationPlan(planId: number): ActionResult {
  const plan = listRows(SCHEDULE_KEY).find((row) => Number(row.id) === planId)
  if (!plan) {
    return { ok: false, message: `没有找到编号为 ${planId} 的编排计划` }
  }
  const planCode = String(plan['计划编号'] ?? `PLAN-${planId}`)
  const state = clone(loadOrchestrationState())
  const active = state.batches.find((batch) => batch.planId === planId && ACTIVE_BATCH_STATUSES.includes(batch.status))
  if (active) {
    return {
      ok: false,
      message: `计划 ${planCode} 已存在活动批次 ${active.batchNo}（${active.status}），同一计划不得出现两个活动实例`,
    }
  }
  const parsed = parsePlanDag(plan)
  if (!parsed.ok) {
    setPlanStatus(planId, '待校验', true)
    return { ok: false, message: `依赖图校验失败：${parsed.errors.join('；')}。事务已回滚，计划回到待校验` }
  }
  const conflicts = findJobConflicts(state, parsed.dag.jobs)
  if (conflicts.length > 0) {
    const detail = conflicts.map((item) => `${item.job}（${item.batchNo}）`).join('、')
    return { ok: false, message: `作业 ${detail} 正在其他活动批次中执行，同一作业不能跨两个批次` }
  }
  const config = planRunConfig(plan)
  state.batchSeq += 1
  const batchNo = `${planCode}-B${state.batchSeq}`
  const batch: BatchInstance = {
    id: state.batchSeq,
    batchNo,
    planId,
    planCode,
    status: '运行中',
    attempt: 1,
    currentWave: 1,
    totalWaves: parsed.dag.waves.length,
    checkpointJob: '',
    completedJobs: [],
    failedJobs: [],
    skippedJobs: [],
    startedAt: now(),
    finishedAt: '',
    note: `分 ${parsed.dag.waves.length} 波执行，单波并发名额 ${config.concurrency}，节点超时 ${config.timeoutSec}s`,
  }
  state.batches.push(batch)
  for (const wave of parsed.dag.waves) {
    for (const job of wave.jobs) {
      state.queueSeq += 1
      state.dispatchQueue.push({
        id: state.queueSeq,
        queueNo: `QUE-${pad(state.queueSeq)}`,
        planId,
        planCode,
        batchNo,
        jobCode: job,
        waveNo: wave.waveNo,
        targetModule: BATCH_KEY,
        targetKey: job,
        status: '待准入',
        updatedAt: now(),
        note: `第${wave.waveNo}波等待并发名额`,
      })
    }
  }
  setPlanStatus(planId, '运行中', false)
  saveOrchestrationState(state)
  return runBatch(state, plan, batch, parsed.dag, config)
}

export function stopOrchestrationPlan(planId: number): ActionResult {
  const plan = listRows(SCHEDULE_KEY).find((row) => Number(row.id) === planId)
  if (!plan) {
    return { ok: false, message: `没有找到编号为 ${planId} 的编排计划` }
  }
  const state = clone(loadOrchestrationState())
  const active = state.batches.find((batch) => batch.planId === planId && ACTIVE_BATCH_STATUSES.includes(batch.status))
  if (!active) {
    // 历史口径：老数据没有批次实例，只翻计划状态
    if (String(plan.status) !== '运行中') {
      return { ok: false, message: `计划 ${String(plan['计划编号'] ?? planId)} 当前没有活动批次可停止` }
    }
    setPlanStatus(planId, '已停止', false)
    saveOrchestrationState(state)
    return { ok: true, message: '编排计划已停止（历史数据无批次实例，按计划状态处理）' }
  }
  active.status = '已停止'
  active.finishedAt = now()
  active.note = '手动停止，未准入的作业不再下发'
  for (const entry of state.dispatchQueue) {
    if (entry.batchNo === active.batchNo && (entry.status === '待准入' || entry.status === '已准入')) {
      entry.status = '已跳过'
      entry.updatedAt = now()
      entry.note = '批次手动停止'
    }
  }
  for (const record of state.records) {
    if (record.batchNo === active.batchNo && record.status === '运行中') {
      record.status = '已失败'
      record.finishedAt = now()
      record.conclusion = '批次手动停止，执行中断'
      if (!active.failedJobs.includes(record.jobCode)) {
        active.failedJobs.push(record.jobCode)
      }
    }
  }
  // 停止也要把已产生的批次结论写回批量转换任务，保持跨模块一致
  writeBackBatchTasks(active)
  setPlanStatus(planId, '已停止', false)
  saveOrchestrationState(state)
  return { ok: true, message: `批次 ${active.batchNo} 已停止，断点保留在 ${active.checkpointJob || '起点'}` }
}

/** 重试：接着最后完成的作业续跑，已完成的作业不重复执行。 */
export function retryBatch(batchId: number): ActionResult {
  const state = clone(loadOrchestrationState())
  const batch = state.batches.find((item) => item.id === batchId)
  if (!batch) {
    return { ok: false, message: `没有找到编号为 ${batchId} 的批次` }
  }
  if (batch.status !== '已失败' && batch.status !== '已中断') {
    return { ok: false, message: `批次 ${batch.batchNo} 当前状态「${batch.status}」，仅已失败或已中断的批次可以重试` }
  }
  const anotherActive = state.batches.find(
    (item) => item.planId === batch.planId && item.id !== batch.id && ACTIVE_BATCH_STATUSES.includes(item.status),
  )
  if (anotherActive) {
    return { ok: false, message: `计划 ${batch.planCode} 已存在活动批次 ${anotherActive.batchNo}，不得出现两个活动实例` }
  }
  const plan = listRows(SCHEDULE_KEY).find((row) => Number(row.id) === batch.planId)
  if (!plan) {
    return { ok: false, message: `批次 ${batch.batchNo} 对应的编排计划已不存在` }
  }
  const parsed = parsePlanDag(plan)
  if (!parsed.ok) {
    setPlanStatus(batch.planId, '待校验', true)
    return { ok: false, message: `依赖图校验失败：${parsed.errors.join('；')}。事务已回滚，计划回到待校验` }
  }
  const remainingJobs = parsed.dag.jobs.filter((job) => !batch.completedJobs.includes(job))
  const conflicts = findJobConflicts(state, remainingJobs, batch.batchNo)
  if (conflicts.length > 0) {
    const detail = conflicts.map((item) => `${item.job}（${item.batchNo}）`).join('、')
    return { ok: false, message: `作业 ${detail} 正在其他活动批次中执行，同一作业不能跨两个批次` }
  }
  const config = planRunConfig(plan)
  batch.attempt += 1
  batch.status = '运行中'
  batch.failedJobs = []
  batch.skippedJobs = []
  batch.finishedAt = ''
  batch.totalWaves = parsed.dag.waves.length
  batch.note = `第 ${batch.attempt} 次尝试，从断点 ${batch.checkpointJob || '起点'} 续跑`
  // 未完成的作业重新排队；计划定义里新增的作业补进队列
  for (const entry of state.dispatchQueue) {
    if (entry.batchNo === batch.batchNo && !batch.completedJobs.includes(entry.jobCode)) {
      entry.status = '待准入'
      entry.updatedAt = now()
      entry.note = `第 ${batch.attempt} 次尝试重新排队`
    }
  }
  for (const wave of parsed.dag.waves) {
    for (const job of wave.jobs) {
      if (!state.dispatchQueue.some((entry) => entry.batchNo === batch.batchNo && entry.jobCode === job)) {
        state.queueSeq += 1
        state.dispatchQueue.push({
          id: state.queueSeq,
          queueNo: `QUE-${pad(state.queueSeq)}`,
          planId: batch.planId,
          planCode: batch.planCode,
          batchNo: batch.batchNo,
          jobCode: job,
          waveNo: wave.waveNo,
          targetModule: BATCH_KEY,
          targetKey: job,
          status: '待准入',
          updatedAt: now(),
          note: `第${wave.waveNo}波等待并发名额`,
        })
      }
    }
  }
  setPlanStatus(batch.planId, '运行中', false)
  saveOrchestrationState(state)
  return runBatch(state, plan, batch, parsed.dag, config)
}

export function retryFromQueue(retryId: number): ActionResult {
  const state = loadOrchestrationState()
  const entry = state.retryQueue.find((item) => item.id === retryId)
  if (!entry) {
    return { ok: false, message: `没有找到编号为 ${retryId} 的待重试记录` }
  }
  const batch = state.batches.find((item) => item.batchNo === entry.batchNo)
  if (!batch) {
    return { ok: false, message: `待重试记录 ${entry.retryNo} 对应的批次已不存在` }
  }
  return retryBatch(batch.id)
}

// ---------------------------------------------------------------------------
// 导出 / 导入：结果打包成文件，导入时校验内容完整
// ---------------------------------------------------------------------------

function canonicalCore(bundle: Partial<OrchestrationBundle>): string {
  return JSON.stringify({
    plan: bundle.plan,
    dag: bundle.dag ?? null,
    config: bundle.config,
    batches: bundle.batches,
    records: bundle.records,
    dispatchQueue: bundle.dispatchQueue,
    retryQueue: bundle.retryQueue,
    batchTasks: bundle.batchTasks,
  })
}

function bundleChecksum(payload: string): string {
  let hash = 0x811c9dc5
  for (let i = 0; i < payload.length; i += 1) {
    hash ^= payload.charCodeAt(i)
    hash = Math.imul(hash, 0x01000193) >>> 0
  }
  return `${payload.length}:${hash.toString(16)}`
}

export function buildPlanBundle(planId: number): { ok: boolean; message: string; bundle?: OrchestrationBundle } {
  const plan = listRows(SCHEDULE_KEY).find((row) => Number(row.id) === planId)
  if (!plan) {
    return { ok: false, message: `没有找到编号为 ${planId} 的编排计划` }
  }
  const planCode = String(plan['计划编号'] ?? `PLAN-${planId}`)
  const state = loadOrchestrationState()
  const batches = state.batches.filter((batch) => batch.planId === planId)
  const batchNos = new Set(batches.map((batch) => batch.batchNo))
  const records = state.records.filter((record) => batchNos.has(record.batchNo))
  const dispatchQueue = state.dispatchQueue.filter((entry) => batchNos.has(entry.batchNo))
  const retryQueue = state.retryQueue.filter((entry) => entry.planId === planId)
  const jobCodes = new Set(dispatchQueue.map((entry) => entry.jobCode))
  const batchTasks = listRows(BATCH_KEY)
    .filter((row) => jobCodes.has(String(row['任务编号'] ?? '')))
    .map((row) => ({ 任务编号: String(row['任务编号']), status: String(row.status) }))
  const fields: Record<string, string> = {}
  for (const field of SCHEDULE_FIELDS) {
    fields[field] = String(plan[field] ?? '')
  }
  const parsed = parsePlanDag(plan)
  const core = {
    plan: { id: planId, planCode, status: String(plan.status), fields },
    dag: parsed.ok ? parsed.dag : null,
    config: planRunConfig(plan),
    batches,
    records,
    dispatchQueue,
    retryQueue,
    batchTasks,
  }
  const bundle: OrchestrationBundle = {
    kind: 'orchestration-result',
    version: ORCHESTRATION_VERSION,
    exportedAt: now(),
    ...core,
    checksum: bundleChecksum(canonicalCore(core)),
  }
  return { ok: true, message: 'ok', bundle }
}

export function downloadPlanBundle(planId: number): ActionResult {
  const built = buildPlanBundle(planId)
  if (!built.ok || !built.bundle) {
    return { ok: false, message: built.message }
  }
  const filename = `编排结果-${built.bundle.plan.planCode}.json`
  const blob = new Blob([JSON.stringify(built.bundle, null, 2)], { type: 'application/json;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = filename
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
  return { ok: true, message: `已导出 ${filename}，包含批次、执行记录、调度队列与待重试队列` }
}

export function importPlanBundle(content: string): ActionResult {
  let raw: unknown
  try {
    raw = JSON.parse(content)
  } catch {
    return { ok: false, message: '文件不是有效的 JSON，导入取消' }
  }
  const bundle = raw as Partial<OrchestrationBundle>
  if (!bundle || bundle.kind !== 'orchestration-result') {
    return { ok: false, message: '文件不是编排结果包（缺少 kind 标记），导入取消' }
  }
  if (typeof bundle.version !== 'number' || bundle.version > ORCHESTRATION_VERSION) {
    return { ok: false, message: `结果包版本 ${String(bundle.version)} 高于当前支持的 ${ORCHESTRATION_VERSION}，导入取消` }
  }
  for (const key of ['batches', 'records', 'dispatchQueue', 'retryQueue', 'batchTasks'] as const) {
    if (!Array.isArray(bundle[key])) {
      return { ok: false, message: `结果包内容不完整：缺少 ${key}，导入取消` }
    }
  }
  if (!bundle.plan || typeof bundle.plan.planCode !== 'string' || !bundle.config) {
    return { ok: false, message: '结果包内容不完整：缺少计划定义或运行参数，导入取消' }
  }
  if (bundleChecksum(canonicalCore(bundle)) !== bundle.checksum) {
    return { ok: false, message: '结果包校验和不匹配，内容可能被截断或篡改，导入取消' }
  }
  const planCode = bundle.plan.planCode
  // 计划按编号 upsert：已存在则更新定义与状态，不存在则新建
  const plans = listRows(SCHEDULE_KEY)
  const nextPlans = [...plans]
  const existingIndex = plans.findIndex((row) => String(row['计划编号'] ?? '') === planCode)
  let planId: number
  if (existingIndex >= 0) {
    planId = Number(plans[existingIndex].id)
    nextPlans[existingIndex] = {
      ...plans[existingIndex],
      ...bundle.plan.fields,
      status: bundle.plan.status,
      pending: bundle.plan.status !== '已停止',
      abnormal: false,
    }
  } else {
    planId = Math.max(0, ...plans.map((row) => Number(row.id))) + 1
    nextPlans.push({
      id: planId,
      status: bundle.plan.status,
      pending: bundle.plan.status !== '已停止',
      abnormal: false,
      ...bundle.plan.fields,
    })
  }
  // 替换该计划已有的编排数据，再并入导入内容（重新编号避免序号冲突）
  const state = clone(loadOrchestrationState())
  const oldBatchNos = new Set(
    state.batches.filter((batch) => batch.planId === planId || batch.planCode === planCode).map((batch) => batch.batchNo),
  )
  state.batches = state.batches.filter((batch) => !oldBatchNos.has(batch.batchNo))
  state.records = state.records.filter((record) => !oldBatchNos.has(record.batchNo))
  state.dispatchQueue = state.dispatchQueue.filter((entry) => !oldBatchNos.has(entry.batchNo))
  state.retryQueue = state.retryQueue.filter((entry) => entry.planId !== planId && entry.planCode !== planCode)
  for (const batch of bundle.batches ?? []) {
    state.batchSeq += 1
    state.batches.push({ ...batch, id: state.batchSeq, planId, planCode })
  }
  for (const record of bundle.records ?? []) {
    state.recordSeq += 1
    state.records.push({ ...record, id: state.recordSeq, planId, planCode })
  }
  for (const entry of bundle.dispatchQueue ?? []) {
    state.queueSeq += 1
    state.dispatchQueue.push({ ...entry, id: state.queueSeq, planId, planCode, queueNo: `QUE-${pad(state.queueSeq)}` })
  }
  for (const entry of bundle.retryQueue ?? []) {
    state.retrySeq += 1
    state.retryQueue.push({ ...entry, id: state.retrySeq, planId, planCode, retryNo: `RETRY-${pad(state.retrySeq)}` })
  }
  // 批量转换任务状态一并写回，保持跨模块一致
  const batchRows = listRows(BATCH_KEY)
  let batchChanged = false
  const nextBatchRows = batchRows.map((row) => {
    const hit = (bundle.batchTasks ?? []).find((task) => task.任务编号 === String(row['任务编号'] ?? ''))
    if (hit) {
      batchChanged = true
      return { ...row, status: hit.status, pending: hit.status !== '已完成', abnormal: hit.status === '已失败' }
    }
    return row
  })
  try {
    saveRows(SCHEDULE_KEY, nextPlans)
    if (batchChanged) {
      saveRows(BATCH_KEY, nextBatchRows)
    }
    saveOrchestrationState(state)
  } catch (error) {
    const reason = error instanceof Error ? error.message : '未知写入异常'
    return { ok: false, message: `导入写库失败（${reason}），本次导入已放弃` }
  }
  return {
    ok: true,
    message: `已导入计划 ${planCode} 的编排结果：批次 ${bundle.batches?.length ?? 0} 个、执行记录 ${bundle.records?.length ?? 0} 条、调度队列 ${bundle.dispatchQueue?.length ?? 0} 条、待重试 ${bundle.retryQueue?.length ?? 0} 条`,
  }
}

// ---------------------------------------------------------------------------
// 查询
// ---------------------------------------------------------------------------

export function listBatches(): BatchInstance[] {
  return [...loadOrchestrationState().batches].sort((a, b) => b.id - a.id)
}

export function listExecutionRecords(): ExecutionRecord[] {
  return [...loadOrchestrationState().records].sort((a, b) => b.id - a.id)
}

export function listDispatchQueue(): DispatchQueueEntry[] {
  return [...loadOrchestrationState().dispatchQueue].sort((a, b) => b.id - a.id)
}

export function listRetryQueue(): RetryQueueEntry[] {
  return [...loadOrchestrationState().retryQueue].sort((a, b) => b.id - a.id)
}
