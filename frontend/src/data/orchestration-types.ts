/** 编排调度引擎的持久化结构：批次实例、执行记录、调度执行队列与待重试任务队列。 */

export type BatchStatus = '运行中' | '已完成' | '已失败' | '已中断' | '已停止'
export type RecordStatus = '运行中' | '已完成' | '已失败'
export type DispatchStatus = '待准入' | '已准入' | '已完成' | '已失败' | '已跳过'
export type RetryStatus = '待重试' | '已重试' | '已消解'
export type FailurePolicy = '停止后续节点' | '继续后续节点'

/** 一个执行波次：依赖图里同一层、互不依赖的作业编在一起。 */
export type WaveSpec = {
  waveNo: number
  jobs: string[]
}

/** 依赖图解析结果：作业清单、上游依赖、分波结果。 */
export type PlanDag = {
  jobs: string[]
  deps: Record<string, string[]>
  waves: WaveSpec[]
}

/** 从计划行里解析出来的运行参数。 */
export type PlanRunConfig = {
  concurrency: number
  timeoutSec: number
  policy: FailurePolicy
}

/** 执行记录：只有拿到并发名额且上游已完成的作业才会产生。 */
export type ExecutionRecord = {
  id: number
  batchNo: string
  planId: number
  planCode: string
  jobCode: string
  waveNo: number
  slotNo: number
  attempt: number
  status: RecordStatus
  startedAt: string
  finishedAt: string
  durationSec: number
  timeoutSec: number
  conclusion: string
}

/** 批次实例：一次启动产生一个批次，断点续跑沿用同一批次号。 */
export type BatchInstance = {
  id: number
  batchNo: string
  planId: number
  planCode: string
  status: BatchStatus
  attempt: number
  currentWave: number
  totalWaves: number
  checkpointJob: string
  completedJobs: string[]
  failedJobs: string[]
  skippedJobs: string[]
  startedAt: string
  finishedAt: string
  note: string
}

/** 调度执行队列：跨模块的下发通道，把编排结论同步给批量转换等下游模块。 */
export type DispatchQueueEntry = {
  id: number
  queueNo: string
  planId: number
  planCode: string
  batchNo: string
  jobCode: string
  waveNo: number
  targetModule: string
  targetKey: string
  status: DispatchStatus
  updatedAt: string
  note: string
}

/** 待重试任务队列：跟着每次批次结论变化。 */
export type RetryQueueEntry = {
  id: number
  retryNo: string
  planId: number
  planCode: string
  batchNo: string
  jobCode: string
  reason: string
  checkpointJob: string
  status: RetryStatus
  createdAt: string
  updatedAt: string
}

export type OrchestrationState = {
  version: number
  batchSeq: number
  recordSeq: number
  queueSeq: number
  retrySeq: number
  batches: BatchInstance[]
  records: ExecutionRecord[]
  dispatchQueue: DispatchQueueEntry[]
  retryQueue: RetryQueueEntry[]
}

/** 导出/导入用的结果包：打包时内容必须完整，导入时逐项校验。 */
export type OrchestrationBundle = {
  kind: 'orchestration-result'
  version: number
  exportedAt: string
  plan: { id: number; planCode: string; status: string; fields: Record<string, string> }
  dag: PlanDag | null
  config: PlanRunConfig
  batches: BatchInstance[]
  records: ExecutionRecord[]
  dispatchQueue: DispatchQueueEntry[]
  retryQueue: RetryQueueEntry[]
  batchTasks: { 任务编号: string; status: string }[]
  checksum: string
}
