import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

import { downloadEntries, listEntries, moduleMeta, runAction as applyAction } from '@/api/local-service'
import {
  downloadPlanBundle,
  importPlanBundle,
  listBatches,
  listDispatchQueue,
  listExecutionRecords,
  listRetryQueue,
  previewPlan,
  retryBatch,
  retryFromQueue,
} from '@/api/orchestration'
import type {
  BatchInstance,
  DispatchQueueEntry,
  ExecutionRecord,
  RetryQueueEntry,
} from '@/data/orchestration-types'
import type { EntryRow } from '@/data/types'

const meta = moduleMeta('schedule')
const columns = [
  "计划编号",
  "任务列表",
  "依赖图定义",
  "调度表达式",
  "并发上限",
  "失败处理",
  "超时秒数",
  "通知策略"
]
const actions = [
  "提交校验",
  "启动编排",
  "停止编排"
]
const statuses = [
  "草稿",
  "待校验",
  "运行中",
  "已停止"
]

function badgeClass(status: string): string {
  if (['已完成', '已重试'].includes(status)) return 'badge ok'
  if (['已失败', '已中断'].includes(status)) return 'badge fail'
  if (['运行中', '已准入'].includes(status)) return 'badge run'
  if (['待重试', '待准入', '待校验'].includes(status)) return 'badge warn'
  return 'badge'
}

function fmtTime(value: string): string {
  return value ? value.replace('T', ' ').slice(0, 19) : '—'
}

export default function ScheduleView() {
  const [rows, setRows] = useState<EntryRow[]>([])
  const [total, setTotal] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [notice, setNotice] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const [expandedId, setExpandedId] = useState<number | null>(null)
  const [batches, setBatches] = useState<BatchInstance[]>([])
  const [records, setRecords] = useState<ExecutionRecord[]>([])
  const [dispatch, setDispatch] = useState<DispatchQueueEntry[]>([])
  const [retryQueue, setRetryQueue] = useState<RetryQueueEntry[]>([])
  const fileInputRef = useRef<HTMLInputElement | null>(null)
  const filterFields = useMemo(() => columns.slice(0, 3), [])

  const reload = useCallback(() => {
    setErrorMessage('')
    try {
      const payload = listEntries(meta.key, filters)
      setRows(payload.items)
      setTotal(payload.total)
      setBatches(listBatches())
      setRecords(listExecutionRecords())
      setDispatch(listDispatchQueue())
      setRetryQueue(listRetryQueue())
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '编排调度列表读取失败')
    }
  }, [filters])

  useEffect(() => {
    reload()
  }, [reload])

  function report(result: { ok: boolean; message: string }) {
    if (result.ok) {
      setNotice(result.message)
    } else {
      setErrorMessage(result.message)
    }
    reload()
  }

  function safeRun(action: () => { ok: boolean; message: string }) {
    setErrorMessage('')
    setNotice('')
    try {
      report(action())
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '操作执行失败')
      reload()
    }
  }

  function runAction(action: string, row: EntryRow) {
    safeRun(() => applyAction(meta.key, Number(row.id), action))
  }

  function runImport(file: File) {
    setErrorMessage('')
    setNotice('')
    file
      .text()
      .then((text) => report(importPlanBundle(text)))
      .catch(() => {
        setErrorMessage('编排结果文件读取失败')
        reload()
      })
  }

  const stats = [
    { label: '编排计划总数', value: rows.length },
    { label: '运行中计划', value: rows.filter((row) => String(row.status) === '运行中').length },
    { label: '待校验计划', value: rows.filter((row) => String(row.status) === '待校验').length },
    { label: '活动批次', value: batches.filter((batch) => batch.status === '运行中' || batch.status === '已中断').length },
  ]

  return (
    <section className="page" data-module="schedule">
      <header className="page-head">
        <div>
          <h2>编排调度管理</h2>
          <p className="page-desc">解析依赖图分波执行：拿到并发名额且上游完成的作业才准入，结论同步调度执行队列、待重试队列与批量转换任务。</p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => setErrorMessage('编排计划登记入口尚未接入审批流')}>
            登记编排计划
          </button>
          <button className="btn" type="button" onClick={() => fileInputRef.current?.click()}>
            导入编排结果
          </button>
          <button className="btn" type="button" onClick={() => downloadEntries(meta.key)}>
            导出编排调度清单
          </button>
          <input
            ref={fileInputRef}
            type="file"
            accept="application/json,.json"
            style={{ display: 'none' }}
            onChange={(event) => {
              const file = event.target.files?.[0]
              if (file) {
                runImport(file)
              }
              event.target.value = ''
            }}
          />
        </div>
      </header>

      <div className="stat-row">
        {stats.map((item) => (
          <article key={item.label} className="stat-card">
            <span className="stat-label">{item.label}</span>
            <strong className="stat-value">{item.value}</strong>
          </article>
        ))}
      </div>

      <p className="status-legend">
        {statuses.map((status: string) => (
          <span key={status} className="legend-item">
            {status}：{rows.filter((row) => String(row.status) === status).length}
          </span>
        ))}
      </p>

      <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); reload() }}>
        {filterFields.map((field) => (
          <label key={field} className="filter-item">
            <span>{field}</span>
            <input
              value={filters[field] ?? ''}
              placeholder={`按${field}检索`}
              onChange={(event) => setFilters((prev) => ({ ...prev, [field]: event.target.value }))}
            />
          </label>
        ))}
        <button className="btn" type="submit">查询</button>
        <button className="btn ghost" type="button" onClick={() => setFilters({})}>重置条件</button>
      </form>

      <table className="data-table">
        <thead>
          <tr>
            {columns.map((column) => <th key={column}>{column}</th>)}
            <th>当前状态</th>
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => {
            const planId = Number(row.id)
            const expanded = expandedId === planId
            return [
              <tr key={String(row.id)}>
                {columns.map((column) => <td key={column}>{row[column] ?? '—'}</td>)}
                <td><span className={badgeClass(String(row.status))}>{row.status}</span></td>
                <td className="row-actions">
                  {actions.map((action) => (
                    <button key={action} className="link" type="button" onClick={() => runAction(action, row)}>
                      {action}
                    </button>
                  ))}
                  <button
                    className="link"
                    type="button"
                    onClick={() => setExpandedId(expanded ? null : planId)}
                  >
                    {expanded ? '收起波次' : '波次预览'}
                  </button>
                  <button className="link" type="button" onClick={() => safeRun(() => downloadPlanBundle(planId))}>
                    导出结果
                  </button>
                </td>
              </tr>,
              expanded && (
                <tr key={`${String(row.id)}-waves`} className="detail-row">
                  <td colSpan={columns.length + 2}>
                    <WavePreview planId={planId} />
                  </td>
                </tr>
              ),
            ]
          })}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={columns.length + 2}>暂无编排调度数据，可先登记编排计划</td>
            </tr>
          )}
        </tbody>
      </table>

      <section className="section-block">
        <h3>批次实例</h3>
        <table className="data-table">
          <thead>
            <tr>
              <th>批次号</th><th>计划编号</th><th>状态</th><th>尝试次数</th><th>波次进度</th>
              <th>断点（最后完成作业）</th><th>完成/失败/跳过</th><th>开始时间</th><th>结束时间</th><th>操作</th>
            </tr>
          </thead>
          <tbody>
            {batches.map((batch) => (
              <tr key={batch.id}>
                <td>{batch.batchNo}</td>
                <td>{batch.planCode}</td>
                <td><span className={badgeClass(batch.status)}>{batch.status}</span></td>
                <td>{batch.attempt}</td>
                <td>{batch.currentWave}/{batch.totalWaves}</td>
                <td>{batch.checkpointJob || '—'}</td>
                <td>{batch.completedJobs.length}/{batch.failedJobs.length}/{batch.skippedJobs.length}</td>
                <td>{fmtTime(batch.startedAt)}</td>
                <td>{fmtTime(batch.finishedAt)}</td>
                <td className="row-actions">
                  {(batch.status === '已失败' || batch.status === '已中断') && (
                    <button className="link" type="button" onClick={() => safeRun(() => retryBatch(batch.id))}>
                      重试续跑
                    </button>
                  )}
                  <span className="cell-note">{batch.note}</span>
                </td>
              </tr>
            ))}
            {batches.length === 0 && (
              <tr><td className="empty-state" colSpan={10}>暂无批次实例，启动编排计划后在此跟踪</td></tr>
            )}
          </tbody>
        </table>
      </section>

      <section className="section-block">
        <h3>执行记录（仅含获得并发名额且上游已完成的作业）</h3>
        <table className="data-table">
          <thead>
            <tr>
              <th>批次号</th><th>作业编号</th><th>波次</th><th>名额</th><th>尝试</th><th>状态</th>
              <th>耗时(秒)</th><th>超时(秒)</th><th>结论</th><th>时间</th>
            </tr>
          </thead>
          <tbody>
            {records.slice(0, 30).map((record) => (
              <tr key={record.id}>
                <td>{record.batchNo}</td>
                <td>{record.jobCode}</td>
                <td>第{record.waveNo}波</td>
                <td>名额{record.slotNo}</td>
                <td>{record.attempt}</td>
                <td><span className={badgeClass(record.status)}>{record.status}</span></td>
                <td>{record.durationSec}</td>
                <td>{record.timeoutSec}</td>
                <td>{record.conclusion}</td>
                <td>{fmtTime(record.finishedAt || record.startedAt)}</td>
              </tr>
            ))}
            {records.length === 0 && (
              <tr><td className="empty-state" colSpan={10}>暂无执行记录</td></tr>
            )}
          </tbody>
        </table>
      </section>

      <section className="section-block">
        <h3>调度执行队列（跨模块下发批量转换）</h3>
        <table className="data-table">
          <thead>
            <tr>
              <th>队列编号</th><th>计划编号</th><th>批次号</th><th>作业编号</th><th>波次</th>
              <th>目标模块</th><th>目标对象</th><th>队列状态</th><th>说明</th><th>更新时间</th>
            </tr>
          </thead>
          <tbody>
            {dispatch.slice(0, 30).map((entry) => (
              <tr key={entry.id}>
                <td>{entry.queueNo}</td>
                <td>{entry.planCode}</td>
                <td>{entry.batchNo}</td>
                <td>{entry.jobCode}</td>
                <td>第{entry.waveNo}波</td>
                <td>{entry.targetModule}</td>
                <td>{entry.targetKey}</td>
                <td><span className={badgeClass(entry.status)}>{entry.status}</span></td>
                <td>{entry.note}</td>
                <td>{fmtTime(entry.updatedAt)}</td>
              </tr>
            ))}
            {dispatch.length === 0 && (
              <tr><td className="empty-state" colSpan={10}>暂无调度执行队列记录</td></tr>
            )}
          </tbody>
        </table>
      </section>

      <section className="section-block">
        <h3>待重试任务队列</h3>
        <table className="data-table">
          <thead>
            <tr>
              <th>重试编号</th><th>计划编号</th><th>批次号</th><th>作业编号</th>
              <th>失败原因</th><th>断点位置</th><th>状态</th><th>更新时间</th><th>操作</th>
            </tr>
          </thead>
          <tbody>
            {retryQueue.map((entry) => (
              <tr key={entry.id}>
                <td>{entry.retryNo}</td>
                <td>{entry.planCode}</td>
                <td>{entry.batchNo}</td>
                <td>{entry.jobCode}</td>
                <td>{entry.reason}</td>
                <td>{entry.checkpointJob || '起点'}</td>
                <td><span className={badgeClass(entry.status)}>{entry.status}</span></td>
                <td>{fmtTime(entry.updatedAt)}</td>
                <td className="row-actions">
                  {entry.status === '待重试' && (
                    <button className="link" type="button" onClick={() => safeRun(() => retryFromQueue(entry.id))}>
                      立即重试
                    </button>
                  )}
                </td>
              </tr>
            ))}
            {retryQueue.length === 0 && (
              <tr><td className="empty-state" colSpan={9}>暂无待重试任务</td></tr>
            )}
          </tbody>
        </table>
      </section>

      <footer className="page-foot">
        <span>共 {total} 条编排调度记录</span>
        {notice && <span className="notice-text">{notice}</span>}
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}

function WavePreview({ planId }: { planId: number }) {
  const preview = previewPlan(planId)
  if (!preview.ok) {
    return <p className="error-text">依赖图校验失败：{preview.errors.join('；')}（启动时事务会回滚，计划回到待校验）</p>
  }
  const { dag, config } = preview
  return (
    <div className="wave-preview">
      <p className="wave-line">
        运行参数：单波并发名额 {config.concurrency} ｜ 节点超时 {config.timeoutSec} 秒 ｜ 失败处理「{config.policy}」
      </p>
      {dag.waves.map((wave) => (
        <p key={wave.waveNo} className="wave-line">
          第{wave.waveNo}波：{wave.jobs.join('、')}
          {wave.jobs.length > 1 ? '（互不依赖，编入同一波次）' : ''}
        </p>
      ))}
    </div>
  )
}
