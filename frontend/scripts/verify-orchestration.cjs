/**
 * 编排调度引擎的离线校验：node scripts/verify-orchestration.cjs
 * 先执行 npx tsc -p tsconfig.verify.json 生成 .verify-out，再运行本脚本。
 * 覆盖：分波准入、并发名额、超时判失败、失败策略、断点续跑、活动实例去重、
 * 跨批次作业冲突、跨模块写回、待重试队列、导出导入完整性、旧数据兼容。
 */
const assert = require('node:assert')
const fs = require('node:fs')
const path = require('node:path')

// frontend 包是 ESM（"type": "module"），给编译产物目录补一个 CommonJS 标记再加载
const outDir = path.join(__dirname, '..', '.verify-out')
fs.writeFileSync(path.join(outDir, 'package.json'), JSON.stringify({ type: 'commonjs' }))

const orch = require('../.verify-out/api/orchestration.js')
const localStore = require('../.verify-out/data/local-store.js')
const orchStore = require('../.verify-out/data/orchestration-store.js')

function planRow(id) {
  return localStore.listRows('schedule').find((row) => Number(row.id) === id)
}

function batchRow(code) {
  return localStore.listRows('batch').find((row) => String(row['任务编号']) === code)
}

function addPlan(fields) {
  const rows = localStore.listRows('schedule')
  const id = Math.max(...rows.map((row) => Number(row.id))) + 1
  localStore.saveRows('schedule', [
    ...rows,
    { id, status: '待校验', pending: true, abnormal: false, ...fields },
  ])
  return id
}

let passed = 0
function check(name, fn) {
  fn()
  passed += 1
  console.log(`ok ${passed} - ${name}`)
}

// 1. 依赖图解析与分波：没有前置关系的作业编入同一波次
check('依赖图分波：SCHE-0001 分两波，BATC-0002/0003 同波', () => {
  const preview = orch.previewPlan(1)
  assert.strictEqual(preview.ok, true)
  assert.deepStrictEqual(
    preview.dag.waves.map((wave) => wave.jobs),
    [['BATC-0001'], ['BATC-0002', 'BATC-0003']],
  )
  assert.deepStrictEqual(preview.config, { concurrency: 2, timeoutSec: 120, policy: '停止后续节点' })
})

// 2. 重复启动同一计划不得出现两个活动实例
check('活动实例去重：SCHE-0003 已有活动批次，重复启动被拒绝', () => {
  const result = orch.startOrchestrationPlan(3)
  assert.strictEqual(result.ok, false)
  assert.match(result.message, /两个活动实例|已存在活动批次/)
})

// 3. 同一作业不能跨两个批次
check('跨批次作业冲突：BATC-0003 被 SCHE-0003-B1 占用，SCHE-0001 启动被拒绝', () => {
  const result = orch.startOrchestrationPlan(1)
  assert.strictEqual(result.ok, false)
  assert.match(result.message, /不能跨两个批次/)
  assert.match(result.message, /BATC-0003/)
})

// 4. 停止活动批次：断点保留，跨模块状态同步
check('停止编排：SCHE-0003 批次停止，执行中记录标记失败并写回批量转换', () => {
  const result = orch.stopOrchestrationPlan(3)
  assert.strictEqual(result.ok, true, result.message)
  const batch = orch.listBatches().find((item) => item.batchNo === 'SCHE-0003-B1')
  assert.strictEqual(batch.status, '已停止')
  const record = orch.listExecutionRecords().find((item) => item.batchNo === 'SCHE-0003-B1')
  assert.strictEqual(record.status, '已失败')
  assert.strictEqual(batchRow('BATC-0003').status, '已失败')
  assert.strictEqual(planRow(3).status, '已停止')
})

// 5. 启动 SCHE-0001：分波准入 + 并发名额 + 结果写回
check('分波执行：SCHE-0001 全部成功，执行记录含波次与名额，结果写回批量转换', () => {
  const result = orch.startOrchestrationPlan(1)
  assert.strictEqual(result.ok, true, result.message)
  const batch = orch.listBatches().find((item) => item.planCode === 'SCHE-0001')
  assert.strictEqual(batch.status, '已完成')
  assert.strictEqual(batch.checkpointJob, 'BATC-0003')
  const records = orch.listExecutionRecords().filter((item) => item.batchNo === batch.batchNo)
  assert.strictEqual(records.length, 3)
  const byJob = Object.fromEntries(records.map((item) => [item.jobCode, item]))
  assert.deepStrictEqual(
    [byJob['BATC-0001'].waveNo, byJob['BATC-0002'].waveNo, byJob['BATC-0003'].waveNo],
    [1, 2, 2],
  )
  assert.deepStrictEqual(
    [byJob['BATC-0001'].slotNo, byJob['BATC-0002'].slotNo, byJob['BATC-0003'].slotNo],
    [1, 1, 2],
  )
  for (const code of ['BATC-0001', 'BATC-0002', 'BATC-0003']) {
    assert.strictEqual(batchRow(code).status, '已完成', `${code} 应写回已完成`)
  }
  // 调度执行队列反映本次结论
  const queue = orch.listDispatchQueue().filter((item) => item.batchNo === batch.batchNo)
  assert.strictEqual(queue.length, 3)
  assert.ok(queue.every((item) => item.status === '已完成' && item.targetModule === 'batch'))
  // 计划进度：执行完毕后回到待校验，可再次启动
  assert.strictEqual(planRow(1).status, '待校验')
})

// 6. 超时判失败 + 继续后续节点策略 + 待重试队列
check('超时失败：SCHE-0002 的 BATC-0002 超时，BATC-0003 未准入无执行记录，进入待重试队列', () => {
  const result = orch.startOrchestrationPlan(2)
  assert.strictEqual(result.ok, false)
  const batch = orch.listBatches().find((item) => item.planCode === 'SCHE-0002')
  assert.strictEqual(batch.status, '已失败')
  assert.deepStrictEqual(batch.failedJobs, ['BATC-0002'])
  assert.deepStrictEqual(batch.skippedJobs, ['BATC-0003'])
  const records = orch.listExecutionRecords().filter((item) => item.batchNo === batch.batchNo)
  assert.strictEqual(records.length, 1, '未获得准入条件的作业不应产生执行记录')
  assert.strictEqual(records[0].jobCode, 'BATC-0002')
  assert.strictEqual(records[0].status, '已失败')
  assert.match(records[0].conclusion, /超过超时秒数 100s/)
  const queue = orch.listDispatchQueue().filter((item) => item.batchNo === batch.batchNo)
  assert.strictEqual(queue.find((item) => item.jobCode === 'BATC-0003').status, '已跳过')
  const retry = orch.listRetryQueue()
  assert.strictEqual(retry.length, 1)
  assert.strictEqual(retry[0].jobCode, 'BATC-0002')
  assert.strictEqual(retry[0].status, '待重试')
  assert.strictEqual(batchRow('BATC-0002').status, '已失败')
  assert.strictEqual(planRow(2).status, '已停止')
})

// 7. 重试续跑：接着最后完成的作业，待重试队列跟着结论变化
check('断点续跑：重试 SCHE-0002 批次，从断点继续并最终完成', () => {
  const batch = orch.listBatches().find((item) => item.planCode === 'SCHE-0002')
  const before = orch.listExecutionRecords().filter((item) => item.batchNo === batch.batchNo).length
  const result = orch.retryBatch(batch.id)
  assert.strictEqual(result.ok, true, result.message)
  const after = orch.listBatches().find((item) => item.planCode === 'SCHE-0002')
  assert.strictEqual(after.status, '已完成')
  assert.strictEqual(after.attempt, 2)
  const records = orch.listExecutionRecords().filter((item) => item.batchNo === batch.batchNo)
  assert.strictEqual(records.length, before + 2, '重试只补跑未完成节点')
  assert.ok(records.every((item) => item.attempt === 1 || item.attempt === 2))
  assert.strictEqual(orch.listRetryQueue()[0].status, '已重试')
  assert.strictEqual(batchRow('BATC-0002').status, '已完成')
  assert.strictEqual(batchRow('BATC-0003').status, '已完成')
  assert.strictEqual(planRow(2).status, '待校验')
})

// 8. 写入中断保留断点，事务失败回到待校验，重试接着断点跑
check('写中断：名额批次落盘后中断，断点保留，计划回待校验，重试续跑完成', () => {
  const planId = addPlan({
    计划编号: 'SCHE-0009',
    任务列表: 'BATC-0001,BATC-0002',
    依赖图定义: '',
    调度表达式: '0 6 * * *',
    并发上限: '1',
    失败处理: '停止后续节点',
    超时秒数: '120',
    通知策略: '演练用：模拟写中断',
  })
  const started = orch.startOrchestrationPlan(planId)
  assert.strictEqual(started.ok, false)
  assert.match(started.message, /写入中断/)
  const batch = orch.listBatches().find((item) => item.planCode === 'SCHE-0009')
  assert.strictEqual(batch.status, '已中断')
  assert.strictEqual(batch.checkpointJob, 'BATC-0001', '第一个名额批次已落盘，断点应保留')
  assert.deepStrictEqual(batch.completedJobs, ['BATC-0001'])
  assert.strictEqual(planRow(planId).status, '待校验')
  // 中断批次仍算活动实例：同计划不得再启新批次
  assert.strictEqual(orch.startOrchestrationPlan(planId).ok, false)
  const retried = orch.retryBatch(batch.id)
  assert.strictEqual(retried.ok, true, retried.message)
  const done = orch.listBatches().find((item) => item.planCode === 'SCHE-0009')
  assert.strictEqual(done.status, '已完成')
  assert.deepStrictEqual(done.completedJobs.sort(), ['BATC-0001', 'BATC-0002'])
  const records = orch.listExecutionRecords().filter((item) => item.batchNo === batch.batchNo)
  assert.strictEqual(records.filter((item) => item.jobCode === 'BATC-0001').length, 1, '断点前的作业不重复执行')
})

// 9. 循环依赖：事务失败回到待校验
check('循环依赖：依赖图校验失败，事务回滚，计划回到待校验', () => {
  const planId = addPlan({
    计划编号: 'SCHE-0010',
    任务列表: 'JOB-A,JOB-B',
    依赖图定义: 'JOB-A:JOB-B;JOB-B:JOB-A',
    调度表达式: '0 8 * * *',
    并发上限: '1',
    失败处理: '停止后续节点',
    超时秒数: '60',
    通知策略: '站内消息',
  })
  const result = orch.startOrchestrationPlan(planId)
  assert.strictEqual(result.ok, false)
  assert.match(result.message, /循环依赖/)
  assert.match(result.message, /待校验/)
  assert.strictEqual(planRow(planId).status, '待校验')
  assert.ok(orch.listBatches().every((item) => item.planCode !== 'SCHE-0010'), '校验失败不应产生批次')
})

// 10. 旧版本数据兼容：历史口径的占位字段仍可启动并走完整流程
check('旧数据兼容：占位字段的计划按历史口径解析，单波串行执行', () => {
  const planId = addPlan({
    计划编号: 'SCHE-0011',
    任务列表: '编排调度样例X',
    依赖图定义: '编排调度样例X',
    调度表达式: '编排调度样例X',
    并发上限: '编排调度样例X',
    失败处理: '编排调度样例X',
    超时秒数: '编排调度样例X',
    通知策略: '编排调度样例X',
  })
  const result = orch.startOrchestrationPlan(planId)
  assert.strictEqual(result.ok, true, result.message)
  const batch = orch.listBatches().find((item) => item.planCode === 'SCHE-0011')
  assert.strictEqual(batch.status, '已完成')
  assert.strictEqual(batch.totalWaves, 1)
  assert.strictEqual(planRow(planId).status, '待校验')
})

// 11. 导出打包内容完整，导入可还原；篡改或缺段被拒绝
check('导出导入：结果包可完整还原，校验和能识别篡改与缺段', () => {
  const built = orch.buildPlanBundle(1)
  assert.strictEqual(built.ok, true)
  const bundle = built.bundle
  for (const key of ['plan', 'config', 'batches', 'records', 'dispatchQueue', 'retryQueue', 'batchTasks', 'checksum']) {
    assert.ok(bundle[key] !== undefined, `结果包应包含 ${key}`)
  }
  assert.ok(bundle.records.length >= 3)
  // 完整内容可导入
  const imported = orch.importPlanBundle(JSON.stringify(bundle))
  assert.strictEqual(imported.ok, true, imported.message)
  // 篡改内容：校验和不匹配
  const tampered = JSON.parse(JSON.stringify(bundle))
  tampered.records[0].durationSec = 9999
  const tamperedResult = orch.importPlanBundle(JSON.stringify(tampered))
  assert.strictEqual(tamperedResult.ok, false)
  assert.match(tamperedResult.message, /校验和/)
  // 内容缺段：拒绝导入
  const incomplete = JSON.parse(JSON.stringify(bundle))
  delete incomplete.dispatchQueue
  const incompleteResult = orch.importPlanBundle(JSON.stringify(incomplete))
  assert.strictEqual(incompleteResult.ok, false)
  assert.match(incompleteResult.message, /不完整/)
  // 非结果包文件：拒绝导入
  assert.strictEqual(orch.importPlanBundle('{"foo":1}').ok, false)
  assert.strictEqual(orch.importPlanBundle('不是json').ok, false)
})

// 12. 已完成的计划可再次启动：活动实例限制只针对活动批次
check('再次启动：上一批次已结论，SCHE-0001 可启动新批次', () => {
  const result = orch.startOrchestrationPlan(1)
  assert.strictEqual(result.ok, true, result.message)
  const batches = orch.listBatches().filter((item) => item.planCode === 'SCHE-0001')
  assert.ok(batches.length >= 2, '应存在多个历史批次')
  assert.ok(batches.every((item) => item.status === '已完成'))
})

// 13. 存储迁移：v1 历史数据补齐结构后沿用历史口径
check('版本迁移：v1 结构补齐队列字段，版本号升到当前', () => {
  const legacy = { version: 1, batches: [], records: [] }
  const migrated = orchStore.migrateOrchestrationState(legacy)
  assert.strictEqual(migrated.version, orchStore.ORCHESTRATION_VERSION)
  assert.ok(Array.isArray(migrated.dispatchQueue), 'v1 缺少调度队列，迁移后应补齐')
  assert.ok(Array.isArray(migrated.retryQueue), 'v1 缺少待重试队列，迁移后应补齐')
  assert.strictEqual(orchStore.migrateOrchestrationState(null).version, orchStore.ORCHESTRATION_VERSION)
})

console.log(`\n全部 ${passed} 项校验通过`)
