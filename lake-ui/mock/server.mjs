/**
 * 前端联调用的最小后端：本机没有 Presto / Docker 时也能把页面点通。
 *
 *   node mock/server.mjs            # http://127.0.0.1:4173
 *   node mock/server.mjs --port 4300
 *
 * 它同时提供 /api/**、/actuator/health 和构建产物（static/）静态文件，
 * 所以可以直接对着它截图验版式。真实数据请跑 lake-service。
 */
import http from 'node:http'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const ROOT = path.resolve(__dirname, '../../lake-service/src/main/resources/static')
const PORT = Number(process.argv[process.argv.indexOf('--port') + 1]) || 4173

const CATALOG = 'iceberg'
const DATABASE = 'lake'

/**
 * 镜像 platform-common 的 LakeTables：字段中文名就是建表 DDL 里的 COMMENT，
 * 真实后端从 iceberg.information_schema.columns.comment 读，这里照抄一份给 mock 用。
 */
const TABLES = [
  {
    table: 'bronze_orders',
    columns: [
      ['order_id', 'varchar', '订单号'],
      ['user_id', 'varchar', '用户号'],
      ['merchant_id', 'varchar', '商户号'],
      ['category', 'varchar', '品类'],
      ['amount', 'decimal(12,2)', '订单金额'],
      ['currency', 'varchar', '币种'],
      ['status', 'varchar', 'PAID/REFUNDED/CANCELLED'],
      ['event_time', 'timestamp', '业务发生时间(分区字段来源)'],
      ['ingest_time', 'timestamp', '入湖时间'],
      ['source_file', 'varchar', '来源文件']
    ]
  },
  {
    table: 'gold_daily_revenue',
    columns: [
      ['dt', 'date', '业务日期'],
      ['merchant_id', 'varchar', '商户号'],
      ['category', 'varchar', '品类'],
      ['order_cnt', 'bigint', '有效订单数'],
      ['buyer_cnt', 'bigint', '本商户+品类下的下单用户数(跨行不可相加)'],
      ['gmv', 'decimal(18,2)', '成交总额'],
      ['refund_amount', 'decimal(18,2)', '退款金额'],
      ['net_revenue', 'decimal(18,2)', '净收入=GMV-退款'],
      ['updated_at', 'timestamp', '最后更新时间']
    ]
  },
  {
    table: 'silver_orders',
    columns: [
      ['order_id', 'varchar', '订单号(主键)'],
      ['user_id', 'varchar', '用户号'],
      ['merchant_id', 'varchar', '商户号'],
      ['category', 'varchar', '品类'],
      ['amount', 'decimal(12,2)', '订单金额'],
      ['currency', 'varchar', '币种'],
      ['status', 'varchar', 'PAID/REFUNDED/CANCELLED'],
      ['is_refund', 'boolean', '是否退款'],
      ['dt', 'date', '业务日期(UTC)'],
      ['event_time', 'timestamp', '业务发生时间'],
      ['updated_at', 'timestamp', '最后更新时间']
    ]
  }
].map((t) => ({
  schema: DATABASE,
  table: t.table,
  type: 'BASE TABLE',
  columns: t.columns.map(([name, type, comment]) => ({ name, type, comment }))
}))

const DAILY = [
  ['2026-11-16', 412, 186320.5, 7420.0, 178900.5],
  ['2026-11-15', 398, 173880.0, 6980.5, 166899.5],
  ['2026-11-14', 455, 204115.0, 9310.0, 194805.0],
  ['2026-11-13', 377, 158420.8, 5410.0, 153010.8],
  ['2026-11-12', 496, 231740.0, 12040.0, 219700.0],
  ['2026-11-11', 528, 296180.0, 15880.0, 280300.0],
  ['2026-11-10', 341, 142960.5, 4120.0, 138840.5],
  ['2026-11-09', 302, 121480.0, 3860.0, 117620.0],
  ['2026-11-08', 289, 112740.0, 2940.0, 109800.0],
  ['2026-11-07', 311, 128050.0, 3510.0, 124540.0]
]

const MERCHANTS = [
  ['M017', 148920.0, 512],
  ['M003', 132480.5, 471],
  ['M042', 98760.0, 388],
  ['M008', 86420.0, 342],
  ['M031', 74120.5, 296],
  ['M025', 61840.0, 254],
  ['M011', 54320.0, 210],
  ['M006', 48960.5, 187],
  ['M049', 39820.0, 158],
  ['M014', 31240.0, 121]
]

const ORDERS = [
  ['O-20261116-88231', 'M017', '3C', '1288.00', 'PAID', '2026-11-16 21:04:12'],
  ['O-20261116-88214', 'M003', 'HOME', '349.90', 'PAID', '2026-11-16 20:58:44'],
  ['O-20261116-88190', 'M042', 'APPAREL', '259.00', 'REFUNDED', '2026-11-16 20:41:03'],
  ['O-20261116-88173', 'M008', '3C', '4999.00', 'PAID', '2026-11-16 20:22:19'],
  ['O-20261116-88152', 'M031', 'GROCERY', '86.50', 'PAID', '2026-11-16 20:07:56'],
  ['O-20261116-88134', 'M017', '3C', '1599.00', 'CANCELLED', '2026-11-16 19:52:31'],
  ['O-20261116-88110', 'M025', 'HOME', '129.00', 'PAID', '2026-11-16 19:38:02'],
  ['O-20261116-88092', 'M003', 'APPAREL', '699.00', 'PAID', '2026-11-16 19:21:48'],
  ['O-20261116-88071', 'M011', '3C', '379.00', 'PAID', '2026-11-16 19:03:27'],
  ['O-20261116-88045', 'M049', 'GROCERY', '142.30', 'REFUNDED', '2026-11-16 18:47:10']
]

/**
 * 镜像 platform-common 的 LakeJobs.METAS：mock 不进 Java 构建，名字和文案只能靠这几行对齐。
 * group=pipeline 进主链路那组，other 进下面那组；cliOnly 的按钮置灰。
 */
const JOBS = [
  { name: 'init', title: '建库建表', desc: 'Iceberg DDL：库 + ODS/DWD/ADS 三张表与表属性', group: 'pipeline' },
  { name: 'ingest', title: '数据入湖', desc: '生成上游落地文件并写入 bronze（含 3% 重复 + 10% 迟到）', group: 'pipeline' },
  { name: 'transform', title: '加工汇总', desc: 'MERGE 去重进 silver，再重算 gold 汇总', group: 'pipeline' },
  { name: 'evolve', title: 'Schema 演进', desc: '新增 channel 列并回填，演示 ADD COLUMN 与元数据升级', group: 'other' },
  { name: 'maintain', title: '表维护', desc: 'rewrite_data_files 合并小文件 + expire_snapshots', group: 'pipeline' },
  { name: 'show', title: '数据概览', desc: '打印三层行数与分区分布', group: 'pipeline' },
  { name: 'verify-sql', title: 'SQL 校验', desc: '用 Spark 执行 Presto 脚本，需要 --sql-file 参数', group: 'other', cliOnly: true },
  { name: 'all', title: '全链路', desc: 'init → ingest → transform → maintain → show', group: 'other', featured: true }
].map((j) => ({ cliOnly: false, featured: false, ...j }))

const config = {
  // 与 PlatformConfig.describe() 的真实输出保持一致：一句 k=v 摘要，不是对象
  platform:
    'catalog=iceberg(type=hadoop) database=lake warehouse=file:///home/leishu/IdeaProjects/data-demo/warehouse/ ' +
    'formatVersion=2 writeMode=copy-on-write hiveUri=- s3Auth=-',
  catalog: CATALOG,
  database: DATABASE,
  warehouse: '/home/leishu/IdeaProjects/data-demo/warehouse',
  writeMode: 'copy-on-write',
  prestoUrl: 'jdbc:presto://127.0.0.1:8080/iceberg/lake',
  query: { maxRows: 1000, timeoutSeconds: 60, poolSize: 8, allowWrites: false },
  batch: {
    enabled: true,
    commandTemplate: 'bin/dev-batch.sh --job {job}',
    allowedJobs: JOBS.map((j) => j.name),
    // 与服务端 /api/config 同形：前端拿它直接渲染卡片，不再自己维护文案
    jobs: JOBS
  }
}

const runs = []
let seq = 0

function startRun(job) {
  const id = `run-${Date.now()}-${(seq++).toString(16).padStart(6, '0')}`
  const run = {
    id,
    job,
    command: `bin/dev-batch.sh --job ${job}`,
    status: 'RUNNING',
    exitCode: null,
    startedAt: new Date().toISOString(),
    finishedAt: null,
    durationMs: null,
    logFile: `logs/batch/${id}.log`,
    message: null
  }
  runs.unshift(run)
  // 4s 后自动完成，模拟一次 Spark 作业
  setTimeout(() => {
    const elapsed = 4000 + Math.round(Math.random() * 3000)
    run.status = 'SUCCEEDED'
    run.exitCode = 0
    run.finishedAt = new Date().toISOString()
    run.durationMs = elapsed
    run.message = 'completed'
  }, 4000)
  return run
}

const TAIL = (job) => [
  `[mock] job=${job} 开始`,
  '26/11/18 09:12:01 INFO SparkContext: Running Spark version 3.5.1',
  '26/11/18 09:12:03 INFO IcebergSparkSessionExtensions: Iceberg extensions registered',
  `26/11/18 09:12:05 INFO ${'BatchApp'}: job=${job} warehouse=${config.warehouse}`,
  '26/11/18 09:12:08 INFO SparkSqlParser: Parsing command: CREATE TABLE IF NOT EXISTS ...',
  '26/11/18 09:12:11 INFO IcebergWriter: wrote 1400 rows into lake.bronze_orders',
  '26/11/18 09:12:14 INFO SnapshotProducer: committed snapshot 2187431928',
  '[mock] job 完成，exit=0'
]

function send(res, status, body, type = 'application/json') {
  const payload = Buffer.isBuffer(body) || typeof body === 'string' ? body : JSON.stringify(body)
  res.writeHead(status, { 'Content-Type': `${type}; charset=utf-8`, 'Cache-Control': 'no-store' })
  res.end(payload)
}

function queryResult(columns, rows, elapsedMs = 42) {
  return { columns, rows, rowCount: rows.length, truncated: false, elapsedMs, stats: null }
}

/** 开一条 SSE 连接，返回 emit(event, data)；调用方自己负责 res.end() */
function sse(res) {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-cache, no-store',
    Connection: 'keep-alive',
    // 告诉可能存在的中间层别缓冲，否则事件会攒成一团
    'X-Accel-Buffering': 'no'
  })
  return (event, data) => res.write(`event:${event}\ndata:${JSON.stringify(data)}\n\n`)
}

function pick(sql) {
  const s = sql.toLowerCase()
  if (s.includes('gold_daily_revenue') && s.includes('merchant_id')) {
    return queryResult(
      [{ name: 'merchant_id', type: 'varchar' }, { name: 'gmv', type: 'decimal(18,2)' }, { name: 'orders', type: 'bigint' }],
      MERCHANTS.map((m) => [m[0], m[1].toFixed(2), String(m[2])]),
      118
    )
  }
  if (s.includes('gold_daily_revenue')) {
    return queryResult(
      [
        { name: 'dt', type: 'date' },
        { name: 'orders', type: 'bigint' },
        { name: 'gmv', type: 'decimal(18,2)' },
        { name: 'refunded', type: 'decimal(18,2)' },
        { name: 'net_revenue', type: 'decimal(18,2)' }
      ],
      DAILY.map((d) => [d[0], String(d[1]), d[2].toFixed(2), d[3].toFixed(2), d[4].toFixed(2)]),
      187
    )
  }
  return queryResult(
    [
      { name: 'order_id', type: 'varchar' },
      { name: 'merchant_id', type: 'varchar' },
      { name: 'category', type: 'varchar' },
      { name: 'amount', type: 'decimal(12,2)' },
      { name: 'status', type: 'varchar' },
      { name: 'event_time', type: 'timestamp' }
    ],
    ORDERS,
    96
  )
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`)
  const p = url.pathname
  const method = req.method

  if (p === '/actuator/health') {
    return send(res, 200, {
      status: 'UP',
      components: { presto: { status: 'UP' }, diskSpace: { status: 'UP' } }
    })
  }

  if (p === '/api/config') return send(res, 200, config)

  if (p === '/api/tables') {
    return send(res, 200, TABLES)
  }

  if (p.startsWith('/api/tables/') && p.endsWith('/sample')) {
    const table = decodeURIComponent(p.split('/')[3])
    const limit = Number(url.searchParams.get('limit') || 50)
    const body =
      table === 'gold_daily_revenue'
        ? queryResult(
            [
              { name: 'dt', type: 'date' },
              { name: 'merchant_id', type: 'varchar' },
              { name: 'category', type: 'varchar' },
              { name: 'order_cnt', type: 'bigint' },
              { name: 'buyer_cnt', type: 'bigint' },
              { name: 'gmv', type: 'decimal(18,2)' },
              { name: 'refund_amount', type: 'decimal(18,2)' },
              { name: 'net_revenue', type: 'decimal(18,2)' }
            ],
            DAILY.slice(0, limit).map((d, i) => [
              d[0],
              MERCHANTS[i % MERCHANTS.length][0],
              ['3C', 'HOME', 'APPAREL', 'GROCERY'][i % 4],
              String(d[1]),
              String(Math.round(d[1] * 0.61)),
              d[2].toFixed(2),
              d[3].toFixed(2),
              d[4].toFixed(2)
            ])
          )
        : queryResult(
            [
              { name: 'order_id', type: 'varchar' },
              { name: 'user_id', type: 'varchar' },
              { name: 'merchant_id', type: 'varchar' },
              { name: 'category', type: 'varchar' },
              { name: 'amount', type: 'decimal(12,2)' },
              { name: 'status', type: 'varchar' },
              { name: 'event_time', type: 'timestamp' }
            ],
            ORDERS.slice(0, limit).map((o, i) => [
              o[0],
              `U-${1000 + i}`,
              o[1],
              o[2],
              o[3],
              o[4],
              o[5]
            ])
          )
    return send(res, 200, body)
  }

  if (p === '/api/query/stream' && method === 'POST') {
    let raw = ''
    req.on('data', (c) => (raw += c))
    req.on('end', () => {
      let sql = ''
      try {
        sql = JSON.parse(raw || '{}').sql || ''
      } catch {
        return send(res, 400, { status: 400, error: 'Bad Request', message: '请求体不是合法 JSON', hint: null })
      }
      if (!sql.trim()) {
        return send(res, 400, {
          status: 400,
          error: 'Bad Request',
          message: 'SQL 需为单条只读语句（select/with/show/describe/explain）',
          hint: null
        })
      }

      const body = pick(sql)
      const emit = sse(res)
      let closed = false
      res.on('close', () => (closed = true))

      // 和真后端一样：先发列定义，再把行拆成几批发
      emit('meta', { columns: body.columns })
      const chunks = []
      for (let i = 0; i < body.rows.length; i += 4) chunks.push(body.rows.slice(i, i + 4))

      let i = 0
      const tick = () => {
        if (closed) return
        if (i < chunks.length) {
          emit('rows', { rows: chunks[i++] })
          return setTimeout(tick, 120)
        }
        emit('done', {
          rowCount: body.rowCount,
          truncated: body.truncated,
          elapsedMs: body.elapsedMs,
          stats: body.stats
        })
        res.end()
      }
      setTimeout(tick, 160)
    })
    return
  }

  if (p === '/api/query' && method === 'POST') {
    let raw = ''
    req.on('data', (c) => (raw += c))
    req.on('end', () => {
      let sql = ''
      try {
        sql = JSON.parse(raw || '{}').sql || ''
      } catch {
        return send(res, 400, { status: 400, error: 'Bad Request', message: '请求体不是合法 JSON', hint: null })
      }
      if (!sql.trim()) {
        return send(res, 400, {
          status: 400,
          error: 'Bad Request',
          message: 'SQL 需为单条只读语句（select/with/show/describe/explain）',
          hint: null
        })
      }
      setTimeout(() => send(res, 200, pick(sql)), 260)
    })
    return
  }

  if (p.startsWith('/api/batch/runs/') && p.endsWith('/events')) {
    const id = decodeURIComponent(p.split('/')[4])
    const run = runs.find((r) => r.id === id)
    if (!run) {
      return send(res, 404, { status: 404, error: 'Not Found', message: '没有这个运行记录: ' + id, hint: null })
    }

    const emit = sse(res)
    const lines = TAIL(run.job)
    let sent = 0
    let closed = false
    res.on('close', () => (closed = true))

    emit('status', { run })
    const timer = setInterval(() => {
      if (closed) return clearInterval(timer)
      if (sent < lines.length) {
        emit('log', { lines: [lines[sent++]] })
        return
      }
      // startRun 4s 后把状态改成 SUCCEEDED，这里跟着收尾
      if (run.status === 'RUNNING') return
      emit('end', { run })
      clearInterval(timer)
      res.end()
    }, 350)
    return
  }

  if (p.startsWith('/api/batch/runs/')) {
    const id = decodeURIComponent(p.split('/')[4])
    const run = runs.find((r) => r.id === id)
    if (!run) {
      return send(res, 404, { status: 404, error: 'Not Found', message: '没有这个运行记录: ' + id, hint: null })
    }
    return send(res, 200, { run, tail: TAIL(run.job) })
  }

  if (p === '/api/batch/runs') {
    return send(res, 200, {
      runs: runs.slice(0, Number(url.searchParams.get('limit') || 20)),
      history: { status: 'OK', hint: null, table: 'iceberg.ops.batch_runs' }
    })
  }

  if (p.startsWith('/api/batch/') && method === 'POST') {
    const job = decodeURIComponent(p.split('/')[3])
    if (!config.batch.allowedJobs.includes(job)) {
      return send(res, 400, {
        status: 400,
        error: 'Bad Request',
        message: '不允许的作业: ' + job,
        hint: 'allowed = ' + config.batch.allowedJobs.join(', ')
      })
    }
    return send(res, 202, startRun(job))
  }

  // 静态文件
  const rel = p === '/' ? 'index.html' : p.replace(/^\/+/, '')
  const file = path.join(ROOT, rel)
  if (!file.startsWith(ROOT) || !fs.existsSync(file) || fs.statSync(file).isDirectory()) {
    // 单页应用的前端路由兜底
    if (!p.startsWith('/api')) {
      const fallback = path.join(ROOT, 'ui/index.html')
      if (fs.existsSync(fallback)) return send(res, 200, fs.readFileSync(fallback), 'text/html')
    }
    return send(res, 404, 'Not Found', 'text/plain')
  }
  const ext = path.extname(file)
  const type =
    ext === '.html'
      ? 'text/html'
      : ext === '.js'
        ? 'text/javascript'
        : ext === '.css'
          ? 'text/css'
          : ext === '.svg'
            ? 'image/svg+xml'
            : 'application/octet-stream'
  return send(res, 200, fs.readFileSync(file), type)
})

server.listen(PORT, '127.0.0.1', () => {
  console.log(`[mock] http://127.0.0.1:${PORT}  static=${ROOT}`)
})
