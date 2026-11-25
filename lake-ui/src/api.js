import { authHeader, onUnauthorized } from './auth'

const BASE = (import.meta.env.VITE_API_BASE || '').replace(/\/$/, '')

/** 受保护接口统一带 Basic 头；登录/健康/文档这类公开端点用不到 */
function authHeaders(extra = {}) {
  const headers = { ...extra }
  const auth = authHeader()
  if (auth) headers.Authorization = auth
  return headers
}

/** 后端统一错误体 {status, error, message, hint}，这里原样带出来给页面用 */
export class ApiError extends Error {
  constructor(message, { status = 0, hint = '', body = null } = {}) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.hint = hint
    this.body = body
  }
}

async function request(path, { method = 'GET', body, headers } = {}) {
  let res
  try {
    const finalHeaders = authHeaders(headers || {})
    if (body !== undefined) finalHeaders['Content-Type'] = 'application/json'
    res = await fetch(BASE + path, {
      method,
      headers: Object.keys(finalHeaders).length ? finalHeaders : undefined,
      body: body === undefined ? undefined : JSON.stringify(body)
    })
  } catch (e) {
    throw new ApiError('请求发不出去：' + e.message, {
      status: 0,
      hint: '确认 lake-service 还在运行，以及页面和接口同源'
    })
  }

  const text = await res.text()
  let data = null
  if (text) {
    try {
      data = JSON.parse(text)
    } catch {
      data = null
    }
  }

  if (!res.ok) {
    if (res.status === 401) onUnauthorized()
    const msg = (data && (data.message || data.error)) || text || res.statusText
    throw new ApiError(msg, {
      status: res.status,
      hint: (data && data.hint) || '',
      body: data
    })
  }
  return data === null ? text : data
}

/**
 * 幂等键：后端只接受 [A-Za-z0-9._:-]，UUID 的连字符合法；
 * crypto.randomUUID 需要安全上下文（https / localhost），非安全上下文用时间戳+随机数兜底。
 */
function newIdempotencyKey() {
  const uuid = globalThis.crypto?.randomUUID?.()
  return uuid
    ? `ui-${uuid}`
    : `ui-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`
}

/**
 * 同一作业提交失败（含响应丢失）时重试复用上一个 key，让后端返回原运行记录；
 * 提交成功后清掉，下次点击才算一次新作业。
 */
const pendingSubmitKeys = new Map()

async function submitJob(job) {
  let key = pendingSubmitKeys.get(job)
  if (!key) {
    key = newIdempotencyKey()
    pendingSubmitKeys.set(job, key)
  }
  try {
    const run = await request(`/api/batch/${encodeURIComponent(job)}`, {
      method: 'POST',
      headers: { 'Idempotency-Key': key }
    })
    pendingSubmitKeys.delete(job)
    return run
  } catch (e) {
    // 保留 key：请求可能已到达服务端并落库，重试必须命中同一条 run
    throw e
  }
}

export const api = {
  submitJob,
  query: (sql, maxRows) => request('/api/query', { method: 'POST', body: { sql, maxRows } }),
  tables: () => request('/api/tables'),
  governanceAssets: () => request('/api/governance/assets'),
  dqResults: (limit = 50) => request(`/api/governance/dq?limit=${limit}`),
  lineage: (table, limit = 50) =>
    request(`/api/governance/lineage?table=${encodeURIComponent(table)}&limit=${limit}`),
  retentionAudit: (limit = 50) => request(`/api/governance/retention?limit=${limit}`),
  sample: (table, limit = 50) =>
    request(`/api/tables/${encodeURIComponent(table)}/sample?limit=${limit}`),
  config: () => request('/api/config'),
  semantics: () => request('/api/semantics'),
  semanticRender: (payload) => request('/api/semantics/render', { method: 'POST', body: payload }),
  semanticQuery: (payload) => request('/api/semantics/query', { method: 'POST', body: payload }),

  runs: (limit = 20) => request(`/api/batch/runs?limit=${limit}`),
  run: (id, tail = 40) =>
    request(`/api/batch/runs/${encodeURIComponent(id)}?tail=${tail}`),

  /** 最近审计事件（列表 QUERY / AUDITOR 均可；原文调阅/保全另需 AUDITOR） */
  audit: (limit = 100) => request(`/api/audit?limit=${limit}`),
  auditText: (eventId, reason = 'compliance-review') =>
    request(`/api/audit/sql/${encodeURIComponent(eventId)}?reason=${encodeURIComponent(reason)}`),
  auditHold: (eventId, hold) =>
    request(`/api/audit/sql/${encodeURIComponent(eventId)}/hold?hold=${hold}`, { method: 'POST' })
}

/**
 * 统一的 SSE 读取器：查询流与作业监控流共用。
 *
 * 不用 EventSource 的两个原因：SQL 走 POST 不能塞进 query string；而且
 * EventSource 带不了 Authorization 头，显式登录之后就没法鉴权。
 */
async function openSse(path, { body, signal, onEvent } = {}) {
  let res
  try {
    const headers = authHeaders({ Accept: 'text/event-stream' })
    const isPost = body !== undefined
    if (isPost) headers['Content-Type'] = 'application/json'
    res = await fetch(BASE + path, {
      method: isPost ? 'POST' : 'GET',
      headers,
      body: isPost ? JSON.stringify(body) : undefined,
      signal
    })
  } catch (e) {
    if (e.name === 'AbortError') throw e
    throw new ApiError('请求发不出去：' + e.message, {
      status: 0,
      hint: '确认 lake-service 还在运行，以及页面和接口同源'
    })
  }

  if (!res.ok) {
    if (res.status === 401) onUnauthorized()
    // 流没开起来时后端返回的还是统一错误体（JSON）
    const text = await res.text()
    let data = null
    try {
      data = JSON.parse(text)
    } catch {
      data = null
    }
    throw new ApiError((data && (data.message || data.error)) || text || res.statusText, {
      status: res.status,
      hint: (data && data.hint) || '',
      body: data
    })
  }

  const reader = res.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''
  for (;;) {
    const { value, done } = await reader.read()
    if (done) break
    buffer += decoder.decode(value, { stream: true }).replace(/\r\n/g, '\n')
    let cut = buffer.indexOf('\n\n')
    while (cut >= 0) {
      const frame = buffer.slice(0, cut)
      buffer = buffer.slice(cut + 2)
      const ev = parseFrame(frame)
      if (ev) onEvent?.(ev)
      cut = buffer.indexOf('\n\n')
    }
  }
}

/** 查询流（SSE）：meta → rows… → done，失败则 error */
export function streamQuery(sql, maxRows, { onEvent, signal } = {}) {
  return openSse('/api/query/stream', { body: { sql, maxRows }, onEvent, signal })
}

/** SSE 帧：`event:rows` + `data:{...}`；冒号后的空格可有可无，注释帧（心跳）直接忽略 */
function parseFrame(frame) {
  let event = 'message'
  const data = []
  for (const line of frame.split('\n')) {
    if (!line || line.startsWith(':')) continue
    const sep = line.indexOf(':')
    const field = sep < 0 ? line : line.slice(0, sep)
    let value = sep < 0 ? '' : line.slice(sep + 1)
    if (value.startsWith(' ')) value = value.slice(1)
    if (field === 'event') event = value
    else if (field === 'data') data.push(value)
  }
  if (!data.length) return null
  const raw = data.join('\n')
  let payload = raw
  try {
    payload = JSON.parse(raw)
  } catch {
    payload = raw
  }
  return { event, data: payload }
}

/**
 * 作业监控流：status / log / ping / end。
 *
 * tail 是首帧回放的历史日志行数：服务端只从文件末尾往前读这么多，不从文件头回放。
 * 改为 fetch 流之后没有 EventSource 的自动重连：收到 end 或断流就 close()，
 * 重连交给调用方（JobsView 已有退避逻辑），这样作业结束后不会白建连接。
 */
export function watchRun(id, { tail = 80, onStatus, onLog, onEnd, onError } = {}) {
  const controller = new AbortController()
  let closed = false
  const close = () => {
    if (!closed) {
      closed = true
      controller.abort()
    }
  }

  openSse(`/api/batch/runs/${encodeURIComponent(id)}/events?tail=${tail}`, {
    signal: controller.signal,
    onEvent({ event, data }) {
      if (closed) return
      if (event === 'status') onStatus?.(data.run)
      else if (event === 'log') onLog?.(data.lines || [])
      else if (event === 'end') {
        close()
        onEnd?.(data.run)
      }
    }
  })
    .catch((e) => {
      if (closed) return
      close()
      onError?.(e)
    })
    .then(() => {
      // 服务端没发 end 就关流：按断线处理，交给调用方决定是否重连
      if (!closed) {
        close()
        onError?.()
      }
    })

  return { close }
}

/**
 * 健康检查：依赖挂掉时后端返回 503，但 body 里仍有组件状态，
 * 所以不能走 request()，否则拿不到 trino 的具体状态。
 */
export async function fetchHealth() {
  try {
    const res = await fetch(BASE + '/actuator/health', {
      headers: { Accept: 'application/json' }
    })
    const body = await res.json().catch(() => ({}))
    return {
      reachable: true,
      status: body.status || 'UNKNOWN',
      components: body.components || {}
    }
  } catch {
    return { reachable: false, status: 'DOWN', components: {} }
  }
}
