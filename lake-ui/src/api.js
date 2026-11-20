const BASE = (import.meta.env.VITE_API_BASE || '').replace(/\/$/, '')

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

async function request(path, { method = 'GET', body } = {}) {
  let res
  try {
    res = await fetch(BASE + path, {
      method,
      headers: body === undefined ? undefined : { 'Content-Type': 'application/json' },
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
    const msg = (data && (data.message || data.error)) || text || res.statusText
    throw new ApiError(msg, {
      status: res.status,
      hint: (data && data.hint) || '',
      body: data
    })
  }
  return data === null ? text : data
}

export const api = {
  query: (sql, maxRows) => request('/api/query', { method: 'POST', body: { sql, maxRows } }),
  tables: () => request('/api/tables'),
  sample: (table, limit = 50) =>
    request(`/api/tables/${encodeURIComponent(table)}/sample?limit=${limit}`),
  config: () => request('/api/config'),
  semantics: () => request('/api/semantics'),
  semanticRender: (payload) => request('/api/semantics/render', { method: 'POST', body: payload }),
  semanticQuery: (payload) => request('/api/semantics/query', { method: 'POST', body: payload }),

  submitJob: (job) => request(`/api/batch/${encodeURIComponent(job)}`, { method: 'POST' }),
  runs: (limit = 20) => request(`/api/batch/runs?limit=${limit}`),
  run: (id, tail = 40) =>
    request(`/api/batch/runs/${encodeURIComponent(id)}?tail=${tail}`)
}

/**
 * 查询流（SSE）：meta → rows… → done，失败则 error。
 *
 * 走 POST 而不是 EventSource：EventSource 只支持 GET，把 SQL 塞进 query string 会被
 * 访问日志完整记下来、也受 URL 长度限制。所以用 fetch + ReadableStream 自己解 SSE 帧。
 */
export async function streamQuery(sql, maxRows, { onEvent, signal } = {}) {
  let res
  try {
    res = await fetch(BASE + '/api/query/stream', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Accept: 'text/event-stream' },
      body: JSON.stringify({ sql, maxRows }),
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
 * EventSource 断线后会自己重连，而作业结束后那条连接重连毫无意义，
 * 所以收到 end 或出错就显式 close()，交给调用方决定要不要重连。
 */
export function watchRun(id, { tail = 80, onStatus, onLog, onEnd, onError } = {}) {
  const es = new EventSource(`${BASE}/api/batch/runs/${encodeURIComponent(id)}/events?tail=${tail}`)
  let closed = false
  const close = () => {
    if (!closed) {
      closed = true
      es.close()
    }
  }

  es.addEventListener('status', (e) => onStatus?.(JSON.parse(e.data).run))
  es.addEventListener('log', (e) => onLog?.(JSON.parse(e.data).lines || []))
  es.addEventListener('end', (e) => {
    const run = JSON.parse(e.data).run
    close()
    onEnd?.(run)
  })
  es.addEventListener('error', () => {
    if (closed) return
    close()
    onError?.()
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
