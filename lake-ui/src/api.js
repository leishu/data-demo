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

  submitJob: (job) => request(`/api/batch/${encodeURIComponent(job)}`, { method: 'POST' }),
  runs: (limit = 20) => request(`/api/batch/runs?limit=${limit}`),
  run: (id, tail = 40) =>
    request(`/api/batch/runs/${encodeURIComponent(id)}?tail=${tail}`)
}

/**
 * 健康检查：依赖挂掉时后端返回 503，但 body 里仍有组件状态，
 * 所以不能走 request()，否则拿不到 presto 的具体状态。
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
