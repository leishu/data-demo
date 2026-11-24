import { reactive } from 'vue'

const BASE = (import.meta.env.VITE_API_BASE || '').replace(/\/$/, '')
const STORAGE_KEY = 'lake.auth'

/**
 * 控制台登录态。后端仍是无状态 Basic：登录成功后把用户名/口令留在内存 +
 * sessionStorage，之后每个请求（含 SSE）都显式带 Authorization 头。
 *
 * 不依赖浏览器原生 Basic 弹框有两个原因：
 *   1. 凭据能被「退出」真正清掉（浏览器自己缓存的 Basic 凭据清不掉）；
 *   2. EventSource 不能带头，SSE 改成 fetch 流之后才能走同一套鉴权。
 */
export const auth = reactive({
  /** checking → authenticated / guest */
  status: 'checking',
  user: null,
  roles: [],
  mode: 'local',
  enabled: true,
  /** expired = 凭据失效被登出；logged-out = 主动退出 */
  reason: ''
})

/** 内存中的凭据，不参与渲染；sessionStorage 只用于刷新页面后恢复 */
let credentials = null

function basic(user, password) {
  const bytes = new TextEncoder().encode(`${user}:${password}`)
  let binary = ''
  bytes.forEach((b) => (binary += String.fromCharCode(b)))
  return 'Basic ' + btoa(binary)
}

/** api.js 用：当前账号的 Basic 头，未登录时返回 null */
export function authHeader() {
  return credentials ? basic(credentials.username, credentials.password) : null
}

function readSaved() {
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY)
    return raw ? JSON.parse(raw) : null
  } catch {
    return null
  }
}

function clearCredentials() {
  credentials = null
  try {
    sessionStorage.removeItem(STORAGE_KEY)
  } catch {
    // 隐私模式下 sessionStorage 可能不可写：内存清掉即可，退出仍然生效
  }
  auth.user = null
  auth.roles = []
}

/** 应用启动：用本地凭据问一次 /api/auth/session，确认是否还有效 */
export async function restore() {
  auth.status = 'checking'
  const saved = readSaved()
  if (saved?.username && saved?.password) {
    credentials = { username: saved.username, password: saved.password }
  }

  try {
    const headers = authHeader() ? { Authorization: authHeader() } : {}
    const res = await fetch(BASE + '/api/auth/session', { headers })
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    const data = await res.json()
    auth.mode = data.mode || 'local'
    auth.enabled = data.enabled !== false
    if (data.authenticated) {
      auth.user = data.user
      auth.roles = data.roles || []
      auth.status = 'authenticated'
      auth.reason = ''
    } else {
      clearCredentials()
      auth.status = 'guest'
    }
  } catch {
    // 认证端点不可用（后端未起或老版本）：有本地凭据就先按已登录走，
    // 让各页面自己报网络错误；没有则回到登录页。
    if (credentials) {
      auth.user = saved?.user || credentials.username
      auth.roles = saved?.roles || []
      auth.status = 'authenticated'
    } else {
      auth.status = 'guest'
    }
  }
}

/** 登录：用公开端点校验，绕开鉴权过滤器，失败不会弹浏览器原生登录框 */
export async function login(username, password) {
  const res = await fetch(BASE + '/api/auth/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username, password })
  })
  const data = await res.json().catch(() => ({}))
  if (!res.ok) {
    throw new Error(data.message || data.error || `登录失败（HTTP ${res.status}）`)
  }

  credentials = { username, password }
  auth.mode = data.mode || 'local'
  auth.user = data.user
  auth.roles = data.roles || []
  auth.status = 'authenticated'
  auth.reason = ''
  try {
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify({
      username,
      password,
      user: auth.user,
      roles: auth.roles,
      mode: auth.mode
    }))
  } catch {
    // 同上：写不进去不影响本次会话
  }
}

/** 退出：只清前端凭据（Basic 没有服务端会话），刷新页面也不会自动回来 */
export function logout() {
  clearCredentials()
  auth.status = 'guest'
  auth.reason = 'logged-out'
}

/** 任意受保护接口返回 401：凭据已失效，踢回登录页 */
export function onUnauthorized() {
  if (auth.status === 'authenticated') {
    clearCredentials()
    auth.status = 'guest'
    auth.reason = 'expired'
  }
}
