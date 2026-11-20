export function fmtDuration(ms) {
  if (ms === null || ms === undefined) return '—'
  if (ms < 1000) return `${ms} ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(1)} s`
  const m = Math.floor(ms / 60000)
  const s = Math.round((ms % 60000) / 1000)
  return `${m}m ${s}s`
}

export function fmtTime(iso) {
  if (!iso) return '—'
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return String(iso)
  const pad = (n) => String(n).padStart(2, '0')
  return `${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
}

const NUMERIC = /^(bigint|integer|int|smallint|tinyint|double|real|decimal|numeric|float)/i
export function isNumericType(type) {
  return NUMERIC.test(type || '')
}

/** 后端状态（RUNNING/SUCCEEDED/FAILED/TIMEOUT）映射到页面色调 */
export function statusTone(status) {
  switch (status) {
    case 'SUCCEEDED':
    case 'UP':
      return 'ok'
    case 'QUEUED':
    case 'RUNNING':
    case 'CHECKING':
      return 'warn'
    case 'FAILED':
    case 'TIMEOUT':
    case 'DOWN':
      return 'err'
    default:
      return 'muted'
  }
}

export function fmtJobStatus(status) {
  const map = {
    QUEUED: '排队中',
    RUNNING: '运行中',
    SUCCEEDED: '成功',
    FAILED: '失败',
    TIMEOUT: '超时'
  }
  return map[status] || status || '—'
}

/** statusTone() 的语义色 → Element Plus 的 el-tag type（error 叫 danger，默认叫 info） */
export function tagType(tone) {
  return { ok: 'success', warn: 'warning', err: 'danger' }[tone] || 'info'
}

export function copyText(text) {
  if (navigator.clipboard && window.isSecureContext) {
    return navigator.clipboard.writeText(text)
  }
  // 非 https / localhost 场景下 clipboard API 不可用，退回到 textarea
  return new Promise((resolve, reject) => {
    const ta = document.createElement('textarea')
    ta.value = text
    ta.style.position = 'fixed'
    ta.style.opacity = '0'
    document.body.appendChild(ta)
    ta.select()
    try {
      document.execCommand('copy') ? resolve() : reject(new Error('复制失败'))
    } finally {
      document.body.removeChild(ta)
    }
  })
}
