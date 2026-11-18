import { reactive } from 'vue'
import { api, fetchHealth } from './api'

export const store = reactive({
  config: null,
  configError: null,
  loadingConfig: false,
  health: { reachable: false, status: 'CHECKING', components: {} },
  healthCheckedAt: null,
  /** 从表浏览跳到查询台时带的 SQL */
  draftSql: ''
})

/** 平台配置既是配置页的数据源，也决定「作业白名单 / 表全名」这些页面细节 */
export async function loadConfig(force = false) {
  if (store.config && !force) return store.config
  if (store.loadingConfig) return store.config
  store.loadingConfig = true
  try {
    store.config = await api.config()
    store.configError = null
  } catch (e) {
    store.configError = e
  } finally {
    store.loadingConfig = false
  }
  return store.config
}

export async function refreshHealth() {
  const h = await fetchHealth()
  store.health = h
  store.healthCheckedAt = new Date()
  return h
}

/** iceberg.lake.xxx —— catalog / database 由后端配置决定，前端不硬编码 */
export function tableFqn(name) {
  const cfg = store.config
  return cfg ? `${cfg.catalog}.${cfg.database}.${name}` : name
}
