<script setup>
import { computed, onMounted, onUnmounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus/es/components/message/index.mjs'
import { refreshHealth, store } from '@/store'
import { fmtTime } from '@/utils'

const route = useRoute()
const router = useRouter()

const NAV = [
  { key: '/query', label: '查询台', index: '1' },
  { key: '/tables', label: '表浏览', index: '2' },
  { key: '/semantics', label: '指标', index: '3' },
  { key: '/jobs', label: '批作业', index: '4' },
  { key: '/config', label: '配置', index: '5' }
]

const activeKey = computed(() => '/' + (route.path.split('/')[1] || 'query'))

/** Element Plus 的 el-tag type 只有 success / warning / danger / info */
const serviceStatus = computed(() => (store.health.reachable ? 'success' : 'danger'))
const trinoStatus = computed(() => {
  const c = store.health.components || {}
  if (!store.health.reachable) return 'UNKNOWN'
  return c.trino ? c.trino.status : store.health.status
})
const trinoTone = computed(() => {
  switch (trinoStatus.value) {
    case 'UP':
      return 'success'
    case 'DOWN':
      return 'danger'
    case 'OUT_OF_SERVICE':
      return 'warning'
    default:
      return 'info'
  }
})

const checking = computed(() => store.health.status === 'CHECKING')

async function check() {
  await refreshHealth()
  const p = trinoStatus.value
  if (!store.health.reachable) ElMessage.error('服务接口没响应')
  else if (p === 'UP') ElMessage.success('依赖正常：Trino UP')
  else ElMessage.warning(`服务在线，但 Trino 是 ${p}（起 coordinator 后可用查询）`)
}

let timer = null

/** 后台标签页不刷健康检查：多开页面时别把 select 1 放大成 N 倍 */
function onVisibility() {
  if (!document.hidden) refreshHealth()
}

onMounted(() => {
  refreshHealth()
  timer = setInterval(() => {
    if (!document.hidden) refreshHealth()
  }, 30000)
  document.addEventListener('visibilitychange', onVisibility)
})
onUnmounted(() => {
  timer && clearInterval(timer)
  document.removeEventListener('visibilitychange', onVisibility)
})
</script>

<template>
  <div class="app">
    <aside class="sidebar">
      <div class="brand">
        <span class="brand-mark" aria-hidden="true">
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6"
               stroke-linecap="round" stroke-linejoin="round">
            <path d="M12 3 3 7.5l9 4.5 9-4.5L12 3Z" />
            <path d="m3 12.5 9 4.5 9-4.5" />
            <path d="m3 17 9 4.5L21 17" />
          </svg>
        </span>
        <span class="brand-text">
          <strong>Lakehouse</strong>
          <span>console</span>
        </span>
      </div>

      <el-menu :default-active="activeKey" router>
        <el-menu-item v-for="item in NAV" :key="item.key" :index="item.key">
          <span class="nav-index">{{ item.index }}</span>
          <span>{{ item.label }}</span>
        </el-menu-item>
      </el-menu>

      <div class="sidebar-foot">
        <div>Spark · Iceberg · Trino</div>
        <code>Element Plus · Spring Boot 4</code>
      </div>
    </aside>

    <div class="main">
      <header class="topbar">
        <div>
          <h1>{{ route.meta.title }}</h1>
          <p>{{ route.meta.subtitle }}</p>
        </div>
        <div class="topbar-right">
          <el-tag size="small" round effect="light" :type="serviceStatus">
            <span class="dot" />
            服务 {{ store.health.reachable ? 'UP' : 'DOWN' }}
          </el-tag>
          <el-tooltip
            content="来自 /actuator/health 的 trino 组件状态；DOWN 时查询接口返回 503"
            placement="bottom"
            :show-after="120"
          >
            <el-tag size="small" round effect="light" :type="trinoTone">
              <span class="dot" />
              Trino {{ trinoStatus }}
            </el-tag>
          </el-tooltip>
          <el-button size="small" text :loading="checking" @click="check">刷新</el-button>
          <span class="faint ts">{{ store.healthCheckedAt ? fmtTime(store.healthCheckedAt) : '' }}</span>
        </div>
      </header>

      <main class="view">
        <RouterView />
      </main>
    </div>
  </div>
</template>

<style scoped>
.nav-index {
  font-family: var(--mono);
  font-size: 11px;
  color: var(--faint);
  width: 12px;
  margin-right: 8px;
  display: inline-block;
  text-align: center;
}

:deep(.el-menu-item.is-active) .nav-index {
  color: var(--accent);
}

/* 服务状态点：跟着 tag 的文字色走，比塞一个图标组件轻 */
.dot {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: currentColor;
  display: inline-block;
  margin-right: 5px;
  vertical-align: 1px;
}

.ts {
  font-size: 11px;
  min-width: 52px;
  text-align: right;
}
</style>
