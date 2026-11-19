<script setup>
import { computed, onMounted, ref } from 'vue'
import { useRoute } from 'vue-router'
import { ElMessage } from 'element-plus/es/components/message/index.mjs'
import { CaretRight, Loading } from '@element-plus/icons-vue'
import { api } from '@/api'
import { loadConfig, store, tableFqn } from '@/store'
import ResultTable from '@/components/ResultTable.vue'
import CopyButton from '@/components/CopyButton.vue'
import ErrorNotice from '@/components/ErrorNotice.vue'

const STORAGE_KEY = 'lake.console.sql'

/**
 * catalog / database 以后端 /api/config 为准；配置还没拉到时退回默认目录，
 * 至少保证示例 SQL 语法完整（真连不上时后端也会给出明确错误）。
 */
function qualified(name) {
  return store.config ? tableFqn(name) : `iceberg.lake.${name}`
}

function defaultSql() {
  return `select dt,
       sum(order_cnt)     as orders,
       sum(gmv)           as gmv,
       sum(refund_amount) as refunded,
       sum(net_revenue)   as net_revenue
from ${qualified('gold_daily_revenue')}
group by dt
order by dt desc`
}

const EXAMPLES = computed(() => [
  {
    label: '每日营收',
    sql: `select dt, sum(order_cnt) as orders, sum(gmv) as gmv, sum(net_revenue) as net_revenue
from ${qualified('gold_daily_revenue')}
group by dt
order by dt desc`
  },
  {
    label: '商户 Top 10',
    sql: `select merchant_id, sum(gmv) as gmv, sum(order_cnt) as orders
from ${qualified('gold_daily_revenue')}
group by merchant_id
order by gmv desc
limit 10`
  },
  {
    label: '原始订单采样',
    sql: `select order_id, merchant_id, category, amount, status, event_time
from ${qualified('silver_orders')}
order by event_time desc
limit 50`
  }
])

const placeholder = computed(
  () => `select * from ${qualified('gold_daily_revenue')} limit 20`
)

const ROW_OPTIONS = [
  { label: '100 行', value: 100 },
  { label: '500 行', value: 500 },
  { label: '1000 行', value: 1000 }
]

const route = useRoute()

// 优先级：URL 里带的 sql（可分享） > 表浏览跳转带过来的 > 上次编辑的 > 默认示例
const sql = ref(
  (route.query.sql && String(route.query.sql)) ||
    store.draftSql ||
    localStorage.getItem(STORAGE_KEY) ||
    ''
)
const maxRows = ref(500)
const result = ref(null)
const error = ref(null)
const running = ref(false)
const statsOpen = ref(false)

const statsText = computed(() => (result.value && result.value.stats) || '')

onMounted(async () => {
  // 从表浏览带过来的 SQL 只用一次，之后以编辑器内容为准
  store.draftSql = ''
  // 示例和默认 SQL 都要用后端的 catalog / database，所以无论有没有历史 SQL 都先拉配置
  await loadConfig()
  if (!sql.value) sql.value = defaultSql()
  if (route.query.run === '1') run()
})

async function run() {
  if (running.value) return
  const text = sql.value.trim()
  if (!text) {
    error.value = { message: 'SQL 是空的', status: 0, hint: '先写一条 select，或点下面的示例' }
    return
  }
  running.value = true
  error.value = null
  statsOpen.value = false
  try {
    result.value = await api.query(text, Number(maxRows.value))
    localStorage.setItem(STORAGE_KEY, sql.value)
    store.draftSql = sql.value
    if (result.value.truncated) {
      ElMessage.warning(`结果已按 ${result.value.rowCount} 行截断，加 where 或调大上限`)
    }
  } catch (e) {
    result.value = null
    error.value = e
    // 503 是「依赖没起」，属于环境问题，不弹红条，页面上已经有说明
    if (e.status !== 503) ElMessage.error(e.message || '查询失败')
  } finally {
    running.value = false
  }
}

function applyExample(ex) {
  sql.value = ex.sql
  run()
}

function onKeydown(e) {
  if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
    e.preventDefault()
    run()
  }
}
</script>

<template>
  <el-card>
    <template #header>
      <div class="card-head">
        <div class="head-left">
          <span class="card-title">SQL 编辑器</span>
          <el-tag size="small" effect="light">只读</el-tag>
        </div>
        <span class="faint note">单条语句 · 服务端超时 60s</span>
      </div>
    </template>

    <div class="editor-wrap" @keydown.capture="onKeydown">
      <el-input
        v-model="sql"
        type="textarea"
        class="sql-input"
        :placeholder="placeholder"
        :autosize="{ minRows: 9, maxRows: 18 }"
        resize="vertical"
        spellcheck="false"
      />
    </div>

    <div class="toolbar">
      <el-button type="primary" size="small" :loading="running" @click="run">
        <el-icon v-if="!running"><CaretRight /></el-icon>
        {{ running ? '执行中' : '执行' }}
      </el-button>
      <span class="kbd-hint"><kbd>Ctrl</kbd><kbd>↵</kbd></span>
      <span class="spacer"></span>
      <span class="faint note">返回上限</span>
      <el-select v-model="maxRows" size="small" class="row-select">
        <el-option v-for="o in ROW_OPTIONS" :key="o.value" :label="o.label" :value="o.value" />
      </el-select>
      <el-button size="small" text @click="sql = ''">清空</el-button>
    </div>
  </el-card>

  <el-card>
    <template #header>
      <div class="card-head">
        <span class="card-title">结果</span>
        <div class="result-extra">
          <template v-if="result && !error">
            <span class="faint meta">{{ result.rowCount }} 行 · {{ result.elapsedMs }} ms</span>
            <el-tag v-if="result.truncated" size="small" type="warning" effect="light">已截断</el-tag>
            <CopyButton :result="result" />
          </template>
          <el-button v-if="statsText" size="small" text @click="statsOpen = !statsOpen">
            {{ statsOpen ? '收起 QueryStats' : 'QueryStats' }}
          </el-button>
        </div>
      </div>
    </template>

    <ErrorNotice
      v-if="error"
      :error="error"
      :title="error.status === 503 ? 'Trino 不可用' : 'SQL 执行失败'"
      @retry="run"
    />

    <div v-else-if="running" class="state-box">
      <el-icon class="is-loading loading-icon"><Loading /></el-icon>
      <div class="state-text">
        <strong>正在执行</strong>
        <span class="faint">查询会走 Trino coordinator，大表慢查询请自行加分区条件</span>
      </div>
    </div>

    <template v-else-if="result">
      <ResultTable :result="result" :max-height="420" :show-meta="false" />
      <pre v-if="statsOpen && statsText" class="raw">{{ statsText }}</pre>
    </template>

    <el-empty v-else description="还没有结果，写一条 SQL 或选个示例">
      <div class="chip-row">
        <el-button
          v-for="ex in EXAMPLES"
          :key="ex.label"
          size="small"
          round
          @click="applyExample(ex)"
        >
          {{ ex.label }}
        </el-button>
      </div>
    </el-empty>
  </el-card>
</template>

<style scoped>
.card-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
}

.head-left {
  display: flex;
  align-items: center;
  gap: 8px;
}

.card-title {
  font-size: 13px;
  font-weight: 600;
}

.note {
  font-size: 11.5px;
}

.meta {
  font-size: 11.5px;
}

.result-extra {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.editor-wrap {
  min-width: 0;
}

/* 编辑器：等宽字体 + 代码底，边框/聚焦环手写（el-textarea 默认用 box-shadow 当边框） */
.sql-input :deep(.el-textarea__inner) {
  font: 13px/1.7 var(--mono);
  tab-size: 2;
  color: #dbe4f0;
  background: #0c1118;
  border-radius: 8px;
  padding: 10px 12px;
  box-shadow: 0 0 0 1px var(--border) inset;
}

.sql-input :deep(.el-textarea__inner:focus) {
  box-shadow: 0 0 0 1px var(--accent) inset, 0 0 0 3px rgba(90, 162, 240, 0.14);
}

.sql-input :deep(.el-textarea__inner::placeholder) {
  color: var(--faint);
}

/* 把工具条压到卡片底边，省掉 el-card 没有 footer 插槽的麻烦 */
.toolbar {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  margin: 12px -14px -14px;
  padding: 10px 14px;
  border-top: 1px solid var(--border-soft);
}

.row-select {
  width: 104px;
}

.spacer {
  margin-left: auto;
}

.kbd-hint {
  display: inline-flex;
  gap: 3px;
}

.kbd-hint kbd {
  font-family: var(--mono);
  font-size: 10.5px;
  padding: 1px 5px;
  border: 1px solid var(--border);
  border-bottom-width: 2px;
  border-radius: 4px;
  color: var(--muted);
  background: var(--panel-2);
}

.loading-icon {
  font-size: 18px;
  color: var(--accent);
}

.state-box {
  display: flex;
  align-items: center;
  gap: 14px;
  padding: 28px 8px;
}

.state-text {
  display: flex;
  flex-direction: column;
  gap: 2px;
  font-size: 13px;
}

.state-text span {
  font-size: 12px;
}

.chip-row {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  justify-content: center;
}

.raw {
  margin: 10px 0 0;
  padding: 12px;
  border-radius: 8px;
  border: 1px solid var(--border);
  background: #0c1118;
  color: #cfe0f5;
  font: 11.5px/1.65 var(--mono);
  overflow: auto;
  max-height: 240px;
}
</style>
