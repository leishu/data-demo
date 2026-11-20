<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import { ElMessage } from 'element-plus/es/components/message/index.mjs'
import { api } from '@/api'
import ResultTable from '@/components/ResultTable.vue'
import ErrorNotice from '@/components/ErrorNotice.vue'

const metrics = ref([])
const metricName = ref('')
const dimensions = ref([])
const filters = ref([])
const result = ref(null)
const error = ref(null)
const running = ref(false)
const sqlText = ref('')
const engine = ref('trino')

const metric = computed(() => metrics.value.find((m) => m.name === metricName.value) || null)
const payload = computed(() => ({
  metric: metricName.value,
  dimensions: dimensions.value,
  filters: filters.value
    .filter((f) => f.value && String(f.value).trim())
    .map((f) => ({ field: f.field, op: f.op, value: String(f.value).trim() })),
  limit: 500
}))

const OPS = [
  { label: '=', value: 'eq' },
  { label: '≠', value: 'neq' },
  { label: '≥', value: 'gte' },
  { label: '>', value: 'gt' },
  { label: '≤', value: 'lte' },
  { label: '<', value: 'lt' }
]

onMounted(async () => {
  try {
    metrics.value = await api.semantics()
    if (metrics.value.length) metricName.value = metrics.value[0].name
  } catch (e) {
    error.value = e
  }
})

watch(metricName, () => {
  dimensions.value = []
  filters.value = []
  sqlText.value = ''
  result.value = null
})

function addFilter() {
  if (!metric.value) return
  filters.value.push({ field: metric.value.dimensions[0], op: 'eq', value: '' })
}

async function run() {
  if (!metricName.value || running.value) return
  running.value = true
  error.value = null
  try {
    result.value = await api.semanticQuery(payload.value)
  } catch (e) {
    result.value = null
    error.value = e
    if (e.status !== 503) ElMessage.error(e.message || '查询失败')
  } finally {
    running.value = false
  }
}

async function preview() {
  if (!metricName.value) return
  try {
    const res = await api.semanticRender({ ...payload.value, engine: engine.value })
    sqlText.value = res.sql
  } catch (e) {
    error.value = e
  }
}
</script>

<template>
  <el-card>
    <template #header>
      <div class="head">
        <span class="title">指标定义</span>
        <span class="faint">口径由服务端统一维护，维度与过滤受白名单约束</span>
      </div>
    </template>

    <el-form label-width="76px" size="small">
      <el-form-item label="指标">
        <el-select v-model="metricName" style="width: 260px">
          <el-option v-for="m in metrics" :key="m.name" :label="m.label" :value="m.name" />
        </el-select>
        <span v-if="metric" class="faint desc">{{ metric.description }} · <code>{{ metric.measure }}</code></span>
      </el-form-item>

      <el-form-item label="维度">
        <el-checkbox-group v-model="dimensions">
          <el-checkbox v-for="d in metric?.dimensions || []" :key="d" :value="d" :label="d" />
        </el-checkbox-group>
      </el-form-item>

      <el-form-item label="过滤">
        <div class="filters">
          <div v-for="(f, i) in filters" :key="i" class="filter-row">
            <el-select v-model="f.field" style="width: 140px">
              <el-option v-for="d in metric?.dimensions || []" :key="d" :label="d" :value="d" />
            </el-select>
            <el-select v-model="f.op" style="width: 84px">
              <el-option v-for="o in OPS" :key="o.value" :label="o.label" :value="o.value" />
            </el-select>
            <el-input v-model="f.value" placeholder="值（dt 用 yyyy-MM-dd）" style="width: 220px" />
            <el-button text @click="filters.splice(i, 1)">删除</el-button>
          </div>
          <el-button text size="small" @click="addFilter">+ 加条件</el-button>
        </div>
      </el-form-item>

      <el-form-item>
        <el-button type="primary" :loading="running" @click="run">执行指标查询</el-button>
        <el-select v-model="engine" size="small" style="width: 104px; margin-left: 8px">
          <el-option label="Trino SQL" value="trino" />
          <el-option label="Spark SQL" value="spark" />
        </el-select>
        <el-button size="small" @click="preview">预览 SQL</el-button>
      </el-form-item>
    </el-form>
  </el-card>

  <ErrorNotice v-if="error" :error="error" title="指标查询失败" @retry="run" />

  <el-card v-if="sqlText">
    <template #header><span class="title">生成的 SQL（{{ engine }} 方言）</span></template>
    <pre class="sql">{{ sqlText }}</pre>
  </el-card>

  <el-card>
    <template #header>
      <div class="head">
        <span class="title">结果</span>
        <span v-if="result" class="faint">{{ result.rowCount }} 行 · {{ result.elapsedMs }} ms</span>
      </div>
    </template>
    <ResultTable v-if="result" :result="result" :max-height="420" />
    <el-empty v-else description="选好指标与维度，点「执行指标查询」" />
  </el-card>
</template>

<style scoped>
.head { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }
.title { font-size: 13px; font-weight: 600; }
.faint { font-size: 11.5px; color: var(--faint); }
.desc { margin-left: 12px; }
.filters { display: flex; flex-direction: column; gap: 6px; }
.filter-row { display: flex; align-items: center; gap: 8px; }
.sql {
  margin: 0; padding: 12px; border-radius: 8px; border: 1px solid var(--border);
  background: #0c1118; color: #cfe0f5; font: 11.5px/1.65 var(--mono); overflow: auto;
}
</style>
