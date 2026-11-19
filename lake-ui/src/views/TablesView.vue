<script setup>
import { computed, onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { Loading } from '@element-plus/icons-vue'
import { api } from '@/api'
import { loadConfig, store, tableFqn } from '@/store'
import ResultTable from '@/components/ResultTable.vue'
import ErrorNotice from '@/components/ErrorNotice.vue'

const LIMIT_OPTIONS = [20, 50, 200]

const router = useRouter()

const tables = ref([])
const loading = ref(false)
const error = ref(null)

const selected = ref(null)
const sample = ref(null)
const sampleError = ref(null)
const sampleLoading = ref(false)
const limit = ref(50)

/** 字段名 → 中文名（建表 DDL 里的 COMMENT）；采样表的表头直接用它 */
const columnComments = computed(() => {
  const info = tables.value.find((t) => t.table === selected.value)
  const map = {}
  for (const c of info?.columns || []) if (c.comment) map[c.name] = c.comment
  return map
})

async function loadTables() {
  loading.value = true
  error.value = null
  try {
    tables.value = await api.tables()
    if (!selected.value && tables.value.length) {
      await open(tables.value[0].table)
    }
  } catch (e) {
    tables.value = []
    error.value = e
  } finally {
    loading.value = false
  }
}

async function open(table) {
  selected.value = table
  sampleError.value = null
  sample.value = null
  sampleLoading.value = true
  try {
    sample.value = await api.sample(table, Number(limit.value))
  } catch (e) {
    sampleError.value = e
  } finally {
    sampleLoading.value = false
  }
}

function onLimitChange() {
  if (selected.value) open(selected.value)
}

function openInQuery(table) {
  store.draftSql = `select *\nfrom ${tableFqn(table)}\nlimit 100`
  router.push('/query')
}

onMounted(async () => {
  await loadConfig()
  await loadTables()
})
</script>

<template>
  <el-card v-if="error">
    <template #header><span class="card-title">表清单</span></template>
    <ErrorNotice :error="error" title="读不到表清单" @retry="loadTables" />
  </el-card>

  <div v-else class="browser">
    <el-card>
      <template #header>
        <div class="card-head">
          <div class="head-left">
            <span class="card-title">表清单</span>
            <el-tag size="small" effect="light" type="info">{{ tables.length }} 张</el-tag>
          </div>
          <el-button size="small" text :loading="loading" @click="loadTables">刷新</el-button>
        </div>
      </template>

      <div v-if="loading && !tables.length" class="state-box">
        <el-icon class="is-loading loading-icon"><Loading /></el-icon>
      </div>

      <el-empty
        v-else-if="!tables.length"
        description="没有表，先到「批作业」跑一次 init / ingest"
      />

      <div v-else class="tlist">
        <div
          v-for="t in tables"
          :key="t.table"
          class="titem"
          :class="{ active: selected === t.table }"
          @click="open(t.table)"
        >
          <span class="tname mono">{{ t.table }}</span>
          <el-tag size="small" effect="light" type="info">{{ t.type }}</el-tag>
        </div>
      </div>
    </el-card>

    <el-card>
      <template #header>
        <div class="card-head">
          <span class="card-title mono">{{ selected ? tableFqn(selected) : '预览' }}</span>
          <div class="extra">
            <span class="faint note">采样</span>
            <el-select v-model="limit" size="small" class="limit-select" @change="onLimitChange">
              <el-option v-for="n in LIMIT_OPTIONS" :key="n" :label="`${n} 行`" :value="n" />
            </el-select>
            <el-button size="small" text :disabled="!selected || sampleLoading" @click="onLimitChange">
              重新采样
            </el-button>
            <el-button size="small" type="primary" :disabled="!selected" @click="openInQuery(selected)">
              在查询台打开
            </el-button>
          </div>
        </div>
      </template>

      <ErrorNotice
        v-if="sampleError"
        :error="sampleError"
        title="采样失败"
        @retry="selected && open(selected)"
      />

      <div v-else-if="sampleLoading" class="state-box">
        <el-icon class="is-loading loading-icon"><Loading /></el-icon>
        <span class="faint note">正在采样…</span>
      </div>

      <ResultTable
        v-else-if="sample"
        :result="sample"
        :comments="columnComments"
        :max-height="420"
        title="采样结果"
      />

      <el-empty v-else description="选一张表，右侧出前 N 行" />
    </el-card>
  </div>
</template>

<style scoped>
.browser {
  display: grid;
  grid-template-columns: 272px minmax(0, 1fr);
  gap: 14px;
  align-items: start;
}

.card-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
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

.extra {
  display: flex;
  align-items: center;
  gap: 8px;
}

.limit-select {
  width: 96px;
}

/* 表清单自己滚，不占满整列高度 */
.tlist {
  max-height: 62vh;
  overflow: auto;
  margin: 0 -6px;
}

.titem {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 8px;
  border-radius: 8px;
  cursor: pointer;
  transition: background 0.12s ease;
}

.titem:hover {
  background: var(--panel-2);
}

.titem.active {
  background: rgba(90, 162, 240, 0.13);
}

.tname {
  font-size: 12.5px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  min-width: 0;
}

.loading-icon {
  font-size: 18px;
  color: var(--accent);
}

.state-box {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 22px 8px;
}

@media (max-width: 900px) {
  .browser {
    grid-template-columns: minmax(0, 1fr);
  }
}
</style>
