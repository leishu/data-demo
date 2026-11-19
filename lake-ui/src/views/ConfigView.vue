<script setup>
import { computed, onMounted, ref } from 'vue'
import { Loading } from '@element-plus/icons-vue'
import { loadConfig, store } from '@/store'
import ErrorNotice from '@/components/ErrorNotice.vue'

const rawOpen = ref(false)
const loading = ref(false)

const cfg = computed(() => store.config)

/** 兜底格式化：数组里是对象时取 name，绝不让 [object Object] 露到页面上 */
function fmtValue(v) {
  if (v === null || v === undefined) return '—'
  if (Array.isArray(v)) {
    if (!v.length) return '—'
    return v
      .map((item) =>
        item && typeof item === 'object' ? (item.name ?? JSON.stringify(item)) : String(item)
      )
      .join(', ')
  }
  if (typeof v === 'object') return JSON.stringify(v)
  return String(v)
}

function entries(obj) {
  if (!obj || typeof obj !== 'object') return []
  return Object.entries(obj).map(([k, v]) => [k, fmtValue(v)])
}

/**
 * 后端 /api/config 里的 platform 是一句 "catalog=iceberg(type=hadoop) database=lake ..."
 * 的摘要字符串（PlatformConfig.describe()），不是对象；这里拆成 k=v 更好读，
 * 万一以后改成对象也兼容。
 */
const platformEntries = computed(() => {
  const p = cfg.value?.platform
  if (!p) return []
  if (typeof p !== 'string') return entries(p)
  return p
    .split(/\s+/)
    .filter(Boolean)
    .map((token) => {
      const i = token.indexOf('=')
      const value = i < 0 ? '' : token.slice(i + 1)
      return [i < 0 ? token : token.slice(0, i), value === '-' || value === '' ? '—' : value]
    })
})

// jobs 是对象数组，内容用下面的作业表格渲染，不塞进键值对
const batchEntries = computed(() =>
  entries({ ...(cfg.value?.batch || {}) }).filter(([k]) => k !== 'enabled' && k !== 'jobs')
)

const jobCards = computed(() => cfg.value?.batch?.jobs || [])
const allowedCount = computed(() => (cfg.value?.batch?.allowedJobs || []).length)

/** 白名单里写了、但 LakeJobs 里没有的名字：拼出来的命令必定报「未知 job」，配置页要露出来 */
const unknownJobs = computed(() => {
  const known = new Set(jobCards.value.map((j) => j.name))
  return (cfg.value?.batch?.allowedJobs || []).filter((name) => !known.has(name))
})

async function reload() {
  loading.value = true
  await loadConfig(true)
  loading.value = false
}

onMounted(reload)
</script>

<template>
  <el-card v-if="store.configError">
    <template #header><span class="card-title">配置</span></template>
    <ErrorNotice :error="store.configError" title="读不到配置" @retry="reload" />
  </el-card>

  <div v-else-if="loading && !cfg" class="state-box">
    <el-icon class="is-loading loading-icon"><Loading /></el-icon>
    <span class="faint note">正在读取配置…</span>
  </div>

  <template v-else-if="cfg">
    <div class="grid-2">
      <el-card>
        <template #header><span class="card-title">存储与 catalog</span></template>
        <el-descriptions :column="1" size="small" class="kv">
          <el-descriptions-item label="catalog">
            <span class="mono">{{ cfg.catalog }}</span>
          </el-descriptions-item>
          <el-descriptions-item label="database">
            <span class="mono">{{ cfg.database }}</span>
          </el-descriptions-item>
          <el-descriptions-item label="warehouse">
            <span class="mono wrap">{{ cfg.warehouse }}</span>
          </el-descriptions-item>
          <el-descriptions-item label="写模式">
            <el-tag size="small" effect="light" type="primary">{{ cfg.writeMode }}</el-tag>
            <span v-if="cfg.writeMode === 'copy-on-write'" class="faint note block">
              Presto 读 MoR 要处理 delete file，CoW 更稳
            </span>
          </el-descriptions-item>
        </el-descriptions>
      </el-card>

      <el-card>
        <template #header><span class="card-title">Presto 连接</span></template>
        <el-descriptions :column="1" size="small" class="kv">
          <el-descriptions-item label="JDBC URL">
            <span class="mono wrap">{{ cfg.prestoUrl }}</span>
          </el-descriptions-item>
          <el-descriptions-item label="连接池">{{ cfg.query?.poolSize }} 个连接</el-descriptions-item>
          <el-descriptions-item label="单次上限">{{ cfg.query?.maxRows }} 行</el-descriptions-item>
          <el-descriptions-item label="查询超时">{{ cfg.query?.timeoutSeconds }} s</el-descriptions-item>
          <el-descriptions-item label="允许写">
            <el-tag size="small" effect="light" :type="cfg.query?.allowWrites ? 'warning' : 'success'">
              {{ cfg.query?.allowWrites ? 'true（SQL 守卫放开）' : 'false（只读）' }}
            </el-tag>
          </el-descriptions-item>
        </el-descriptions>
      </el-card>
    </div>

    <el-card>
      <template #header>
        <div class="card-head">
          <div class="head-left">
            <span class="card-title">平台配置</span>
            <el-tag size="small" effect="light" type="info">platform-common 归一化结果</el-tag>
          </div>
          <el-button size="small" text :loading="loading" @click="reload">重新读取</el-button>
        </div>
      </template>
      <el-descriptions :column="1" size="small" class="kv">
        <el-descriptions-item v-for="[k, v] in platformEntries" :key="k" :label="k">
          <span class="mono wrap">{{ v }}</span>
        </el-descriptions-item>
      </el-descriptions>
    </el-card>

    <el-card>
      <template #header>
        <div class="card-head">
          <div class="head-left">
            <span class="card-title">作业编排</span>
            <el-tag size="small" effect="light" type="info">命令由模板拼接，作业名必须命中白名单</el-tag>
          </div>
        </div>
      </template>
      <el-descriptions :column="1" size="small" class="kv">
        <el-descriptions-item label="启用">
          <el-tag size="small" effect="light" :type="cfg.batch?.enabled ? 'success' : 'warning'">
            {{ cfg.batch?.enabled ? 'true' : 'false' }}
          </el-tag>
        </el-descriptions-item>
        <el-descriptions-item v-for="[k, v] in batchEntries" :key="k" :label="k">
          <span class="mono wrap">{{ v }}</span>
        </el-descriptions-item>
      </el-descriptions>

      <div class="jobs-head">
        <span class="card-title">可触发作业</span>
        <el-tag size="small" effect="light" type="info">
          {{ jobCards.length }} / 白名单 {{ allowedCount }}
        </el-tag>
        <span class="faint note">标题与描述来自 platform-common 的 LakeJobs</span>
      </div>

      <el-alert
        v-if="unknownJobs.length"
        class="unknown-alert"
        type="warning"
        :closable="false"
        show-icon
        :title="`白名单里有 ${unknownJobs.length} 个名字不在 LakeJobs 中：${unknownJobs.join('、')}`"
      />

      <el-table v-if="jobCards.length" :data="jobCards" size="small" max-height="320" class="jobs">
        <el-table-column label="作业" width="110">
          <template #default="{ row }">
            <span class="mono job-name" :class="{ featured: row.featured }">{{ row.name }}</span>
          </template>
        </el-table-column>
        <el-table-column label="标题" width="128" prop="title" />
        <el-table-column label="分组" width="96">
          <template #default="{ row }">
            <el-tag size="small" effect="plain" :type="row.group === '补充项' ? 'info' : 'primary'">
              {{ row.group }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="说明" min-width="240">
          <template #default="{ row }">
            <span class="desc">{{ row.desc }}</span>
            <el-tag v-if="row.cliOnly" size="small" effect="light" type="warning" class="flag">
              仅命令行
            </el-tag>
          </template>
        </el-table-column>
      </el-table>
      <el-empty
        v-else
        :image-size="56"
        description="白名单为空，或写进去的作业名都不认识"
      />
    </el-card>

    <el-card>
      <template #header>
        <div class="card-head">
          <span class="card-title">原始 JSON</span>
          <el-button size="small" text @click="rawOpen = !rawOpen">
            {{ rawOpen ? '收起' : '展开' }}
          </el-button>
        </div>
      </template>
      <pre v-if="rawOpen" class="raw">{{ JSON.stringify(cfg, null, 2) }}</pre>
      <span v-else class="faint note">GET /api/config 的原始响应</span>
    </el-card>
  </template>
</template>

<style scoped>
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

.block {
  display: block;
  margin-top: 2px;
}

.wrap {
  word-break: break-all;
}

.grid-2 {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
  gap: 14px;
}

/* 标签列固定宽度，值列贴左 —— 不然 warehouse / JDBC URL 会把标签挤成一条 */
.kv :deep(.el-descriptions__label) {
  width: 118px;
  padding-right: 12px;
}

.kv :deep(.el-descriptions__content) {
  font-size: 12px;
}

.jobs-head {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  margin: 16px 0 8px;
}

.unknown-alert {
  margin-bottom: 8px;
  font-size: 11.5px;
}

.jobs :deep(.el-table__cell) {
  padding: 6px 0;
}

.job-name.featured {
  color: var(--accent);
  font-weight: 600;
}

.desc {
  font-size: 11.5px;
  color: var(--muted);
}

.flag {
  margin-left: 6px;
}

.loading-icon {
  font-size: 18px;
  color: var(--accent);
}

.state-box {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 28px 8px;
}

.raw {
  margin: 0;
  padding: 12px;
  border-radius: 8px;
  border: 1px solid var(--border);
  background: #0c1118;
  color: #cfe0f5;
  font: 11.5px/1.65 var(--mono);
  overflow: auto;
  max-height: 56vh;
}
</style>
