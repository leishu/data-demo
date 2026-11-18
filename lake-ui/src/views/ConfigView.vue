<script setup>
import { computed, onMounted, ref } from 'vue'
import { Loading } from '@element-plus/icons-vue'
import { loadConfig, store } from '@/store'
import ErrorNotice from '@/components/ErrorNotice.vue'

const rawOpen = ref(false)
const loading = ref(false)

const cfg = computed(() => store.config)

function entries(obj) {
  if (!obj || typeof obj !== 'object') return []
  return Object.entries(obj).map(([k, v]) => [
    k,
    v === null || v === undefined
      ? '—'
      : typeof v === 'object'
        ? Array.isArray(v)
          ? v.join(', ')
          : JSON.stringify(v)
        : String(v)
  ])
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

const batchEntries = computed(() =>
  entries({ ...(cfg.value?.batch || {}) }).filter(([k]) => k !== 'enabled')
)

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
