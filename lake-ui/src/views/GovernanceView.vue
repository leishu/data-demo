<script setup>
import { computed, onMounted, ref } from 'vue'
import { Loading } from '@element-plus/icons-vue'
import { api } from '@/api'
import { fmtTime } from '@/utils'
import ErrorNotice from '@/components/ErrorNotice.vue'

const LIMIT = 100

const dq = ref([])
const assets = ref([])
const loading = ref(false)
const error = ref(null)
const showAll = ref(false)

const failed = computed(() => dq.value.filter((r) => !r.passed))
const visible = computed(() => (showAll.value ? dq.value : failed.value))
const failedErrors = computed(() => failed.value.filter((r) => r.severity === 'ERROR').length)

function severityType(severity) {
  return severity === 'ERROR' ? 'danger' : 'warning'
}

function num(value) {
  return value === null || value === undefined ? '—' : Number(value).toFixed(4)
}

function runHref(runId) {
  return `#/jobs?run=${encodeURIComponent(runId || '')}`
}

function freshnessTag(row) {
  const status = row.freshness?.status
  return ({ OK: 'success', WARN: 'warning', BREACH: 'danger', UNKNOWN: 'info' })[status] || 'info'
}

async function load() {
  loading.value = true
  error.value = null
  try {
    // 目录读不到不挡质量页：它只是补充信息
    const [dqRows, assetRows] = await Promise.all([
      api.dqResults(LIMIT),
      api.governanceAssets().catch(() => [])
    ])
    dq.value = Array.isArray(dqRows) ? dqRows : []
    assets.value = Array.isArray(assetRows) ? assetRows : []
  } catch (e) {
    dq.value = []
    error.value = e
  } finally {
    loading.value = false
  }
}

onMounted(load)
</script>

<template>
  <div class="governance">
    <el-card v-if="error">
      <template #header><span class="card-title">数据治理</span></template>
      <ErrorNotice :error="error" title="读不到质量检查结果" @retry="load" />
    </el-card>

    <template v-else>
      <el-card>
        <template #header>
          <div class="card-head">
            <div class="head-left">
              <span class="card-title">质量检查</span>
              <el-tag size="small" type="danger" effect="light">{{ failedErrors }} 项 ERROR 失败</el-tag>
              <el-tag size="small" type="warning" effect="light">{{ failed.length }} 项失败</el-tag>
            </div>
            <div class="extra">
              <el-checkbox v-model="showAll" size="small">显示全部</el-checkbox>
              <el-button size="small" text :loading="loading" @click="load">刷新</el-button>
            </div>
          </div>
        </template>

        <div v-if="loading && !dq.length" class="state-box">
          <el-icon class="is-loading loading-icon"><Loading /></el-icon>
        </div>
        <el-empty
          v-else-if="!visible.length"
          :description="showAll ? '还没有检查结果，先跑一次 quality 作业' : '最近没有失败项'"
        />
        <el-table v-else :data="visible" size="small" stripe style="width: 100%">
          <el-table-column prop="check" label="检查项" min-width="150" />
          <el-table-column label="级别" width="90">
            <template #default="{ row }">
              <el-tag size="small" :type="severityType(row.severity)" effect="light">{{ row.severity }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column label="实测" width="100">
            <template #default="{ row }"><span class="mono">{{ num(row.observed) }}</span></template>
          </el-table-column>
          <el-table-column label="阈值" width="100">
            <template #default="{ row }"><span class="mono">{{ num(row.threshold) }}</span></template>
          </el-table-column>
          <el-table-column label="证据" min-width="180">
            <template #default="{ row }">
              <span class="mono evidence" :title="row.detail || ''">{{ row.detail || '—' }}</span>
            </template>
          </el-table-column>
          <el-table-column label="快照" width="110">
            <template #default="{ row }"><span class="mono faint">{{ row.snapshotId || '—' }}</span></template>
          </el-table-column>
          <el-table-column label="运行" min-width="170">
            <template #default="{ row }">
              <a class="run-link mono" :href="runHref(row.runId)">{{ row.runId }}</a>
            </template>
          </el-table-column>
          <el-table-column label="时间" width="130">
            <template #default="{ row }"><span class="faint">{{ fmtTime(row.checkedMs) }}</span></template>
          </el-table-column>
        </el-table>
      </el-card>

      <el-card>
        <template #header>
          <div class="card-head">
            <span class="card-title">资产档案</span>
            <span class="faint note">责任人 / 分层 / 敏感级别 / 新鲜度（来自 GovernanceCatalog）</span>
          </div>
        </template>
        <el-table :data="assets" size="small" stripe style="width: 100%">
          <el-table-column prop="table" label="表" min-width="160" />
          <el-table-column prop="owner" label="负责人" min-width="110">
            <template #default="{ row }">{{ row.owner || '未登记' }}</template>
          </el-table-column>
          <el-table-column prop="domain" label="业务域" min-width="100">
            <template #default="{ row }">{{ row.domain || '—' }}</template>
          </el-table-column>
          <el-table-column label="分层" width="80">
            <template #default="{ row }"><el-tag size="small" effect="plain">{{ row.layer || '—' }}</el-tag></template>
          </el-table-column>
          <el-table-column label="敏感级别" width="110">
            <template #default="{ row }">{{ row.sensitivityLabel }}</template>
          </el-table-column>
          <el-table-column label="新鲜度" width="100">
            <template #default="{ row }">
              <el-tag size="small" :type="freshnessTag(row)" effect="light">
                {{ ({ OK: '新鲜', WARN: '偏旧', BREACH: '超期', UNKNOWN: '无快照' })[row.freshness?.status] || '无快照' }}
              </el-tag>
            </template>
          </el-table-column>
        </el-table>
      </el-card>
    </template>
  </div>
</template>

<style scoped>
.governance {
  display: grid;
  gap: 14px;
}

.card-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  flex-wrap: wrap;
}

.head-left,
.extra {
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

.state-box {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 22px 8px;
}

.loading-icon {
  font-size: 18px;
  color: var(--accent);
}

.evidence {
  display: inline-block;
  max-width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  vertical-align: bottom;
}

.run-link {
  color: var(--accent);
  text-decoration: none;
}

.run-link:hover {
  text-decoration: underline;
}
</style>
