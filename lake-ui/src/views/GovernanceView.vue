<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import { Loading } from '@element-plus/icons-vue'
import { api } from '@/api'
import { fmtTime } from '@/utils'
import ErrorNotice from '@/components/ErrorNotice.vue'

const LIMIT = 100

const dq = ref([])
const assets = ref([])
const lineage = ref(null)
const selectedTable = ref('gold_daily_revenue')
const retention = ref([])
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

const tableOptions = computed(() =>
  assets.value.length ? assets.value.map((a) => a.table) : ['bronze_orders', 'silver_orders', 'gold_daily_revenue'])

async function loadLineage() {
  try {
    lineage.value = await api.lineage(selectedTable.value, LIMIT)
  } catch (e) {
    lineage.value = null
  }
}

async function load() {
  loading.value = true
  error.value = null
  try {
    // 目录/血缘/审计读不到不挡质量页：它们都是补充信息
    const [dqRows, assetRows, retentionRows] = await Promise.all([
      api.dqResults(LIMIT),
      api.governanceAssets().catch(() => []),
      api.retentionAudit(LIMIT).catch(() => [])
    ])
    dq.value = Array.isArray(dqRows) ? dqRows : []
    assets.value = Array.isArray(assetRows) ? assetRows : []
    retention.value = Array.isArray(retentionRows) ? retentionRows : []
    await loadLineage()
  } catch (e) {
    dq.value = []
    error.value = e
  } finally {
    loading.value = false
  }
}

watch(selectedTable, loadLineage)
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

      <el-card>
        <template #header>
          <div class="card-head">
            <span class="card-title">血缘</span>
            <div class="extra">
              <span class="faint note">表/列声明来自转换 SQL 注释，运行记录来自 ops.lineage_runs</span>
              <el-select v-model="selectedTable" size="small" class="table-select">
                <el-option v-for="t in tableOptions" :key="t" :label="t" :value="t" />
              </el-select>
            </div>
          </div>
        </template>
        <div v-if="lineage" class="lineage">
          <div class="lineage-block">
            <span class="faint">上游</span>
            <el-tag v-for="t in lineage.upstream" :key="t" size="small" effect="plain">{{ t }}</el-tag>
            <span v-if="!lineage.upstream.length" class="faint">无</span>
          </div>
          <div class="lineage-block">
            <span class="faint">下游</span>
            <el-tag v-for="t in lineage.downstream" :key="t" size="small" effect="plain">{{ t }}</el-tag>
            <span v-if="!lineage.downstream.length" class="faint">无</span>
          </div>
          <el-table :data="lineage.columns" size="small" stripe style="width: 100%">
            <el-table-column prop="sourceTable" label="来源表" min-width="150" />
            <el-table-column prop="sourceColumn" label="来源列" min-width="120" />
            <el-table-column prop="targetColumn" label="目标列" min-width="120" />
          </el-table>
          <div class="sub-title">最近写入（点 run_id 看作业日志）</div>
          <el-table :data="lineage.runs" size="small" stripe style="width: 100%">
            <el-table-column label="运行" min-width="180">
              <template #default="{ row }">
                <a class="run-link mono" :href="runHref(row.runId)">{{ row.runId }}</a>
              </template>
            </el-table-column>
            <el-table-column prop="job" label="作业" width="120" />
            <el-table-column prop="snapshotId" label="快照 ID" width="120" />
            <el-table-column label="时间" width="140">
              <template #default="{ row }">{{ fmtTime(row.recordedMs) }}</template>
            </el-table-column>
          </el-table>
          <div v-if="!lineage.runs.length" class="faint note empty-note">
            这张表还没有写入血缘记录，先跑一次 ingest / transform
          </div>
        </div>
      </el-card>

      <el-card>
        <template #header>
          <div class="card-head">
            <span class="card-title">保留审计</span>
            <span class="faint note">执行前后各一条事件，dry-run 同样留痕</span>
          </div>
        </template>
        <el-table :data="retention" size="small" stripe style="width: 100%">
          <el-table-column prop="event" label="事件" width="140" />
          <el-table-column prop="table" label="表" min-width="150" />
          <el-table-column label="模式" width="100">
            <template #default="{ row }">
              <el-tag size="small" :type="row.dryRun ? 'info' : 'warning'" effect="light">
                {{ row.dryRun ? 'dry-run' : 'execute' }}
              </el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="partitions" label="分区" width="80" />
          <el-table-column prop="rowCount" label="行数" width="90" />
          <el-table-column label="运行" min-width="180">
            <template #default="{ row }">
              <a class="run-link mono" :href="runHref(row.runId)">{{ row.runId }}</a>
            </template>
          </el-table-column>
          <el-table-column label="时间" width="140">
            <template #default="{ row }">{{ fmtTime(row.auditedMs) }}</template>
          </el-table-column>
        </el-table>
        <el-empty v-if="!retention.length" description="还没有保留作业审计事件" :image-size="40" />
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

.table-select {
  width: 180px;
}

.lineage {
  display: grid;
  gap: 10px;
}

.lineage-block {
  display: flex;
  align-items: center;
  gap: 6px;
  flex-wrap: wrap;
}

.sub-title {
  font-size: 12px;
  font-weight: 600;
  margin-top: 4px;
}

.empty-note {
  padding: 8px 0;
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
