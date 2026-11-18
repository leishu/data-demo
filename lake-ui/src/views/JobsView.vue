<script setup>
import { computed, nextTick, onMounted, onUnmounted, ref } from 'vue'
import { useRoute } from 'vue-router'
import { ElMessage } from 'element-plus/es/components/message/index.mjs'
import { Loading } from '@element-plus/icons-vue'
import { api } from '@/api'
import { loadConfig, store } from '@/store'
import { fmtDuration, fmtJobStatus, fmtTime, statusTone, tagType } from '@/utils'
import ErrorNotice from '@/components/ErrorNotice.vue'

const JOB_META = {
  init: { title: '建库建表', desc: 'Iceberg DDL：库 + ODS/DWD/ADS 三张表与表属性' },
  ingest: { title: '数据入湖', desc: '生成上游落地文件并写入 bronze（含 3% 重复 + 10% 迟到）' },
  transform: { title: '加工汇总', desc: 'MERGE 去重进 silver，再重算 gold 汇总' },
  evolve: { title: 'Schema 演进', desc: '新增 channel 列并回填，演示 ADD COLUMN 与元数据升级' },
  maintain: { title: '表维护', desc: 'rewrite_data_files 合并小文件 + expire_snapshots' },
  show: { title: '数据概览', desc: '打印三层行数与分区分布' },
  'verify-sql': { title: 'SQL 校验', desc: '用 Spark 执行 Presto 脚本，需要 --sql-file 参数', cliOnly: true },
  all: { title: '全链路', desc: 'init → ingest → transform → maintain → show', featured: true }
}

const route = useRoute()

const runs = ref([])
// 湖表历史这份的可用性：OK / NOT_INITIALIZED / UNAVAILABLE / ERROR，读不到只降级提示
const historyInfo = ref(null)
const runsError = ref(null)
const runError = ref(null)
const submitting = ref(null)
const activeRun = ref(null)
const logTail = ref([])
const logWrap = ref(null)

let listTimer = null
let pollTimer = null

const allowedJobs = computed(() => (store.config?.batch?.allowedJobs || []).filter((j) => JOB_META[j] || j))
const pipelineJobs = computed(() => allowedJobs.value.filter((j) => !['evolve', 'verify-sql', 'all'].includes(j)))
const otherJobs = computed(() => allowedJobs.value.filter((j) => ['evolve', 'verify-sql', 'all'].includes(j)))
const batchEnabled = computed(() => store.config?.batch?.enabled !== false)
const commandTemplate = computed(() => store.config?.batch?.commandTemplate || '—')
const logText = computed(() => (logTail.value.length ? logTail.value.join('\n') : '暂无日志'))
const isRunning = computed(() => activeRun.value && activeRun.value.status === 'RUNNING')
const historyNotice = computed(() => {
  const info = historyInfo.value
  if (!info || info.status === 'OK') return null
  return {
    type: info.status === 'NOT_INITIALIZED' ? 'info' : 'warning',
    text: info.hint || `运行历史不可用（${info.status}）`
  }
})

async function scrollLogBottom() {
  await nextTick()
  const el = logWrap.value?.querySelector('.log')
  if (el) el.scrollTop = el.scrollHeight
}

async function loadRuns() {
  try {
    const res = await api.runs(20)
    runs.value = res?.runs || []
    historyInfo.value = res?.history || null
    runsError.value = null
  } catch (e) {
    runsError.value = e
  }
}

async function submit(job) {
  submitting.value = job
  runError.value = null
  try {
    const run = await api.submitJob(job)
    activeRun.value = run
    logTail.value = []
    ElMessage.success(`已提交 ${job}，运行 ID ${run.id}`)
    poll(run.id)
    await loadRuns()
  } catch (e) {
    runError.value = e
    ElMessage.error(e.message || '提交失败')
  } finally {
    submitting.value = null
  }
}

async function poll(id) {
  clearTimeout(pollTimer)
  try {
    const detail = await api.run(id, 60)
    activeRun.value = detail.run
    logTail.value = detail.tail || []
    if (detail.run.status === 'RUNNING') {
      pollTimer = setTimeout(() => poll(id), 2500)
    } else {
      const ok = detail.run.status === 'SUCCEEDED'
      const text = `${detail.run.job} ${fmtJobStatus(detail.run.status)}` +
        `（耗时 ${fmtDuration(detail.run.durationMs)}）`
      ok ? ElMessage.success(text) : ElMessage.error(text)
      await loadRuns()
    }
    await scrollLogBottom()
  } catch (e) {
    runError.value = e
  }
}

async function inspect(run) {
  activeRun.value = run
  logTail.value = []
  if (run.status === 'RUNNING') {
    poll(run.id)
    return
  }
  await showDetail(run.id)
}

/** 支持 #/jobs?run=xxx 直达某次运行（便于贴链接给别人看） */
async function showDetail(id) {
  try {
    const detail = await api.run(id, 80)
    activeRun.value = detail.run
    logTail.value = detail.tail || []
    await scrollLogBottom()
    if (detail.run.status === 'RUNNING') poll(detail.run.id)
  } catch (e) {
    runError.value = e
  }
}

onMounted(async () => {
  await loadConfig()
  await loadRuns()
  const target = route.query.run
  if (target) {
    const hit = runs.value.find((r) => r.id === target)
    if (hit) await inspect(hit)
    else await showDetail(String(target))
  }
  listTimer = setInterval(() => {
    loadRuns()
    if (isRunning.value) poll(activeRun.value.id)
  }, 6000)
})

onUnmounted(() => {
  clearInterval(listTimer)
  clearTimeout(pollTimer)
})
</script>

<template>
  <el-alert v-if="!batchEnabled" type="warning" :closable="false" show-icon title="作业编排已关闭">
    <code>lake.batch.enabled=false</code>，接口返回 403。需要时改 application.yml 后重启。
  </el-alert>

  <el-card>
    <template #header>
      <div class="card-head">
        <div class="head-left">
          <span class="card-title">触发作业</span>
          <el-tag size="small" effect="light" type="info">{{ allowedJobs.length }} 个白名单</el-tag>
        </div>
        <span class="faint note">命令模板 <code class="mono">{{ commandTemplate }}</code></span>
      </div>
    </template>

    <div class="job-grid">
      <div v-for="job in pipelineJobs" :key="job" class="job-card">
        <div class="job-head">
          <span class="job-name mono">{{ job }}</span>
          <span class="job-title">{{ JOB_META[job]?.title || job }}</span>
        </div>
        <p class="job-desc">{{ JOB_META[job]?.desc }}</p>
        <el-button
          size="small"
          class="block-btn"
          :loading="submitting === job"
          :disabled="!batchEnabled"
          @click="submit(job)"
        >
          {{ submitting === job ? '提交中' : '运行' }}
        </el-button>
      </div>
    </div>

    <div class="divider"></div>

    <div class="job-grid">
      <div
        v-for="job in otherJobs"
        :key="job"
        class="job-card"
        :class="{ featured: JOB_META[job]?.featured }"
      >
        <div class="job-head">
          <span class="job-name mono">{{ job }}</span>
          <span class="job-title">{{ JOB_META[job]?.title || job }}</span>
        </div>
        <p class="job-desc">{{ JOB_META[job]?.desc }}</p>
        <el-button
          size="small"
          class="block-btn"
          :type="JOB_META[job]?.featured ? 'primary' : 'default'"
          :loading="submitting === job"
          :disabled="!batchEnabled || JOB_META[job]?.cliOnly"
          :title="JOB_META[job]?.cliOnly ? '需要 --sql-file 参数，请在命令行执行' : ''"
          @click="submit(job)"
        >
          {{ JOB_META[job]?.cliOnly ? '仅命令行' : submitting === job ? '提交中' : '运行' }}
        </el-button>
      </div>
    </div>
  </el-card>

  <ErrorNotice v-if="runError" :error="runError" title="作业提交失败" @retry="loadRuns" />

  <div class="split">
    <el-card>
      <template #header>
        <div class="card-head">
          <div class="head-left">
            <span class="card-title">运行记录</span>
            <el-tag size="small" effect="light" type="info">最近 20 次</el-tag>
          </div>
          <el-button size="small" text @click="loadRuns">刷新</el-button>
        </div>
      </template>

      <ErrorNotice
        v-if="runsError"
        :error="runsError"
        title="读不到运行记录"
        @retry="loadRuns"
      />
      <template v-else>
        <!-- 湖表历史读不到只是降级：本进程记录照常显示，不把整块变成错误 -->
        <el-alert
          v-if="historyNotice"
          class="history-hint"
          :type="historyNotice.type"
          :closable="false"
          show-icon
          :title="historyNotice.text"
        />
        <el-empty v-if="!runs.length" description="还没有运行记录，上面点一个作业就会出现在这里" />
        <el-table
          v-else
          :data="runs"
          size="small"
          max-height="330"
          class="runs"
          :row-style="{ cursor: 'pointer' }"
          @row-click="(row) => inspect(row)"
        >
          <el-table-column label="作业" width="96">
            <template #default="{ row }"><span class="mono">{{ row.job }}</span></template>
          </el-table-column>
          <el-table-column label="状态" width="84">
            <template #default="{ row }">
              <el-tag size="small" effect="light" :type="tagType(statusTone(row.status))">
                {{ fmtJobStatus(row.status) }}
              </el-tag>
            </template>
          </el-table-column>
          <el-table-column label="开始" min-width="110">
            <template #default="{ row }"><span class="faint">{{ fmtTime(row.startedAt) }}</span></template>
          </el-table-column>
          <el-table-column label="耗时" width="88" align="right">
            <template #default="{ row }"><span class="mono">{{ fmtDuration(row.durationMs) }}</span></template>
          </el-table-column>
          <el-table-column label="退出码" width="76" align="right">
            <template #default="{ row }">
              <span v-if="row.exitCode === null || row.exitCode === undefined" class="faint">—</span>
              <span v-else :class="row.exitCode === 0 ? 'mono ok-text' : 'mono err-text'">{{ row.exitCode }}</span>
            </template>
          </el-table-column>
        </el-table>
      </template>
    </el-card>

    <el-card>
      <template #header>
        <div class="card-head">
          <div class="head-left">
            <span class="card-title">运行详情</span>
            <el-tag
              v-if="activeRun"
              size="small"
              effect="light"
              :type="tagType(statusTone(activeRun.status))"
            >
              {{ fmtJobStatus(activeRun.status) }}
            </el-tag>
            <span v-if="activeRun" class="faint note mono">{{ activeRun.job }}</span>
          </div>
          <el-icon v-if="isRunning" class="is-loading loading-icon"><Loading /></el-icon>
        </div>
      </template>

      <el-empty v-if="!activeRun" description="选一条运行记录，这里显示退出码、耗时与日志尾部" />

      <template v-else>
        <el-descriptions :column="2" size="small" class="detail">
          <el-descriptions-item label="运行 ID" :span="2">
            <span class="mono wrap">{{ activeRun.id }}</span>
          </el-descriptions-item>
          <el-descriptions-item label="命令" :span="2">
            <span class="mono wrap">{{ activeRun.command }}</span>
          </el-descriptions-item>
          <el-descriptions-item label="开始">{{ fmtTime(activeRun.startedAt) }}</el-descriptions-item>
          <el-descriptions-item label="结束">{{ fmtTime(activeRun.finishedAt) }}</el-descriptions-item>
          <el-descriptions-item label="耗时">{{ fmtDuration(activeRun.durationMs) }}</el-descriptions-item>
          <el-descriptions-item label="退出码">
            <span
              :class="
                activeRun.exitCode === 0
                  ? 'ok-text mono'
                  : activeRun.exitCode
                    ? 'err-text mono'
                    : 'faint mono'
              "
            >
              {{ activeRun.exitCode === null || activeRun.exitCode === undefined ? '—' : activeRun.exitCode }}
            </span>
          </el-descriptions-item>
          <el-descriptions-item label="日志文件" :span="2">
            <span class="mono wrap">{{ activeRun.logFile || '—' }}</span>
          </el-descriptions-item>
          <el-descriptions-item v-if="activeRun.message" label="说明" :span="2">
            {{ activeRun.message }}
          </el-descriptions-item>
        </el-descriptions>

        <div class="log-head">
          <span class="faint note">日志尾部（{{ logTail.length }} 行）</span>
          <el-tag v-if="isRunning" size="small" type="warning" effect="light">跟踪中 · 2.5s</el-tag>
        </div>
        <div ref="logWrap">
          <pre class="log">{{ logText }}</pre>
        </div>
      </template>
    </el-card>
  </div>
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

.history-hint {
  margin-bottom: 10px;
  font-size: 11.5px;
}

.note {
  font-size: 11.5px;
}

.wrap {
  font-size: 11.5px;
  word-break: break-all;
}

.block-btn {
  width: 100%;
}

.job-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(212px, 1fr));
  gap: 10px;
}

.job-card {
  border: 1px solid var(--border);
  border-radius: 8px;
  background: var(--panel-2);
  padding: 11px 12px 12px;
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.job-card.featured {
  border-color: rgba(90, 162, 240, 0.35);
  background: rgba(90, 162, 240, 0.07);
}

.job-head {
  display: flex;
  align-items: baseline;
  gap: 8px;
}

.job-name {
  font-size: 12.5px;
  font-weight: 600;
  color: #cfe3fb;
}

.job-title {
  font-size: 11.5px;
  color: var(--faint);
}

.job-desc {
  margin: 0 0 4px;
  font-size: 11.5px;
  line-height: 1.55;
  color: var(--muted);
  min-height: 36px;
}

.divider {
  height: 1px;
  background: var(--border-soft);
  margin: 14px 0;
}

.split {
  display: grid;
  grid-template-columns: minmax(0, 1.02fr) minmax(0, 1fr);
  gap: 14px;
  align-items: start;
}

/* 详情里标签列固定窄宽，值列占满，比 el-descriptions 默认的均分更像信息面板 */
.detail :deep(.el-descriptions__label) {
  width: 68px;
  white-space: nowrap;
}

.loading-icon {
  font-size: 16px;
  color: var(--accent);
}

.log-head {
  display: flex;
  align-items: center;
  gap: 8px;
  margin: 14px 0 6px;
}

/* Element Plus 没有 Log 组件，日志就是一段等宽 pre，滚动条自己滚 */
.log {
  margin: 0;
  padding: 10px 12px;
  max-height: 232px;
  overflow: auto;
  border: 1px solid var(--border-soft);
  border-radius: 8px;
  background: #0c1118;
  color: #c8d2e0;
  font: 11.5px/1.65 var(--mono);
  white-space: pre;
}

@media (max-width: 1080px) {
  .split {
    grid-template-columns: minmax(0, 1fr);
  }
}
</style>
