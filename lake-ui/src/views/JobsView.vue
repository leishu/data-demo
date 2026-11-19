<script setup>
import { computed, nextTick, onMounted, onUnmounted, ref } from 'vue'
import { useRoute } from 'vue-router'
import { ElMessage } from 'element-plus/es/components/message/index.mjs'
import { Loading } from '@element-plus/icons-vue'
import { api, watchRun } from '@/api'
import { loadConfig, store } from '@/store'
import { fmtDuration, fmtJobStatus, fmtTime, statusTone, tagType } from '@/utils'
import ErrorNotice from '@/components/ErrorNotice.vue'
import JobCard from '@/components/JobCard.vue'

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
/** SSE 链路是否在跟着：断线退避期间为 false，模板上显示「重连中」 */
const watching = ref(false)

let listTimer = null
/** 列表请求是否在飞：轮询与手动刷新共用，刷新按钮的 loading 也绑它 */
const runsLoading = ref(false)

/** SSE 监控流与断线重连状态（不放到 ref：它们不参与渲染） */
let watch = null
let watchSeq = 0
let retryTimer = null
let retryDelay = 1000

/** 详情面板最多留多少行日志：SSE 是增量推送，不截断会一直涨 */
const MAX_LOG_LINES = 400

// 卡片内容全部由服务端下发（源头是 platform-common 的 LakeJobs.METAS），这里只分组
const jobCards = computed(() => store.config?.batch?.jobs || [])
const pipelineJobs = computed(() => jobCards.value.filter((j) => j.group !== 'other'))
const otherJobs = computed(() => jobCards.value.filter((j) => j.group === 'other'))
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
  // 上一轮还没回来就跳过：Trino 慢的时候别让请求排队叠加
  if (runsLoading.value) return
  runsLoading.value = true
  try {
    const res = await api.runs(20)
    runs.value = res?.runs || []
    historyInfo.value = res?.history || null
    runsError.value = null
  } catch (e) {
    runsError.value = e
  } finally {
    runsLoading.value = false
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
    follow(run.id)
    await loadRuns()
  } catch (e) {
    runError.value = e
    ElMessage.error(e.message || '提交失败')
  } finally {
    submitting.value = null
  }
}

/** 停掉当前监控流与待执行的重连 */
function stopFollow() {
  watchSeq++ // 让旧流的迟到回调失效
  watching.value = false
  if (retryTimer) {
    clearTimeout(retryTimer)
    retryTimer = null
  }
  if (watch) {
    watch.close()
    watch = null
  }
}

/**
 * 跟随一次运行的 SSE：status 推状态、log 推日志增量、end 推终态。
 *
 * 断线按 1s/2s/4s/5s 退避重连；重连后服务端会重发尾部 80 行日志，
 * 所以首个 status 到达时先清空日志，避免与断线前的内容重复。
 */
function follow(id) {
  stopFollow()
  if (document.hidden) return // 后台标签页不建流，切回前台由 visibilitychange 拉起
  const seq = ++watchSeq
  retryDelay = 1000
  let firstStatus = true
  watch = watchRun(id, {
    tail: 80,
    onStatus(run) {
      if (seq !== watchSeq) return
      activeRun.value = run
      watching.value = true
      retryDelay = 1000 // 收到帧说明链路通，下次断线从 1s 重新退避
      if (firstStatus) {
        firstStatus = false
        logTail.value = []
      }
    },
    onLog(lines) {
      if (seq !== watchSeq || !lines.length) return
      logTail.value.push(...lines)
      if (logTail.value.length > MAX_LOG_LINES) {
        logTail.value.splice(0, logTail.value.length - MAX_LOG_LINES)
      }
      scrollLogBottom()
    },
    onEnd(run) {
      if (seq !== watchSeq) return
      watch = null
      watching.value = false
      watchSeq++ // 终态后不再重连，也不接受迟到事件
      finishRun(run)
    },
    onError() {
      if (seq !== watchSeq) return
      watch = null
      watching.value = false
      scheduleReconnect(id, seq)
    }
  })
}

/** 终态：详情定格，提示结果并刷新列表 */
function finishRun(run) {
  activeRun.value = run
  const ok = run.status === 'SUCCEEDED'
  const text = `${run.job} ${fmtJobStatus(run.status)}` +
    `（耗时 ${fmtDuration(run.durationMs)}）`
  ok ? ElMessage.success(text) : ElMessage.error(text)
  loadRuns()
}

/** 断线重连：后台标签页不重试，切回前台由 visibilitychange 拉起 */
function scheduleReconnect(id, seq) {
  if (document.hidden || retryTimer) return
  retryTimer = setTimeout(() => {
    retryTimer = null
    if (document.hidden || seq !== watchSeq) return
    if (!isRunning.value) {
      // 断线期间作业已经结束：补一次详情即可，不再建流
      showDetail(id)
      return
    }
    follow(id)
  }, retryDelay)
  retryDelay = Math.min(retryDelay * 2, 5000)
}

async function inspect(run) {
  activeRun.value = run
  logTail.value = []
  if (run.status === 'RUNNING') {
    follow(run.id)
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
    if (detail.run.status === 'RUNNING') follow(detail.run.id)
  } catch (e) {
    runError.value = e
  }
}

/** 一个轮询节拍：后台标签页跳过；详情由 SSE 推，这里只刷列表 */
function tick() {
  if (document.hidden) return
  loadRuns()
}

function onVisibility() {
  if (document.hidden) {
    // 后台不占用 SSE 连接，也不重连
    stopFollow()
    return
  }
  tick()
  if (!activeRun.value) return
  if (isRunning.value) {
    follow(activeRun.value.id)
  } else {
    // 隐藏期间作业可能已经结束：补一次详情，SSE 就不必再建
    showDetail(activeRun.value.id)
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
  listTimer = setInterval(tick, 6000)
  document.addEventListener('visibilitychange', onVisibility)
})

onUnmounted(() => {
  clearInterval(listTimer)
  stopFollow()
  document.removeEventListener('visibilitychange', onVisibility)
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
          <el-tag size="small" effect="light" type="info">{{ jobCards.length }} 个作业</el-tag>
        </div>
        <span class="faint note">命令模板 <code class="mono">{{ commandTemplate }}</code></span>
      </div>
    </template>

    <el-empty
      v-if="!jobCards.length"
      description="白名单里没有可用的作业，检查 lake.batch.allowed-jobs"
    />

    <template v-else>
      <div class="job-grid">
        <JobCard
          v-for="job in pipelineJobs"
          :key="job.name"
          :job="job"
          :submitting="submitting"
          :disabled="!batchEnabled"
          @run="submit"
        />
      </div>

      <template v-if="otherJobs.length">
        <div class="divider"></div>

        <div class="job-grid">
          <JobCard
            v-for="job in otherJobs"
            :key="job.name"
            :job="job"
            :submitting="submitting"
            :disabled="!batchEnabled"
            @run="submit"
          />
        </div>
      </template>
    </template>
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
          <el-button size="small" text :loading="runsLoading" @click="loadRuns">刷新</el-button>
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
          <el-tag v-if="isRunning" size="small" type="warning" effect="light">
            {{ watching ? '实时跟踪' : '连接中断，重连中…' }}
          </el-tag>
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

.job-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(212px, 1fr));
  gap: 10px;
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
