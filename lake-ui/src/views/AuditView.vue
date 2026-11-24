<script setup>
import { computed, onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus/es/components/message/index.mjs'
import { api } from '@/api'
import { copyText, fmtTime } from '@/utils'
import ErrorNotice from '@/components/ErrorNotice.vue'

const LIMITS = [50, 100, 200]

const events = ref([])
const limit = ref(100)
const listLoading = ref(false)
const listError = ref(null)

const selected = ref(null)
const text = ref(null)
const textError = ref(null)
const textLoading = ref(false)
const reason = ref('compliance-review')
const holdBusy = ref(false)
const copied = ref(false)

const eventId = computed(() => selected.value?.fields?.event_id || '')
const eventFields = computed(() => {
  const fields = selected.value?.fields || {}
  return Object.entries(fields).filter(([key]) => key !== 'event_id')
})

async function loadEvents() {
  listLoading.value = true
  try {
    const data = await api.audit(limit.value)
    events.value = Array.isArray(data) ? data : []
    listError.value = null
  } catch (e) {
    listError.value = e
  } finally {
    listLoading.value = false
  }
}

function select(event) {
  selected.value = event
  resetText()
}

/** 切换事件 / 重新调阅前清掉上一次的原文，避免串到别的事件上 */
function resetText() {
  text.value = null
  textError.value = null
  reason.value = 'compliance-review'
  copied.value = false
}

function rowClass({ row }) {
  return row.fields?.event_id === eventId.value ? 'selected-row' : ''
}

function fieldText(event) {
  return Object.entries(event.fields || {})
    .filter(([key]) => key !== 'event_id')
    .map(([key, value]) => `${key}=${value}`)
    .join('  ')
}

function eventTone(event) {
  if (/denied|failed|error/i.test(event)) return 'danger'
  if (/query|semantic/i.test(event)) return 'primary'
  if (/batch/i.test(event)) return 'success'
  return 'info'
}

async function loadText() {
  if (!eventId.value) return
  textLoading.value = true
  textError.value = null
  try {
    text.value = await api.auditText(eventId.value, reason.value.trim() || 'compliance-review')
  } catch (e) {
    text.value = null
    textError.value = e
  } finally {
    textLoading.value = false
  }
}

async function setHold(value) {
  holdBusy.value = true
  try {
    await api.auditHold(eventId.value, value)
    text.value = { ...text.value, legal_hold: value }
    ElMessage.success(value ? '已置位法律保全' : '已解除法律保全')
  } catch (e) {
    textError.value = e
    ElMessage.error(e.message || '保全操作失败')
  } finally {
    holdBusy.value = false
  }
}

async function copy(value, label) {
  try {
    await copyText(value)
    copied.value = true
    ElMessage.success(`已复制${label}`)
    setTimeout(() => (copied.value = false), 1600)
  } catch {
    ElMessage.error('复制失败，浏览器可能禁止了剪贴板')
  }
}

onMounted(loadEvents)
</script>

<template>
  <el-card>
    <template #header>
      <div class="card-head">
        <div class="head-left">
          <span class="card-title">最近审计事件</span>
          <el-tag size="small" effect="light" type="info">内存缓冲 · 最多 200 条</el-tag>
        </div>
        <div class="head-right">
          <el-select v-model="limit" size="small" class="limit" @change="loadEvents">
            <el-option v-for="n in LIMITS" :key="n" :label="`${n} 条`" :value="n" />
          </el-select>
          <el-button size="small" text :loading="listLoading" @click="loadEvents">刷新</el-button>
        </div>
      </div>
    </template>

    <ErrorNotice
      v-if="listError"
      :error="listError"
      title="读不到审计事件"
      @retry="loadEvents"
    />
    <template v-else>
      <el-alert
        class="role-hint"
        type="info"
        :closable="false"
        show-icon
        title="列表需要 QUERY 或 AUDITOR 角色（viewer / operator / auditor 都可以）；查询原文与保全另需 AUDITOR 角色"
      />
      <el-empty v-if="!events.length" :image-size="56" description="暂无审计事件" />
      <el-table
        v-else
        :data="events"
        size="small"
        max-height="420"
        class="events"
        :row-class-name="rowClass"
        :row-style="{ cursor: 'pointer' }"
        @row-click="select"
      >
        <el-table-column label="时间" width="106">
          <template #default="{ row }"><span class="faint mono">{{ fmtTime(row.at) }}</span></template>
        </el-table-column>
        <el-table-column label="动作" width="116">
          <template #default="{ row }">
            <el-tag size="small" effect="light" :type="eventTone(row.event)">{{ row.event }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="身份" width="92">
          <template #default="{ row }"><span class="mono">{{ row.actor }}</span></template>
        </el-table-column>
        <el-table-column label="字段" min-width="260">
          <template #default="{ row }"><span class="fields wrap">{{ fieldText(row) || '—' }}</span></template>
        </el-table-column>
      </el-table>
    </template>
  </el-card>

  <el-card>
    <template #header>
      <div class="card-head">
        <div class="head-left">
          <span class="card-title">事件详情</span>
          <el-tag v-if="selected" size="small" effect="light" :type="eventTone(selected.event)">
            {{ selected.event }}
          </el-tag>
        </div>
        <el-button v-if="selected" size="small" text @click="selected = null; resetText()">关闭</el-button>
      </div>
    </template>

    <el-empty v-if="!selected" description="点一条事件，看字段与关联的查询原文" />

    <template v-else>
      <el-descriptions :column="1" size="small" class="detail">
        <el-descriptions-item label="时间">{{ fmtTime(selected.at) }}</el-descriptions-item>
        <el-descriptions-item label="身份"><span class="mono">{{ selected.actor }}</span></el-descriptions-item>
        <el-descriptions-item label="事件 ID">
          <span class="mono wrap">{{ eventId }}</span>
          <el-button size="small" text class="copy-btn" @click="copy(eventId, '事件 ID')">
            {{ copied ? '已复制' : '复制' }}
          </el-button>
        </el-descriptions-item>
        <el-descriptions-item v-for="[key, value] in eventFields" :key="key" :label="key">
          <span class="mono wrap">{{ value }}</span>
        </el-descriptions-item>
      </el-descriptions>

      <div class="vault">
        <div class="vault-head">
          <span class="card-title">查询原文保险箱</span>
          <el-tag size="small" effect="plain" type="warning">需 AUDITOR · 每次读取留痕</el-tag>
        </div>
        <p class="faint note">
          只有 <code class="mono">query.execute</code> / <code class="mono">semantic.query</code>
          事件在保险箱开启时才有原文；SQL 在写入前已脱敏，读取需填理由。
        </p>
        <p v-if="!eventId" class="warn-text note">这条事件没有 event_id，无法关联原文记录。</p>

        <template v-if="!text">
          <div class="reason">
            <el-input
              v-model="reason"
              size="small"
              placeholder="调阅理由，会记入审计"
              clearable
              @keyup.enter="loadText"
            />
            <el-button
              size="small"
              type="primary"
              :loading="textLoading"
              :disabled="!eventId"
              @click="loadText"
            >
              调阅原文
            </el-button>
          </div>

          <el-alert
            v-if="textError && textError.status === 403"
            class="vault-alert"
            type="warning"
            :closable="false"
            show-icon
            title="权限不足：调阅原文需要 AUDITOR 角色"
          >
            用 <code class="mono">auditor/auditor</code> 重新登录；若浏览器缓存了上一个账号的凭据，需先清除该站点的登录信息。
          </el-alert>
          <el-alert
            v-else-if="textError && textError.status === 404"
            class="vault-alert"
            type="info"
            :closable="false"
            show-icon
            title="这条事件没有原文记录"
          >
            保险箱可能未开启（<code class="mono">lake.audit.sql-capture-enabled</code>），或该事件不写 SQL 原文。
          </el-alert>
          <ErrorNotice
            v-else-if="textError"
            :error="textError"
            title="调阅原文失败"
            @retry="loadText"
          />
        </template>

        <template v-else>
          <div class="text-meta">
            <el-tag size="small" effect="light" :type="text.redacted ? 'warning' : 'success'">
              {{ text.redacted ? '已脱敏' : '未脱敏' }}
            </el-tag>
            <el-tag size="small" effect="plain" type="info">key {{ text.key_id }}</el-tag>
            <el-tag size="small" effect="plain" type="info">{{ text.client_ip || '—' }}</el-tag>
            <span class="faint note">留存至 {{ fmtTime(text.expires_at) }}</span>
          </div>

          <div class="hold-row">
            <span class="note">法律保全</span>
            <el-switch
              :model-value="!!text.legal_hold"
              :disabled="holdBusy"
              inline-prompt
              active-text="保全中"
              inactive-text="未保全"
              @change="setHold"
            />
            <el-button size="small" text :disabled="holdBusy" @click="resetText">重新调阅</el-button>
          </div>

          <div class="sql-head">
            <span class="faint note">SQL 原文（{{ text.sql?.length || 0 }} 字符）</span>
            <el-button size="small" text @click="copy(text.sql || '', 'SQL')">复制</el-button>
          </div>
          <pre class="sql">{{ text.sql || '—' }}</pre>

          <ErrorNotice
            v-if="textError"
            :error="textError"
            title="保全操作失败"
            @retry="setHold(!text.legal_hold)"
          />
        </template>
      </div>
    </template>
  </el-card>
</template>

<style scoped>
.card-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  flex-wrap: wrap;
}

.head-left,
.head-right {
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

.wrap {
  word-break: break-all;
}

.role-hint {
  margin-bottom: 10px;
  font-size: 11.5px;
}

.events :deep(.el-table__cell) {
  padding: 6px 0;
}

.events :deep(.selected-row) {
  --el-table-tr-bg-color: var(--accent-weak);
}

.fields {
  font: 11px/1.5 var(--mono);
  color: var(--muted);
}

.limit {
  width: 88px;
}

.detail :deep(.el-descriptions__label) {
  width: 78px;
  white-space: nowrap;
}

.detail :deep(.el-descriptions__content) {
  font-size: 12px;
}

.copy-btn {
  margin-left: 6px;
}

.vault {
  margin-top: 16px;
  padding-top: 14px;
  border-top: 1px solid var(--border-soft);
}

.vault-head {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.vault p {
  margin: 8px 0 10px;
}

.reason {
  display: flex;
  gap: 8px;
}

.vault-alert {
  margin-top: 10px;
  font-size: 11.5px;
}

.text-meta {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  margin: 10px 0;
}

.hold-row {
  display: flex;
  align-items: center;
  gap: 10px;
  margin: 10px 0;
}

.sql-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin: 12px 0 6px;
}

.sql {
  margin: 0;
  padding: 10px 12px;
  max-height: 260px;
  overflow: auto;
  border: 1px solid var(--border-soft);
  border-radius: 8px;
  background: #0c1118;
  color: #c8d2e0;
  font: 11.5px/1.65 var(--mono);
  white-space: pre;
}
</style>
