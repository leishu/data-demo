<script setup>
import { computed } from 'vue'

const props = defineProps({
  error: { type: Object, default: null },
  title: { type: String, default: '' }
})

defineEmits(['retry'])

const statusText = computed(() =>
  props.error && props.error.status ? `HTTP ${props.error.status}` : '网络错误'
)
</script>

<template>
  <el-alert v-if="error" type="error" :closable="false" show-icon class="err-box">
    <template #title>
      <span class="err-title">{{ title || '请求失败' }}</span>
      <el-tag size="small" type="danger" effect="light" class="code-tag">{{ statusText }}</el-tag>
    </template>

    <pre class="msg">{{ error.message }}</pre>
    <div v-if="error.hint" class="hint">提示：{{ error.hint }}</div>

    <div class="actions">
      <el-button size="small" @click="$emit('retry')">重试</el-button>
    </div>
  </el-alert>
</template>

<style scoped>
.err-title {
  vertical-align: 1px;
}

.code-tag {
  margin-left: 8px;
  vertical-align: 1px;
}

.msg {
  margin: 4px 0 0;
  white-space: pre-wrap;
  word-break: break-word;
  font: 12px/1.6 var(--mono);
  color: #ffd0d0;
}

.hint {
  margin-top: 6px;
  font-size: 12px;
  color: var(--muted);
}

.actions {
  margin-top: 8px;
}
</style>
