<script setup>
import { computed, ref } from 'vue'
// 从具体组件路径导入：从 'element-plus' 根导入会把全部组件（含 installer）
// 一起拖进 bundle，按需引入就白做了
import { ElMessage } from 'element-plus/es/components/message/index.mjs'
import { copyText } from '@/utils'

const props = defineProps({
  result: { type: Object, default: null }
})

const copied = ref(false)

const tsv = computed(() => {
  const r = props.result
  if (!r || !r.columns || !r.columns.length) return ''
  const head = r.columns.map((c) => c.name).join('\t')
  const body = (r.rows || [])
    .map((row) => row.map((v) => (v === null || v === undefined ? '' : v)).join('\t'))
    .join('\n')
  return [head, body].filter(Boolean).join('\n')
})

async function copy() {
  try {
    await copyText(tsv.value)
    copied.value = true
    ElMessage.success('已复制为 TSV，可直接贴进 Excel')
    setTimeout(() => (copied.value = false), 1600)
  } catch {
    ElMessage.error('复制失败，浏览器可能禁止了剪贴板')
  }
}
</script>

<template>
  <el-button size="small" text :disabled="!tsv" @click="copy">
    {{ copied ? '已复制' : '复制 TSV' }}
  </el-button>
</template>
