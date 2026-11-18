<script setup>
/**
 * Element Plus 是全局插件式安装（main.js 里 app.use），没有 Naive 那套
 * provider 链要包，所以这里只渲染壳。ElMessage / ElMessageBox 都是
 * 直接调用式 API，样式由 element-plus/dist/index.css 提供。
 */
import AppShell from '@/components/AppShell.vue'
</script>

<template>
  <AppShell />
</template>
