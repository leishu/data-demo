<script setup>
/**
 * Element Plus 组件由 unplugin-vue-components 在模板里按需注册（见 vite.config.js），
 * main.js 只装路由；ElMessage / ElMessageBox 这类函数式 API 直接 import 使用。
 * 所以这里没有全局 provider 链，只渲染壳。
 */
import AppShell from '@/components/AppShell.vue'
</script>

<template>
  <AppShell />
</template>
