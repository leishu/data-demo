<script setup>
/**
 * Element Plus 组件由 unplugin-vue-components 在模板里按需注册（见 vite.config.js），
 * main.js 只装路由；ElMessage / ElMessageBox 这类函数式 API 直接 import 使用。
 * 所以这里没有全局 provider 链，只渲染壳。
 *
 * 登录态没确认之前不挂 AppShell：否则页面一 mount 就会打 /api/*，
 * 没带凭据会触发 401 + 浏览器原生 Basic 弹框，和自己的登录页打架。
 */
import { onMounted } from 'vue'
import { auth, restore } from '@/auth'
import LoginView from '@/views/LoginView.vue'
import AppShell from '@/components/AppShell.vue'

onMounted(restore)
</script>

<template>
  <LoginView v-if="auth.status !== 'authenticated'" />
  <AppShell v-else />
</template>
