import { createApp } from 'vue'
import App from './App.vue'
import router from './router'

// 顺序要紧：Element Plus 按需样式 → 官方深色变量 → 项目骨架 → 项目色板覆盖
import 'element-plus/es/components/message/style/css'
import 'element-plus/theme-chalk/dark/css-vars.css'
import './styles.css'
import './theme.css'

// Element Plus 的深色主题靠 html.dark 激活（变量都在 html.dark 作用域下）
document.documentElement.classList.add('dark')

// 组件本身由 unplugin-vue-components 在模板里按需注册（见 vite.config.js），
// 这里只装路由；ElMessage 这类函数式 API 直接 import 使用。
createApp(App).use(router).mount('#app')
