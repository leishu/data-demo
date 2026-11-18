import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath, URL } from 'node:url'
import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import Components from 'unplugin-vue-components/vite'

/**
 * Element Plus 按需引入。
 *
 * 为什么不用 unplugin-vue-components 自带的 ElementPlusResolver：它从
 * 'element-plus/es' 根入口导入组件，而根入口 re-export 了全部组件，每个组件
 * 模块顶层的 `withInstall(xxx)` 是普通函数调用，Rollup 不敢判为无副作用，
 * 结果整个库还是被打进来（实测 922KB / gzip 298KB）。
 *
 * 这里扫一遍 es/components/<dir>/index.mjs 的导出名，建立
 * 「组件名 → 目录」的索引，让每个 ElXxx 只从自己的目录导入。
 * 扫描是自动的，Element Plus 升版本、加新组件都不用改这里。
 */
function elementPlusOnDemand() {
  const base = fileURLToPath(new URL('./node_modules/element-plus/es/components', import.meta.url))
  const index = new Map()

  for (const dir of fs.readdirSync(base)) {
    const entry = path.join(base, dir, 'index.mjs')
    if (!fs.existsSync(entry)) continue
    const source = fs.readFileSync(entry, 'utf8')
    // 形如：export { ElMenu, ElMenu as default, ElMenuItem, ... }
    for (const match of source.matchAll(/export\s*\{([^}]+)\}/g)) {
      for (const token of match[1].split(',')) {
        const name = token.trim().split(/\s+as\s+/).pop()
        if (name && name.startsWith('El') && !index.has(name)) index.set(name, dir)
      }
    }
  }

  return {
    // 这个字段必须写：插件内部按 `resolver.type !== type` 过滤，
    // 不给就会被当成未知类型直接跳过（症状：组件全部渲成原生标签）
    type: 'component',
    resolve(name) {
      const dir = index.get(name)
      if (!dir) return
      return {
        name,
        from: `element-plus/es/components/${dir}/index.mjs`,
        // 组件样式单独引，全量的 dist/index.css 就不要了
        sideEffects: `element-plus/es/components/${dir}/style/css`
      }
    }
  }
}

// 构建产物直接落进 lake-service 的静态资源目录，
// 这样 mvn package 出来的 jar 里自带控制台，不需要额外的 Web 服务器。
export default defineConfig({
  plugins: [
    vue(),
    Components({
      resolvers: [elementPlusOnDemand()],
      dts: false
    })
  ],
  // 相对路径：页面被放在 /ui/ 下也能正确加载 assets
  base: './',
  // Vite 7 默认用 lightningcss 转换 CSS，它会把 scoped 的单类选择器
  // `.foo[data-v-x]` 简化成 `[data-v-x]` —— 语义并不等价，同一组件里
  // 几条规则会互相污染（实测给 el-tag 也套上了 .dot 的样式）。
  // 退回 postcss + esbuild 压缩后选择器原样保留。
  css: {
    transformer: 'postcss'
  },
  resolve: {
    alias: { '@': fileURLToPath(new URL('./src', import.meta.url)) }
  },
  build: {
    outDir: '../lake-service/src/main/resources/static/ui',
    emptyOutDir: true,
    cssMinify: 'esbuild',
    chunkSizeWarningLimit: 600,
    rollupOptions: {
      output: {
        // 把 Vue 运行时和单独拆开：升级业务代码时这两个大块还能命中缓存
        manualChunks(id) {
          if (!id.includes('node_modules')) return
          if (id.includes('element-plus') || id.includes('@element-plus')) return 'element-plus'
          if (id.includes('/vue/') || id.includes('vue-router') || id.includes('@vue/')) return 'vue'
        }
      }
    }
  },
  server: {
    port: 5173,
    // 开发时前端跑 5173，接口转发到 Spring；生产由 Spring 自己同源提供
    proxy: {
      '/api': { target: 'http://127.0.0.1:8088', changeOrigin: true },
      '/actuator': { target: 'http://127.0.0.1:8088', changeOrigin: true }
    }
  }
})
