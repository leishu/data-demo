import { createRouter, createWebHashHistory } from 'vue-router'

import QueryView from './views/QueryView.vue'
import TablesView from './views/TablesView.vue'
import JobsView from './views/JobsView.vue'
import SemanticView from './views/SemanticView.vue'
import ConfigView from './views/ConfigView.vue'

// hash 模式：被 Spring Boot 当静态资源托管时不需要服务端做 history fallback
const router = createRouter({
  history: createWebHashHistory(),
  routes: [
    { path: '/', redirect: '/query' },
    {
      path: '/query',
      name: 'query',
      component: QueryView,
      meta: { title: '查询台', subtitle: '走 Trino 执行只读 SQL，结果直接看' }
    },
    {
      path: '/tables',
      name: 'tables',
      component: TablesView,
      meta: { title: '表浏览', subtitle: 'Iceberg 三层表的清单与采样预览' }
    },
    {
      path: '/jobs',
      name: 'jobs',
      component: JobsView,
      meta: { title: '批作业', subtitle: '触发 Spark 作业，跟踪状态与日志尾部' }
    },
    {
      path: '/semantics',
      name: 'semantics',
      component: SemanticView,
      meta: { title: '指标语义', subtitle: '受控口径查询，可预览 Trino / Spark 两种方言 SQL' }
    },
    {
      path: '/config',
      name: 'config',
      component: ConfigView,
      meta: { title: '配置', subtitle: '服务实际生效的 catalog / 仓库 / 查询参数' }
    },
    { path: '/:pathMatch(.*)*', redirect: '/query' }
  ]
})

export default router
