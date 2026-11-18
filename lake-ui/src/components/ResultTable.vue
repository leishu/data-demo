<script setup>
import { computed } from 'vue'
import { isNumericType } from '@/utils'
import CopyButton from '@/components/CopyButton.vue'

const props = defineProps({
  result: { type: Object, required: true },
  maxHeight: { type: Number, default: 460 },
  /** 嵌在自己的卡片里时可以关掉这条头部，由外层卡片提供 */
  showMeta: { type: Boolean, default: true },
  title: { type: String, default: '结果' }
})

/** el-table-column 是组件，得在模板里 v-for，这里只准备行数据 */
const data = computed(() =>
  (props.result.rows || []).map((cells, i) => ({ __idx: i + 1, __cells: cells }))
)

const columns = computed(() => props.result.columns || [])

const hasColumns = computed(() => !!(props.result.columns && props.result.columns.length))

/** 列宽下限：内容少时按比例铺满，内容多时横向滚动 —— 全交给浏览器均分的话短值列会被拉得很开 */
function colMinWidth(type) {
  return isNumericType(type) ? 112 : 152
}

function isNull(v) {
  return v === null || v === undefined
}
</script>

<template>
  <div class="result">
    <div v-if="showMeta" class="result-head">
      <span class="result-title">{{ title }}</span>
      <el-tag size="small" type="primary" effect="light">{{ result.rowCount }} 行</el-tag>
      <el-tag v-if="result.truncated" size="small" type="warning" effect="light">已截断</el-tag>
      <span class="faint dot">·</span>
      <span class="faint meta">{{ result.elapsedMs }} ms</span>
      <span v-if="result.truncated" class="faint meta">调小上限或加 where 条件</span>
      <span class="spacer"></span>
      <CopyButton :result="result" />
    </div>

    <el-table
      v-if="hasColumns"
      :data="data"
      :max-height="maxHeight"
      size="small"
      class="grid-table"
    >
      <el-table-column
        type="index"
        label="#"
        width="52"
        align="right"
        header-align="right"
        fixed="left"
        class-name="idx-cell"
      />

      <el-table-column
        v-for="(c, i) in columns"
        :key="i"
        :label="c.name"
        :min-width="colMinWidth(c.type)"
        :align="isNumericType(c.type) ? 'right' : 'left'"
        :header-align="isNumericType(c.type) ? 'right' : 'left'"
        show-overflow-tooltip
      >
        <template #header>
          <div class="col-head">
            <span class="col-name">{{ c.name }}</span>
            <!-- 类型作为副标题：Presto 返回的类型偶尔和直觉不一致，标出来省得猜 -->
            <span class="col-type">{{ c.type }}</span>
          </div>
        </template>

        <template #default="{ row }">
          <span v-if="isNull(row.__cells[i])" class="null-cell">NULL</span>
          <span v-else-if="isNumericType(c.type)" class="num-cell">{{ row.__cells[i] }}</span>
          <span v-else>{{ row.__cells[i] }}</span>
        </template>
      </el-table-column>

      <template #empty>
        <div class="tbl-empty">该语句没有返回任何行</div>
      </template>
    </el-table>

    <div v-else class="tbl-empty">该语句执行成功，但没有返回结果集（例如 DDL）</div>
  </div>
</template>

<style scoped>
.result {
  min-width: 0;
}

.result-head {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  padding: 10px 14px;
  border-bottom: 1px solid var(--border-soft);
}

.result-title {
  font-size: 13px;
  font-weight: 600;
}

.dot {
  font-size: 11px;
}

.meta {
  font-size: 11.5px;
}

.spacer {
  margin-left: auto;
}

.tbl-empty {
  padding: 34px 16px;
  text-align: center;
  color: var(--faint);
  font-size: 13px;
}

/* el-table 没有开 border，这里补一条上边线，和头部分隔更清楚 */
.grid-table {
  border-top: 1px solid var(--border-soft);
}

:deep(.col-head) {
  display: flex;
  flex-direction: column;
  line-height: 1.25;
}

:deep(.col-name) {
  font-size: 12.5px;
  font-weight: 600;
  color: var(--text);
}

:deep(.col-type) {
  font: 400 10.5px var(--mono);
  color: var(--faint);
}

:deep(.num-cell) {
  font-family: var(--mono);
  font-size: 12.5px;
  font-variant-numeric: tabular-nums;
  color: #cfdaea;
}

:deep(.idx-cell .cell) {
  font-family: var(--mono);
  font-size: 11px;
  color: var(--faint);
}

:deep(.null-cell) {
  font-size: 12px;
  font-style: italic;
  color: var(--faint);
}
</style>
