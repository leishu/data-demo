<script setup>
/**
 * 单个作业卡片。
 * 标题 / 描述 / 是否仅命令行 / 是否强调，全部来自 /api/config 的 batch.jobs
 * （源头是 platform-common 的 LakeJobs.METAS），这里不做任何按作业名的判断。
 */
defineProps({
  job: { type: Object, required: true },
  /** 当前正在提交的作业名，用来只让这一张卡转圈 */
  submitting: { type: String, default: null },
  /** 外部总开关：作业编排关闭时整片按钮置灰 */
  disabled: { type: Boolean, default: false }
})

defineEmits(['run'])
</script>

<template>
  <div class="job-card" :class="{ featured: job.featured }">
    <div class="job-head">
      <span class="job-name mono">{{ job.name }}</span>
      <span class="job-title">{{ job.title }}</span>
    </div>
    <p class="job-desc">{{ job.desc }}</p>
    <el-button
      size="small"
      class="block-btn"
      :type="job.featured ? 'primary' : 'default'"
      :loading="submitting === job.name"
      :disabled="disabled || job.cliOnly"
      :title="job.cliOnly ? '需要命令行参数，请在命令行执行' : ''"
      @click="$emit('run', job.name)"
    >
      {{ job.cliOnly ? '仅命令行' : submitting === job.name ? '提交中' : '运行' }}
    </el-button>
  </div>
</template>

<style scoped>
.job-card {
  border: 1px solid var(--border);
  border-radius: 8px;
  background: var(--panel-2);
  padding: 11px 12px 12px;
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.job-card.featured {
  border-color: rgba(90, 162, 240, 0.35);
  background: rgba(90, 162, 240, 0.07);
}

.job-head {
  display: flex;
  align-items: baseline;
  gap: 8px;
}

.job-name {
  font-size: 12.5px;
  font-weight: 600;
  color: #cfe3fb;
}

.job-title {
  font-size: 11.5px;
  color: var(--faint);
}

.job-desc {
  margin: 0 0 4px;
  font-size: 11.5px;
  line-height: 1.55;
  color: var(--muted);
  /* 至少 3 行：描述长短不一时同排卡片高度不会差太多 */
  min-height: 54px;
}

.block-btn {
  width: 100%;
  /* 贴底：同一排卡片描述行数不一致时按钮仍对齐 */
  margin-top: auto;
}
</style>
