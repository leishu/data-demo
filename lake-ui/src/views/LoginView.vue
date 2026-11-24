<script setup>
import { computed, ref } from 'vue'
import { Loading } from '@element-plus/icons-vue'
import { auth, login, restore } from '@/auth'

const username = ref('')
const password = ref('')
const error = ref('')
const busy = ref(false)

const DEMO_ACCOUNTS = [
  { user: 'viewer', pass: 'viewer', role: '只读查询' },
  { user: 'operator', pass: 'operator', role: '查询 + 批作业' },
  { user: 'auditor', pass: 'auditor', role: '审计原文 / 保全' }
]

const external = computed(() => auth.enabled && auth.mode === 'external')

function fill(account) {
  username.value = account.user
  password.value = account.pass
  error.value = ''
}

async function submit() {
  if (!username.value.trim() || !password.value) {
    error.value = '请输入用户名和密码'
    return
  }
  busy.value = true
  error.value = ''
  try {
    await login(username.value.trim(), password.value)
  } catch (e) {
    error.value = e.message || '登录失败'
  } finally {
    busy.value = false
  }
}
</script>

<template>
  <div class="login-page">
    <div class="login-card">
      <div class="brand">
        <span class="brand-mark" aria-hidden="true">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6"
               stroke-linecap="round" stroke-linejoin="round">
            <path d="M12 3 3 7.5l9 4.5 9-4.5L12 3Z" />
            <path d="m3 12.5 9 4.5 9-4.5" />
            <path d="m3 17 9 4.5L21 17" />
          </svg>
        </span>
        <span class="brand-text">
          <strong>Lakehouse</strong>
          <span>console</span>
        </span>
      </div>

      <template v-if="auth.status === 'checking'">
        <div class="state-box">
          <el-icon class="is-loading loading-icon"><Loading /></el-icon>
          <span class="faint">正在检查登录态…</span>
        </div>
      </template>

      <template v-else-if="external">
        <h2>由网关接管认证</h2>
        <p class="faint note">
          当前是 <code class="mono">external</code> 模式，登录由前置网关 / IdP 完成，
          本页不再接受用户名口令。请通过网关地址访问。
        </p>
        <el-button :loading="auth.status === 'checking'" @click="restore">重新检查登录态</el-button>
      </template>

      <template v-else>
        <h2>登录控制台</h2>
        <p class="faint note">演示账号按角色分权，密码与用户名相同</p>

        <el-alert
          v-if="auth.reason === 'expired'"
          class="notice"
          type="warning"
          :closable="false"
          show-icon
          title="登录已失效，请重新登录"
        />
        <el-alert
          v-else-if="auth.reason === 'logged-out'"
          class="notice"
          type="info"
          :closable="false"
          show-icon
          title="已退出，本地凭据已清除"
        />
        <el-alert
          v-if="error"
          class="notice"
          type="error"
          :closable="false"
          show-icon
          :title="error"
        />

        <el-form class="form" label-position="top" @submit.prevent="submit">
          <el-form-item label="用户名">
            <el-input
              v-model="username"
              size="large"
              autocomplete="username"
              placeholder="viewer / operator / auditor"
            />
          </el-form-item>
          <el-form-item label="密码">
            <el-input
              v-model="password"
              size="large"
              type="password"
              show-password
              autocomplete="current-password"
              placeholder="与用户名相同"
              @keyup.enter="submit"
            />
          </el-form-item>
          <el-button
            class="submit"
            type="primary"
            size="large"
            :loading="busy"
            :disabled="busy"
            @click="submit"
          >
            登录
          </el-button>
        </el-form>

        <div class="divider"></div>
        <div class="accounts">
          <span class="faint note">点一下填入演示账号</span>
          <div class="account-grid">
            <button
              v-for="account in DEMO_ACCOUNTS"
              :key="account.user"
              type="button"
              class="account"
              @click="fill(account)"
            >
              <code class="mono">{{ account.user }}</code>
              <span class="faint">{{ account.role }}</span>
            </button>
          </div>
        </div>
      </template>
    </div>

    <p class="foot faint">Spark · Iceberg · Trino</p>
  </div>
</template>

<style scoped>
.login-page {
  min-height: 100vh;
  min-height: 100dvh;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 16px;
  padding: 24px;
  background: radial-gradient(circle at 50% 0%, #152033 0%, var(--bg) 62%);
}

.login-card {
  width: min(100%, 400px);
  padding: 26px 28px 24px;
  border: 1px solid var(--border);
  border-radius: 14px;
  background: var(--panel);
  box-shadow: 0 18px 60px rgba(0, 0, 0, 0.4);
}

.brand {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 20px;
}

.brand-mark {
  width: 34px;
  height: 34px;
  flex: none;
  border-radius: 9px;
  background: var(--accent-weak);
  color: var(--accent);
  display: grid;
  place-items: center;
}

.brand-text {
  display: flex;
  flex-direction: column;
  line-height: 1.15;
}

.brand-text strong {
  font-size: 15px;
}

.brand-text span {
  font-size: 11px;
  color: var(--faint);
  letter-spacing: 0.14em;
  text-transform: uppercase;
}

h2 {
  margin: 0 0 4px;
  font-size: 17px;
  font-weight: 600;
}

.note {
  font-size: 12px;
}

.notice {
  margin-top: 12px;
  font-size: 11.5px;
}

.form {
  margin-top: 18px;
}

.submit {
  width: 100%;
  margin-top: 4px;
}

.divider {
  height: 1px;
  background: var(--border-soft);
  margin: 20px 0 14px;
}

.accounts {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.account-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 8px;
}

.account {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 2px;
  padding: 8px;
  border: 1px solid var(--border);
  border-radius: 8px;
  background: var(--panel-2);
  color: var(--text);
  cursor: pointer;
  text-align: left;
  font-size: 11px;
}

.account:hover {
  border-color: var(--accent);
}

.state-box {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 24px 0;
  font-size: 12px;
}

.loading-icon {
  font-size: 18px;
  color: var(--accent);
}

.foot {
  font-size: 11.5px;
}
</style>
