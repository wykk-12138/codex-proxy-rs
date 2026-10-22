<script setup lang="ts">
import type { ApiKeyFormValue } from '../composables/useApiKeyEditor'
import type { CodexAccountCandidate } from '../composables/useCodexAccountCandidates'
import type { AccountGroupRef, ApiKeyCodexAccessMode } from '@/api'
import { ZButton, ZDialog, ZForm, ZFormItem, ZInput, ZSegmented, ZSelect, ZSwitch } from '@codex-proxy/ui'

import { DollarSign, KeyRound } from '@lucide/vue'
import { computed, watch } from 'vue'
import AccountGroupCheckboxGrid from '@/components/AccountGroupCheckboxGrid.vue'
import ProviderRequestProfilesEditor from '@/components/client-profile/ProviderRequestProfilesEditor.vue'
import { codexAccessModeLabels } from '../constants'

const props = defineProps<{
  groups: AccountGroupRef[]
  groupLoading: boolean
  accounts: CodexAccountCandidate[]
  accountLoading: boolean
  // 已存在的绑定只允许切换启停
  codexLocked: boolean
  editing: boolean
  saving: boolean
}>()
const emit = defineEmits<{
  save: []
  afterLeave: []
}>()
const open = defineModel<boolean>({ default: false })
const form = defineModel<ApiKeyFormValue>('form', { required: true })
const title = computed(() => props.editing ? '编辑密钥' : '创建 API Key')

function updateForm<Key extends keyof ApiKeyFormValue>(key: Key, value: ApiKeyFormValue[Key]) {
  form.value = { ...form.value, [key]: value }
}

const codexOptions = computed(() => props.accounts
  .filter(account => !form.value.groupIds.length
    || account.groups.some(group => form.value.groupIds.includes(group.id) && group.enabled))
  .map(account => ({ label: account.label, value: account.id })))

const boundAccountLabel = computed(() =>
  props.accounts.find(account => account.id === form.value.codexAccountId)?.label ?? form.value.codexAccountId,
)

// 两种模式共用同一份账号选择；差别只在执行账号如何确定
const modeOptions: { label: string, value: ApiKeyCodexAccessMode }[] = [
  { label: codexAccessModeLabels.fixed, value: 'fixed' },
  { label: codexAccessModeLabels.session_pool, value: 'session_pool' },
]
const FIXED_MODE_HINT = '所有原生请求都使用这个账号'
const SESSION_POOL_MODE_HINT = '不同会话可使用不同账号，同一会话不自动换号；入口账号只提供稳定身份与账号级辅助信息'

const activeModeLabel = computed(() => codexAccessModeLabels[form.value.codexAccessMode])

const modeHint = computed(() =>
  form.value.codexAccessMode === 'session_pool' ? SESSION_POOL_MODE_HINT : FIXED_MODE_HINT,
)

// 已建立的绑定只维护启停，模式与账号都不提供原地迁移
const lockedModeHint = computed(() => `模式建立后不能切换，需要其他模式时新建密钥；${modeHint.value}`)

const codexDescription = computed(() => props.codexLocked
  ? '已建立的账号与模式都不能更换，只能调整启停'
  : '只列出当前分组范围内已识别身份的 OpenAI OAuth 账号')

// 未知值按 fixed 处理，旧响应缺少 mode 时与后端缺省语义一致
function updateMode(value: string) {
  updateForm('codexAccessMode', value === 'session_pool' ? 'session_pool' : 'fixed')
}

// 分组变化后已选账号不在绑定范围时清空选择，避免提交服务端会拒绝的组合
watch(() => form.value.groupIds, () => {
  if (props.codexLocked || !form.value.codexAccountId)
    return
  if (!codexOptions.value.some(option => option.value === form.value.codexAccountId))
    updateForm('codexAccountId', '')
})
</script>

<template>
  <ZDialog
    v-model="open"
    :title="title"
    description="配置密钥信息、分组与使用限制"
    type="info"
    width="48rem"
    :show-close="!saving" :close-on-click-modal="!saving" :close-on-press-escape="!saving"
    @closed="emit('afterLeave')"
  >
    <template #icon>
      <KeyRound class="text-cp-text" :size="20" aria-hidden="true" />
    </template>

    <ZForm class="grid gap-6">
      <ZFormItem label="名称" required>
        <ZInput
          :model-value="form.name" aria-label="名称"
          placeholder="例如：生产环境"
          :disabled="saving"
          @update:model-value="updateForm('name', $event)"
        />
      </ZFormItem>

      <div class="grid gap-6" :class="{ 'sm:grid-cols-2': !editing }">
        <ZFormItem label="标签（可选）">
          <ZInput
            :model-value="form.label" aria-label="标签（可选）"
            placeholder="例如：后端服务"
            :disabled="saving"
            @update:model-value="updateForm('label', $event)"
          />
        </ZFormItem>

        <ZFormItem
          v-if="!editing"
          label="自定义 Key（可选）"
        >
          <ZInput
            :model-value="form.customKey" type="password"
            autocomplete="new-password"
            :spellcheck="false"
            aria-label="自定义 Key（可选）"
            placeholder="留空自动生成"
            :disabled="saving"
            @update:model-value="updateForm('customKey', $event)"
          />
        </ZFormItem>
      </div>

      <ZFormItem label="分组">
        <AccountGroupCheckboxGrid
          :model-value="form.groupIds" :groups="groups"
          :loading="groupLoading"
          :disabled="saving"
          @update:model-value="updateForm('groupIds', $event)"
        />
      </ZFormItem>

      <ZFormItem label="原生 Codex 访问" :description="codexDescription">
        <div class="grid gap-4">
          <ZSwitch
            :model-value="form.codexAccessEnabled"
            label="启用原生访问"
            :disabled="saving"
            @update:model-value="updateForm('codexAccessEnabled', $event)"
          />

          <div
            v-if="codexLocked"
            class="grid gap-1 rounded-cp bg-cp-fill-quaternary px-3 py-2"
          >
            <div class="flex min-w-0 items-center gap-2">
              <span class="shrink-0 text-cp-xs text-cp-text-tertiary">{{ activeModeLabel }}</span>
              <span class="min-w-0 truncate text-cp-sm text-cp-text" :title="boundAccountLabel">
                {{ boundAccountLabel }}
              </span>
            </div>
            <p class="m-0 text-cp-xs text-cp-text-tertiary">
              {{ lockedModeHint }}
            </p>
          </div>

          <template v-else-if="form.codexAccessEnabled">
            <ZSegmented
              :model-value="form.codexAccessMode"
              class="w-full"
              aria-label="原生 Codex 访问模式"
              :options="modeOptions"
              :disabled="saving"
              @update:model-value="updateMode"
            />
            <ZSelect
              :model-value="form.codexAccountId"
              class="w-full"
              :options="codexOptions"
              :disabled="saving || accountLoading"
              :aria-label="form.codexAccessMode === 'session_pool' ? '原生 Codex 入口账号' : '原生 Codex 绑定账号'"
              placeholder="选择 OpenAI OAuth 账号"
              empty-text="暂无 OpenAI OAuth 账号"
              @update:model-value="updateForm('codexAccountId', $event ?? '')"
            />
            <p class="m-0 text-cp-xs text-cp-text-tertiary">
              {{ modeHint }}
            </p>
          </template>
        </div>
      </ZFormItem>

      <ZFormItem label="上游身份">
        <ProviderRequestProfilesEditor
          :model-value="form.providerRequestProfileOverrides" allow-inherit
          :active="open"
          :disabled="saving"
          @update:model-value="updateForm('providerRequestProfileOverrides', $event)"
        />
      </ZFormItem>

      <div class="grid gap-6 sm:grid-cols-2">
        <ZFormItem label="日限额">
          <ZInput
            :model-value="form.dailyLimitUsd" type="number"
            min="0"
            step="any"
            aria-label="日限额（美元）"
            placeholder="不限制"
            :disabled="saving"
            @update:model-value="updateForm('dailyLimitUsd', $event)"
          >
            <template #prefix>
              <DollarSign class="size-4" aria-hidden="true" />
            </template>
          </ZInput>
        </ZFormItem>
        <ZFormItem label="周限额">
          <ZInput
            :model-value="form.weeklyLimitUsd" type="number"
            min="0"
            step="any"
            aria-label="周限额（美元）"
            placeholder="不限制"
            :disabled="saving"
            @update:model-value="updateForm('weeklyLimitUsd', $event)"
          >
            <template #prefix>
              <DollarSign class="size-4" aria-hidden="true" />
            </template>
          </ZInput>
        </ZFormItem>
      </div>

      <div class="grid gap-6 sm:grid-cols-2">
        <ZFormItem label="最大并发">
          <ZInput
            :model-value="form.maxConcurrency" type="number"
            aria-label="最大并发"
            min="0"
            step="1"
            placeholder="不限制"
            :disabled="saving"
            @update:model-value="updateForm('maxConcurrency', $event)"
          />
        </ZFormItem>
        <ZFormItem label="每分钟请求数（RPM）">
          <ZInput
            :model-value="form.requestsPerMinute" type="number"
            aria-label="每分钟请求数（RPM）"
            min="0"
            step="1"
            placeholder="不限制"
            :disabled="saving"
            @update:model-value="updateForm('requestsPerMinute', $event)"
          />
        </ZFormItem>
      </div>
    </ZForm>

    <template #footer>
      <ZButton :disabled="saving" @click="open = false">
        取消
      </ZButton>
      <ZButton
        type="primary"
        :loading="saving"
        :disabled="!form.name.trim()"
        @click="emit('save')"
      >
        {{ editing ? '保存更改' : '创建' }}
      </ZButton>
    </template>
  </ZDialog>
</template>
