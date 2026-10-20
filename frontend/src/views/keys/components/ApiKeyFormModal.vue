<script setup lang="ts">
import type { ApiKeyFormValue } from '../composables/useApiKeyEditor'
import type { CodexAccountCandidate } from '../composables/useCodexAccountCandidates'
import type { AccountGroupRef } from '@/api'
import { ZButton, ZDialog, ZForm, ZFormItem, ZInput, ZSelect, ZSwitch } from '@codex-proxy/ui'

import { DollarSign, KeyRound } from '@lucide/vue'
import { computed, watch } from 'vue'
import AccountGroupCheckboxGrid from '@/components/AccountGroupCheckboxGrid.vue'
import ProviderRequestProfilesEditor from '@/components/client-profile/ProviderRequestProfilesEditor.vue'

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

const codexDescription = computed(() => props.codexLocked
  ? '已绑定的账号不能更换，需要其他账号时新建密钥'
  : '只列出当前分组范围内已识别身份的 OpenAI OAuth 账号')

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
            class="flex min-w-0 items-center gap-2 rounded-cp bg-cp-fill-quaternary px-3 py-2"
          >
            <span class="min-w-0 truncate text-cp-sm text-cp-text" :title="boundAccountLabel">
              {{ boundAccountLabel }}
            </span>
          </div>

          <template v-else-if="form.codexAccessEnabled">
            <ZSelect
              :model-value="form.codexAccountId"
              class="w-full"
              :options="codexOptions"
              :disabled="saving || accountLoading"
              aria-label="原生 Codex 绑定账号"
              placeholder="选择 OpenAI OAuth 账号"
              empty-text="暂无 OpenAI OAuth 账号"
              @update:model-value="updateForm('codexAccountId', $event ?? '')"
            />
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
