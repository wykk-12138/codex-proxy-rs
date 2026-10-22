import type { ApiKey, ApiKeyCodexAccess, ApiKeyCodexAccessMode } from '@/api'
import type { ProviderRequestProfiles, ProviderRequestProfileUpdates } from '@/api/modules/settings/profiles'
import { ZNotification } from '@codex-proxy/ui'
import { cloneDeep } from 'es-toolkit'
import { ref, shallowRef } from 'vue'
import { createApiKey, updateApiKey } from '@/api'
import { useAsyncAction } from '@/composables/useAsyncAction'

export interface ApiKeyFormValue {
  providerRequestProfileOverrides: ProviderRequestProfiles
  customKey: string
  codexAccountId: string
  codexAccessEnabled: boolean
  // 仅新建绑定时可编辑；已有绑定沿用其模式，不允许原地切换
  codexAccessMode: ApiKeyCodexAccessMode
  name: string
  label: string
  groupIds: string[]
  maxConcurrency: string
  requestsPerMinute: string
  dailyLimitUsd: string
  weeklyLimitUsd: string
}

export function useApiKeyEditor(options: {
  reload: () => Promise<unknown>
  onCreated: (key: string, name: string) => void
}) {
  const showFormModal = shallowRef(false)
  const showAllAccountsConfirm = shallowRef(false)
  const editingKey = shallowRef<ApiKey | null>(null)
  const savingKeyAction = useAsyncAction()
  const savingKey = savingKeyAction.loading
  const form = ref<ApiKeyFormValue>(emptyForm())

  function openCreate() {
    editingKey.value = null
    form.value = emptyForm()
    showFormModal.value = true
  }

  function openEdit(key: ApiKey) {
    editingKey.value = key
    form.value = {
      providerRequestProfileOverrides: cloneDeep(key.providerRequestProfileOverrides),
      customKey: '',
      codexAccountId: key.codexAccess?.accountId ?? '',
      codexAccessEnabled: key.codexAccess?.enabled ?? false,
      codexAccessMode: key.codexAccess?.mode ?? 'fixed',
      name: key.name,
      label: key.label ?? '',
      groupIds: key.groups.map(group => group.id),
      maxConcurrency: limitInputValue(key.maxConcurrency),
      requestsPerMinute: limitInputValue(key.requestsPerMinute),
      dailyLimitUsd: limitInputValue(key.dailyLimitUsd),
      weeklyLimitUsd: limitInputValue(key.weeklyLimitUsd),
    }
    showFormModal.value = true
  }

  function requestSave() {
    if (!validateForm() || savingKey.value)
      return
    if (form.value.groupIds.length === 0) {
      showAllAccountsConfirm.value = true
      return
    }
    void save()
  }

  async function confirmAllAccountsScope() {
    showAllAccountsConfirm.value = false
    await save()
  }

  async function save() {
    if (!validateForm() || savingKey.value)
      return

    await savingKeyAction.run(
      async () => {
        const payload = {
          name: form.value.name.trim(),
          label: form.value.label.trim() || null,
          groupIds: [...new Set(form.value.groupIds)],
          maxConcurrency: Number(form.value.maxConcurrency),
          requestsPerMinute: Number(form.value.requestsPerMinute),
          dailyLimitUsd: form.value.dailyLimitUsd.trim() || '0',
          weeklyLimitUsd: form.value.weeklyLimitUsd.trim() || '0',
        }
        const current = editingKey.value
        // 已有绑定只能切换启停，账号 ID 与模式始终沿用原值
        const boundAccountId = current?.codexAccess?.accountId ?? null
        const codexAccountId = boundAccountId ?? (form.value.codexAccessEnabled ? form.value.codexAccountId : '')
        let codexAccess: ApiKeyCodexAccess | undefined
        if (codexAccountId) {
          // 已有绑定的模式不可原地切换；省略 mode 即保留既有模式，不会退成 fixed
          codexAccess = current?.codexAccess
            ? { enabled: form.value.codexAccessEnabled, accountId: codexAccountId }
            : {
                enabled: form.value.codexAccessEnabled,
                mode: form.value.codexAccessMode,
                accountId: codexAccountId,
              }
        }
        let created: { key: string, name: string } | undefined
        if (current) {
          await updateApiKey({
            id: current.id,
            ...payload,
            codexAccess,
            providerRequestProfileOverrides: profileOverrideUpdates(
              current.providerRequestProfileOverrides,
              form.value.providerRequestProfileOverrides,
            ),
          })
        }
        else {
          const result = await createApiKey({
            ...payload,
            codexAccess,
            providerRequestProfileOverrides: cloneDeep(form.value.providerRequestProfileOverrides),
            customKey: form.value.customKey || undefined,
          })
          created = { key: result.plaintextKey, name: payload.name }
        }

        showFormModal.value = false
        await options.reload()
        if (current) {
          ZNotification.success({ message: 'API Key 已更新' })
        }
        else if (created) {
          options.onCreated(created.key, created.name)
          ZNotification.success({ message: 'API Key 创建成功' })
        }
      },
      { onError: () => void options.reload() },
    )
  }

  function validateForm() {
    for (const [label, value] of [['日限额', form.value.dailyLimitUsd], ['周限额', form.value.weeklyLimitUsd]]) {
      if (value.trim() && !/^\d{1,10}(?:\.\d{1,10})?$/.test(value.trim())) {
        ZNotification.warning({ message: `${label}必须是非负金额，最多 10 位小数` })
        return false
      }
    }
    if (!form.value.name.trim()) {
      ZNotification.warning({ message: '请输入 API Key 名称' })
      return false
    }
    if (!editingKey.value?.codexAccess && form.value.codexAccessEnabled && !form.value.codexAccountId) {
      ZNotification.warning({ message: '请选择原生 Codex 绑定账号' })
      return false
    }
    if (!editingKey.value && form.value.customKey && !/^[\x21-\x7E]+$/.test(form.value.customKey)) {
      ZNotification.warning({ message: '自定义 Key 只能包含 HTTP 可传输的可见字符，不能包含空格或换行' })
      return false
    }
    for (const [label, value] of [
      ['最大并发', form.value.maxConcurrency],
      ['每分钟请求数', form.value.requestsPerMinute],
    ] as const) {
      const parsed = Number(value)
      if (!Number.isSafeInteger(parsed) || parsed < 0) {
        ZNotification.warning({ message: `${label}必须是非负整数` })
        return false
      }
    }
    return true
  }

  function clearCustomKey() {
    form.value.customKey = ''
  }

  return { showFormModal, showAllAccountsConfirm, editingKey, savingKey, form, openCreate, openEdit, clearCustomKey, requestSave, confirmAllAccountsScope }
}

function emptyForm(): ApiKeyFormValue {
  return {
    providerRequestProfileOverrides: {},
    customKey: '',
    codexAccountId: '',
    codexAccessEnabled: false,
    codexAccessMode: 'fixed',
    name: '',
    label: '',
    groupIds: [],
    maxConcurrency: '',
    requestsPerMinute: '',
    dailyLimitUsd: '',
    weeklyLimitUsd: '',
  }
}

function profileOverrideUpdates(
  previous: ProviderRequestProfiles,
  current: ProviderRequestProfiles,
): ProviderRequestProfileUpdates {
  const updates: ProviderRequestProfileUpdates = cloneDeep(current)
  for (const provider of Object.keys(previous)) {
    if (!Object.hasOwn(current, provider))
      updates[provider] = null
  }
  return updates
}

function limitInputValue(limit: string | number) {
  return Number(limit) === 0 ? '' : String(limit)
}
