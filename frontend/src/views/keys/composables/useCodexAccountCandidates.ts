import type { AccountGroupRef } from '@/api'
import { onMounted, shallowRef } from 'vue'
import { getAccounts } from '@/api'
import { useRequestState } from '@/composables/useRequestState'

export interface CodexAccountCandidate {
  id: string
  label: string
  groups: AccountGroupRef[]
}

// 原生绑定的候选账号只取已识别身份的 OpenAI OAuth 账号，分组范围最终由服务端校验
export function useCodexAccountCandidates(options: { immediate?: boolean } = {}) {
  const accounts = shallowRef<CodexAccountCandidate[]>([])
  const request = useRequestState()
  const { loading } = request

  async function loadAccounts() {
    const requestId = request.start()
    const requestOptions = { signal: request.signal }
    try {
      const first = await getAccounts({ page: 1, pageSize: 200, provider: 'openai' }, requestOptions)
      if (!request.isCurrent(requestId))
        return []
      const items = [...first.items]
      for (let page = 2; page <= first.page.totalPages; page += 1) {
        const result = await getAccounts({ page, pageSize: first.page.pageSize, provider: 'openai' }, requestOptions)
        if (!request.isCurrent(requestId))
          return []
        items.push(...result.items)
      }
      accounts.value = items
        .filter(account => account.authenticationKind === 'oauth' && account.userId && account.accountId)
        .map(account => ({
          id: account.id,
          label: account.email && account.email !== account.name
            ? `${account.name} (${account.email})`
            : account.name,
          groups: account.groups,
        }))
      return accounts.value
    }
    catch {
      return []
    }
    finally {
      request.finish(requestId)
    }
  }

  if (options.immediate !== false)
    onMounted(() => void loadAccounts())

  return { accounts, loading, loadAccounts }
}
