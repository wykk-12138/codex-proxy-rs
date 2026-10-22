import type { ApiKey, ApiKeyCodexAccessMode } from '@/api'

import { defineTableColumns } from '@codex-proxy/ui'

// 列表与表单共用同一套模式文案
export const codexAccessModeLabels: Record<ApiKeyCodexAccessMode, string> = {
  fixed: '固定账号',
  session_pool: '会话池',
}

export const apiKeyColumns = defineTableColumns<ApiKey>([
  { key: 'selection', kind: 'selection' },
  { key: 'identity', label: '名称', kind: 'identity', size: '2xl', sortable: 'name' },
  { key: 'prefix', label: '密钥前缀', kind: 'mono', size: '2xl' },
  { key: 'enabled', label: '状态', kind: 'status', sortable: true },
  { key: 'scope', label: '分组', kind: 'status', size: 'lg' },
  { key: 'codexAccess', label: '原生 Codex', kind: 'status', size: 'md' },
  { key: 'budget', label: '额度使用', kind: 'text', size: 'xl' },
  { key: 'limits', label: '请求限制', kind: 'text', size: 'md' },
  { key: 'createdAtDisplay', label: '创建时间', kind: 'datetime', sortable: 'createdAt' },
  {
    key: 'lastUsedAt',
    label: '最后使用',
    kind: 'datetime',
    size: 'lg',
    sortable: true,
    emptyText: '',
  },
  { key: 'actions', label: '操作', kind: 'actions', size: 'lg' },
])
