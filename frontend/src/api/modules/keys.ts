import type { RequestOptions } from '../request'
import type { AccountGroupRef } from './groups'
import type { ClientProfileSelection, ProviderRequestProfile, ProviderRequestProfiles, XaiClientProfileSelection } from './settings/profiles'
import request from '../request'

export type ApiKeyRoutingScope = 'all' | 'groups'
export type ApiKeyBudgetPeriod = 'daily' | 'weekly' | 'all'

// 原生 Codex 访问模式：fixed 全部请求用入口账号，session_pool 按逻辑会话从授权池认领账号
export type ApiKeyCodexAccessMode = 'fixed' | 'session_pool'

// 原生 Codex 访问的持久绑定；建立后只能切换 enabled，模式与账号不能原地更换
export interface ApiKeyCodexAccess {
  enabled: boolean
  mode: ApiKeyCodexAccessMode
  // fixed 的实际执行账号，session_pool 的入口身份与账号级信息来源
  accountId: string
}

// 写入绑定的形状：创建时省略 mode 按 fixed，更新时省略 mode 保留既有模式
export interface ApiKeyCodexAccessWrite {
  enabled: boolean
  mode?: ApiKeyCodexAccessMode
  accountId: string
}

export interface ApiKey {
  codexAccess: ApiKeyCodexAccess | null
  providerRequestProfileOverrides: ProviderRequestProfiles
  openaiClientProfileOverride: ClientProfileSelection | null
  xaiClientProfileOverride: XaiClientProfileSelection | null

  id: string
  name: string
  label: string | null
  prefix: string
  enabled: boolean
  maxConcurrency: number
  requestsPerMinute: number
  dailyLimitUsd: string
  weeklyLimitUsd: string
  dailyUsedUsd: string
  weeklyUsedUsd: string
  dailyResetsAt: string | null
  dailyResetsAtDisplay: string | null
  weeklyResetsAt: string | null
  weeklyResetsAtDisplay: string | null
  createdAt: string
  createdAtDisplay: string
  updatedAt: string
  updatedAtDisplay: string
  lastUsedAt: string | null
  lastUsedAtDisplay: string
  lastUsedAtFullDisplay: string | null
  routingScope: ApiKeyRoutingScope
  groups: AccountGroupRef[]
  providerKinds: string[]
}

export interface ApiKeyListResponse {
  items: ApiKey[]
  nextCursor: string | null
  total: number
}

export interface ApiKeyCreateResponse {
  id: string
  prefix: string
  plaintextKey: string
}

export interface ApiKeyRevealResponse {
  id: string
  plaintextKey: string
}

export interface ApiKeyMutationResponse {
  id: string
}

// 请求参数类型：仅定义 API 边界的形状，调用方不依赖显式声明。
interface ApiKeyListParams {
  cursor?: string
  limit: number
  search?: string
  sortBy?: string
  sortDirection?: string
}

export interface ApiKeyWriteParam {
  name: string
  label: string | null
  groupIds: string[]
  maxConcurrency: number
  requestsPerMinute: number
  dailyLimitUsd: string
  weeklyLimitUsd: string
}

interface ApiKeyUpdateParam extends ApiKeyWriteParam {
  id: string
  providerRequestProfileOverrides: Record<string, ProviderRequestProfile | null>
  // 省略保留原绑定；已有绑定只接受同一 accountId 与 mode，省略 mode 即保留既有模式。
  codexAccess?: ApiKeyCodexAccessWrite
}

interface ApiKeyCreateParam extends ApiKeyWriteParam {
  providerRequestProfileOverrides: ProviderRequestProfiles
  customKey?: string
  codexAccess?: ApiKeyCodexAccessWrite | null
}

interface ApiKeyIdParam {
  id: string
}

interface ApiKeyResetBudgetParam extends ApiKeyIdParam {
  period: ApiKeyBudgetPeriod
}

export function getApiKeys(data: ApiKeyListParams, options: RequestOptions = {}) {
  return request<ApiKeyListResponse>({
    url: '/api/admin/client-keys',
    method: 'GET',
    params: data,
    ...options,
  })
}

export function createApiKey(data: ApiKeyCreateParam) {
  return request<ApiKeyCreateResponse>({
    url: '/api/admin/client-keys/create',
    method: 'POST',
    data,
  })
}

export function updateApiKey(data: ApiKeyUpdateParam) {
  return request<ApiKeyMutationResponse>({
    url: '/api/admin/client-keys/update',
    method: 'POST',
    data,
  })
}

export function revealApiKey(data: ApiKeyIdParam) {
  return request<ApiKeyRevealResponse>({
    url: '/api/admin/client-keys/reveal',
    method: 'GET',
    params: data,
  })
}

export function deleteApiKey(data: ApiKeyIdParam) {
  return request<ApiKeyMutationResponse>({
    url: '/api/admin/client-keys/delete',
    method: 'POST',
    data,
  })
}

export function resetApiKeyBudget(data: ApiKeyResetBudgetParam) {
  return request<ApiKeyMutationResponse>({
    url: '/api/admin/client-keys/reset-budget',
    method: 'POST',
    data,
  })
}

export function disableApiKey(data: ApiKeyIdParam) {
  return request<ApiKeyMutationResponse>({
    url: '/api/admin/client-keys/disable',
    method: 'POST',
    data,
  })
}

export function enableApiKey(data: ApiKeyIdParam) {
  return request<ApiKeyMutationResponse>({
    url: '/api/admin/client-keys/enable',
    method: 'POST',
    data,
  })
}
