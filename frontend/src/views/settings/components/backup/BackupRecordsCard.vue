<script setup lang="ts">
import type { BackupRecord } from '@/api'

import { defineTableColumns, ZButton, ZCard, ZConfirmDialog, ZIconButton, ZPagination, ZPopover, ZTable } from '@codex-proxy/ui'
import { CircleHelp, Download, Play, RefreshCw, Trash2 } from '@lucide/vue'

import BackupStatusBadge from './BackupStatusBadge.vue'

defineProps<{
  records: BackupRecord[]
  page: number
  pageSize: number
  total: number
  loading: boolean
  error: string
  activeBackup: boolean
  creating: boolean
  refreshing: boolean
  deleting: boolean
  deleteTarget: BackupRecord | null
  downloadStates: Record<string, boolean>
}>()

const emit = defineEmits<{
  pageChange: [page: number]
  pageSizeChange: [pageSize: number]
  create: []
  refresh: []
  download: [record: BackupRecord]
  requestDelete: [record: BackupRecord]
  confirmDelete: []
}>()
const deleteOpen = defineModel<boolean>('deleteOpen', { required: true })

const columns = defineTableColumns<BackupRecord>([
  { key: 'id', label: 'ID', kind: 'mono', size: 'lg' },
  { key: 'status', label: '状态', kind: 'status' },
  { key: 'fileName', label: '文件名', kind: 'text' },
  { key: 'sizeBytes', label: '大小', kind: 'numeric' },
  { key: 'expiresAt', label: '过期时间', kind: 'datetime' },
  { key: 'trigger', label: '触发方式', kind: 'status' },
  { key: 'startedAt', label: '开始时间', kind: 'datetime' },
  { key: 'actions', label: '操作', kind: 'actions' },
])

function triggerLabel(value: string): string {
  return value === 'manual' ? '手动' : '计划'
}

function formatSize(value: unknown): string {
  const bytes = Number(value)
  if (!Number.isFinite(bytes) || bytes <= 0)
    return '—'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let index = 0
  let size = bytes
  while (size >= 1024 && index < units.length - 1) {
    size /= 1024
    index += 1
  }
  return `${size >= 100 || index === 0 ? Math.round(size) : size.toFixed(1)} ${units[index]}`
}

/** 从对象 key 提取文件名（最后一段）。 */
function formatFileName(record: BackupRecord): string {
  if (!record.objectKey)
    return '—'
  return record.objectKey.split('/').pop() || '—'
}

/** 记录 ID 的短标识：`backup_` 前缀 + 前 8 位 hex，与对象 key 文件名一致；完整值通过 title 展示。 */
function shortId(id: string): string {
  const hex = id.startsWith('backup_') ? id.slice('backup_'.length) : id
  return hex.length > 8 ? `backup_${hex.slice(0, 8)}` : id
}

function canDownload(record: BackupRecord): boolean {
  return record.status === 'completed'
}

function canDelete(record: BackupRecord): boolean {
  return record.status === 'completed' || record.status === 'failed'
}
</script>

<template>
  <ZCard description="不包含请求明细与日志">
    <template #title>
      <span class="inline-flex items-center gap-1.5">
        <span>备份记录</span>
        <ZPopover trigger="hover-click" placement="bottom-start">
          <template #reference="{ open }">
            <ZIconButton aria-label="备份范围说明" :title="undefined" :aria-expanded="open" class="size-6! hover:bg-transparent! active:bg-transparent!">
              <CircleHelp class="size-3.5" aria-hidden="true" />
            </ZIconButton>
          </template>
          <div class="max-w-72 space-y-2 px-3 py-2 text-cp-sm leading-relaxed text-cp-text-secondary">
            <p>保留账号、Key、设置、插件及已用额度</p>
            <p>用量保留小时汇总</p>
          </div>
        </ZPopover>
      </span>
    </template>
    <template #actions>
      <div class="flex flex-wrap items-center gap-2">
        <ZButton
          type="primary"
          :loading="creating"
          :disabled="loading || activeBackup"
          @click="emit('create')"
        >
          <template #icon>
            <Play class="size-4" />
          </template>
          {{ creating ? '创建中...' : '创建备份' }}
        </ZButton>
        <ZButton
          :loading="refreshing"
          :disabled="loading"
          @click="emit('refresh')"
        >
          <template #loading>
            <RefreshCw class="size-4 animate-spin motion-reduce:animate-none" />
          </template>
          <template #icon>
            <RefreshCw class="size-4" />
          </template>
          刷新
        </ZButton>
      </div>
    </template>

    <div class="flex min-h-0 flex-1 flex-col">
      <ZTable
        class="min-h-0 flex-1"
        :columns="columns"
        :data="records"
        row-key="id"
        :loading="loading"
        empty-text="暂无备份记录"
      >
        <template #id="{ row }">
          <span class="text-cp-text-secondary" :title="row.id">{{ shortId(row.id) }}</span>
        </template>

        <template #status="{ row }">
          <BackupStatusBadge :status="row.status" />
        </template>

        <template #fileName="{ row }">
          <span class="text-cp-text-secondary" :title="row.objectKey">{{ formatFileName(row) }}</span>
        </template>

        <template #sizeBytes="{ row }">
          <span class="text-cp-text-secondary">{{ formatSize(row.sizeBytes) }}</span>
        </template>

        <template #expiresAt="{ row }">
          <span class="text-cp-text-secondary">
            {{ row.expiresAtDisplay ?? '—' }}
          </span>
        </template>

        <template #reference="{ row }">
          <span class="text-cp-text-secondary">
            {{ triggerLabel(row.triggerKind) }}
          </span>
        </template>

        <template #startedAt="{ row }">
          <span class="text-cp-text-secondary">
            {{ row.startedAtDisplay ?? '—' }}
          </span>
        </template>

        <template #actions="{ row }">
          <div class="flex items-center justify-start gap-1">
            <ZIconButton
              :disabled="!canDownload(row)"
              :loading="Boolean(downloadStates[row.id])"
              aria-label="下载备份"
              @click="emit('download', row)"
            >
              <Download class="size-4" />
            </ZIconButton>
            <ZIconButton
              :disabled="!canDelete(row)"
              aria-label="删除备份"
              @click="emit('requestDelete', row)"
            >
              <Trash2 class="size-4" />
            </ZIconButton>
          </div>
        </template>
      </ZTable>
      <ZPagination
        v-bind="{ currentPage: page, pageSize, total }"
        :disabled="loading"
        @current-change="emit('pageChange', $event)"
        @size-change="emit('pageSizeChange', $event)"
      />
    </div>

    <ZConfirmDialog
      v-model="deleteOpen"
      title="删除备份"
      description="将删除远端对象并移除记录，此操作不可撤销"
      destructive
      confirm-text="确认删除"
      :loading="deleting"
      @confirm="emit('confirmDelete')"
    >
      <p class="m-0">
        确定要删除备份 <span class="font-emphasis">{{ deleteTarget?.id }}</span> 吗？
      </p>
    </ZConfirmDialog>
  </ZCard>
</template>
