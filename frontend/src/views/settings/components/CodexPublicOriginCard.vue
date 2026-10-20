<script setup lang="ts">
import { ZCard, ZForm, ZFormItem, ZInput } from '@codex-proxy/ui'

defineProps<{ error: string }>()
const model = defineModel<string>({ required: true })
</script>

<template>
  <ZCard
    title="原生 Codex 公共地址"
    description="Codex 客户端原生接入时使用的本站 HTTPS 地址，启用原生访问前需要配置"
  >
    <ZForm class="max-w-6xl">
      <ZFormItem
        label="公共地址"
        description="不含路径、参数和账号信息，留空表示未配置"
        :error="error"
      >
        <ZInput
          v-model="model"
          aria-label="原生 Codex 公共地址"
          placeholder="https://codex.example.com"
        />
      </ZFormItem>
    </ZForm>
  </ZCard>
</template>
