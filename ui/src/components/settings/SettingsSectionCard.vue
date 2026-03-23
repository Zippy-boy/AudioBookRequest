<script setup lang="ts">
import StatusPill from "../StatusPill.vue";
import type { SectionTone } from "../../lib/settings";

defineProps<{
  title: string;
  endpoint: string;
  tone: SectionTone;
  statusText: string;
  error?: string;
}>();
</script>

<template>
  <section class="panel settings-section">
    <div class="panel-header settings-header">
      <div>
        <h2>{{ title }}</h2>
        <p class="mono">{{ endpoint }}</p>
      </div>
      <div class="settings-header-actions">
        <StatusPill :tone="tone">{{ statusText }}</StatusPill>
        <slot name="actions" />
      </div>
    </div>

    <div v-if="error" class="inline-error settings-error">
      {{ error }}
    </div>

    <div class="settings-body">
      <slot />
    </div>
  </section>
</template>

<style scoped>
.settings-header {
  gap: 0.65rem;
}

.settings-header-actions {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 0.35rem;
  flex-wrap: wrap;
}

.settings-error {
  margin-bottom: 0.5rem;
}

.settings-body {
  display: grid;
  gap: 0.5rem;
}
</style>
