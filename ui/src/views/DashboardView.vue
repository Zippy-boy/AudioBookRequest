<script setup lang="ts">
import { onMounted, ref } from "vue";
import PageCard from "../components/PageCard.vue";
import { api } from "../lib/api";
import { formatNumber } from "../lib/format";
import { useToasts } from "../lib/toast";

type Stats = {
  requests: number;
  downloaded: number;
  manual: number;
  downloading: number;
  attention: number;
};

const { push } = useToasts();
const stats = ref<Stats | null>(null);
const loading = ref(false);
const error = ref("");

async function loadStats() {
  loading.value = true;
  error.value = "";
  try {
    stats.value = await api.get<Stats>("/settings/stats");
  } catch (err) {
    error.value = err instanceof Error ? err.message : String(err);
    push(error.value, "error");
  } finally {
    loading.value = false;
  }
}

onMounted(loadStats);

const cards = [
  { label: "Requests", key: "requests" as const },
  { label: "Downloaded", key: "downloaded" as const },
  { label: "Manual", key: "manual" as const },
  { label: "Downloading", key: "downloading" as const },
  { label: "Attention", key: "attention" as const },
];
</script>

<template>
  <div class="page-stack">
    <PageCard title="Dashboard">
      <div v-if="loading" class="muted">Loading stats...</div>
      <div v-else-if="error" class="inline-error">{{ error }}</div>
      <div v-else class="stats-grid">
        <div v-for="card in cards" :key="card.key" class="stat-card">
          <div class="stat-label">{{ card.label }}</div>
          <div class="stat-value">{{ formatNumber(stats?.[card.key]) }}</div>
        </div>
      </div>
    </PageCard>
  </div>
</template>
