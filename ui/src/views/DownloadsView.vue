<script setup lang="ts">
import { onMounted, ref } from "vue";
import PageCard from "../components/PageCard.vue";
import StatusPill from "../components/StatusPill.vue";
import { api } from "../lib/api";
import { formatNumber } from "../lib/format";
import { useToasts } from "../lib/toast";

type DownloadItem = {
  asin: string;
  title: string;
  subtitle?: string | null;
  status: string;
  progress: number;
  torrent_hash?: string | null;
  download_state?: string | null;
  downloaded: boolean;
};

const items = ref<DownloadItem[]>([]);
const loading = ref(false);
const error = ref("");
const { push } = useToasts();

async function loadDownloads() {
  loading.value = true;
  error.value = "";
  try {
    items.value = await api.get<DownloadItem[]>("/downloads");
  } catch (err) {
    error.value = err instanceof Error ? err.message : String(err);
    push(error.value, "error");
  } finally {
    loading.value = false;
  }
}

const statusTone: Record<string, "neutral" | "success" | "warning" | "danger" | "info"> = {
  completed: "success",
  queued: "info",
  pending: "warning",
  download_initiated: "info",
  organizing_files: "warning",
  generating_metadata: "warning",
  saving_cover: "warning",
  review_required: "danger",
};

onMounted(loadDownloads);
</script>

<template>
  <div class="page-stack">
    <PageCard title="Downloads">
      <div v-if="loading" class="muted">Loading downloads...</div>
      <div v-else-if="error" class="inline-error">{{ error }}</div>
      <div v-else-if="!items.length" class="empty-state">
        No downloads found.
      </div>
      <div v-else class="table-list">
        <div class="table-row table-head">
          <span>Title</span>
          <span>Status</span>
          <span>Progress</span>
          <span>Hash</span>
        </div>
        <div v-for="item in items" :key="item.asin" class="table-row">
          <span>
            <strong>{{ item.title }}</strong>
            <small>{{ item.asin }}</small>
          </span>
          <span>
            <StatusPill :tone="statusTone[item.status] ?? 'neutral'">{{ item.status }}</StatusPill>
          </span>
          <span>{{ formatNumber(Math.round(item.progress * 100)) }}%</span>
          <span class="mono">{{ item.torrent_hash ?? "-" }}</span>
        </div>
      </div>
    </PageCard>
  </div>
</template>
