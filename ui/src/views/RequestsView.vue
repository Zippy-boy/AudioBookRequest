<script setup lang="ts">
import { onMounted, ref } from "vue";
import PageCard from "../components/PageCard.vue";
import StatusPill from "../components/StatusPill.vue";
import { api } from "../lib/api";
import { formatDate, formatDuration, joinList } from "../lib/format";
import { useToasts } from "../lib/toast";

type Audiobook = {
  asin: string;
  title: string;
  subtitle?: string | null;
  authors: string[];
  narrators: string[];
  series: string[];
  release_date: string;
  runtime_length_min: number;
  cover_image?: string | null;
  downloaded: boolean;
};

type RequestItem = {
  book: Audiobook;
  requests: Array<{ user_username: string; processing_status?: string }>;
  download_error?: string | null;
};

const items = ref<RequestItem[]>([]);
const loading = ref(false);
const error = ref("");
const { push } = useToasts();

async function loadRequests() {
  loading.value = true;
  error.value = "";
  try {
    items.value = await api.get<RequestItem[]>("/requests");
  } catch (err) {
    error.value = err instanceof Error ? err.message : String(err);
    push(error.value, "error");
  } finally {
    loading.value = false;
  }
}

async function removeRequest(asin: string) {
  try {
    await api.delete<void>(`/requests/${asin}`);
    items.value = items.value.filter((item) => item.book.asin !== asin);
    push(`Deleted request ${asin}`, "success");
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    push(message, "error");
  }
}

onMounted(loadRequests);
</script>

<template>
  <div class="page-stack">
    <PageCard title="Requests">
      <div v-if="loading" class="muted">Loading requests...</div>
      <div v-else-if="error" class="inline-error">{{ error }}</div>
      <div v-else-if="!items.length" class="empty-state">
        No requests found.
      </div>
      <div v-else class="results-list">
        <article v-for="item in items" :key="item.book.asin" class="result-card">
          <div class="result-cover" v-if="item.book.cover_image">
            <img :src="item.book.cover_image" :alt="item.book.title" />
          </div>
          <div class="result-body">
            <div class="result-head">
              <div>
                <h3>{{ item.book.title }}</h3>
                <p v-if="item.book.subtitle">{{ item.book.subtitle }}</p>
              </div>
              <StatusPill :tone="item.book.downloaded ? 'success' : 'warning'">
                {{ item.book.downloaded ? "Downloaded" : "Queued" }}
              </StatusPill>
            </div>
            <dl class="meta-grid">
              <div><dt>ASIN</dt><dd>{{ item.book.asin }}</dd></div>
              <div><dt>Requested by</dt><dd>{{ item.requests.map((request) => request.user_username).join(", ") }}</dd></div>
              <div><dt>Authors</dt><dd>{{ joinList(item.book.authors) }}</dd></div>
              <div><dt>Narrators</dt><dd>{{ joinList(item.book.narrators) }}</dd></div>
              <div><dt>Release</dt><dd>{{ formatDate(item.book.release_date) }}</dd></div>
              <div><dt>Length</dt><dd>{{ formatDuration(item.book.runtime_length_min) }}</dd></div>
            </dl>
            <p v-if="item.download_error" class="inline-error">{{ item.download_error }}</p>
            <div class="card-actions">
              <button class="ghost-button" type="button" @click="removeRequest(item.book.asin)">Delete</button>
            </div>
          </div>
        </article>
      </div>
    </PageCard>
  </div>
</template>
