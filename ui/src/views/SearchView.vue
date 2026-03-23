<script setup lang="ts">
import { computed, ref, watch } from "vue";
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
  series_index?: string | null;
  publisher?: string | null;
  description?: string | null;
  language?: string | null;
  cover_image?: string | null;
  release_date: string;
  runtime_length_min: number;
  downloaded: boolean;
};

const query = ref("");
const results = ref<Audiobook[]>([]);
const loading = ref(false);
const searching = ref(false);
const error = ref("");
const requested = ref<Record<string, boolean>>({});
const { push } = useToasts();

const queryLabel = computed(() => query.value.trim() || "Enter a title, author, or ASIN");

async function search() {
  const q = query.value.trim();
  if (!q) {
    results.value = [];
    return;
  }
  loading.value = true;
  error.value = "";
  try {
    results.value = await api.get<Audiobook[]>(`/search?q=${encodeURIComponent(q)}`);
  } catch (err) {
    error.value = err instanceof Error ? err.message : String(err);
    push(error.value, "error");
  } finally {
    loading.value = false;
  }
}

let debounceHandle: number | undefined;
watch(query, () => {
  window.clearTimeout(debounceHandle);
  debounceHandle = window.setTimeout(() => {
    void search();
  }, 350);
});

async function requestBook(asin: string) {
  searching.value = true;
  try {
    await api.post<void>(`/requests/${asin}`);
    requested.value = { ...requested.value, [asin]: true };
    push(`Request created for ${asin}`, "success");
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    push(message, "error");
  } finally {
    searching.value = false;
  }
}
</script>

<template>
  <div class="page-stack">
    <PageCard title="Search">
      <div class="search-row">
        <input
          v-model="query"
          class="text-input"
          type="search"
          placeholder="Search by title, author, or ASIN"
        />
        <button class="primary-button" type="button" @click="search">Search</button>
      </div>
      <div class="muted small">Current query: {{ queryLabel }}</div>
    </PageCard>

    <PageCard title="Results">
      <div v-if="loading" class="muted">Searching...</div>
      <div v-else-if="error" class="inline-error">{{ error }}</div>
      <div v-else-if="!results.length" class="empty-state">
        No search results yet.
      </div>
      <div v-else class="results-list">
        <article v-for="book in results" :key="book.asin" class="result-card">
          <div class="result-cover" v-if="book.cover_image">
            <img :src="book.cover_image" :alt="book.title" />
          </div>
          <div class="result-body">
            <div class="result-head">
              <div>
                <h3>{{ book.title }}</h3>
                <p v-if="book.subtitle">{{ book.subtitle }}</p>
              </div>
              <StatusPill :tone="book.downloaded ? 'success' : 'neutral'">
                {{ book.downloaded ? "Downloaded" : "Available" }}
              </StatusPill>
            </div>
            <dl class="meta-grid">
              <div><dt>ASIN</dt><dd>{{ book.asin }}</dd></div>
              <div><dt>Authors</dt><dd>{{ joinList(book.authors) }}</dd></div>
              <div><dt>Narrators</dt><dd>{{ joinList(book.narrators) }}</dd></div>
              <div><dt>Series</dt><dd>{{ joinList(book.series) }}</dd></div>
              <div><dt>Release</dt><dd>{{ formatDate(book.release_date) }}</dd></div>
              <div><dt>Length</dt><dd>{{ formatDuration(book.runtime_length_min) }}</dd></div>
            </dl>
            <p class="description" v-if="book.description">{{ book.description }}</p>
            <div class="card-actions">
              <button
                class="primary-button"
                type="button"
                :disabled="searching || requested[book.asin]"
                @click="requestBook(book.asin)"
              >
                {{ requested[book.asin] ? "Requested" : "Request" }}
              </button>
            </div>
          </div>
        </article>
      </div>
    </PageCard>
  </div>
</template>
