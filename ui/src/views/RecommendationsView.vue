<script setup lang="ts">
import { onMounted, ref } from "vue";
import PageCard from "../components/PageCard.vue";
import StatusPill from "../components/StatusPill.vue";
import { api } from "../lib/api";
import { formatDate, formatDuration, formatNumber, joinList } from "../lib/format";
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

type PopularRecommendation = {
  book: Audiobook;
  request_count: number;
};

const items = ref<PopularRecommendation[]>([]);
const loading = ref(false);
const error = ref("");
const { push } = useToasts();

async function loadRecommendations() {
  loading.value = true;
  error.value = "";
  try {
    items.value = await api.get<PopularRecommendation[]>("/recommendations/popular");
  } catch (err) {
    error.value = err instanceof Error ? err.message : String(err);
    push(error.value, "error");
  } finally {
    loading.value = false;
  }
}

onMounted(loadRecommendations);
</script>

<template>
  <div class="page-stack">
    <PageCard title="Recommendations">
      <div v-if="loading" class="muted">Loading recommendations...</div>
      <div v-else-if="error" class="inline-error">{{ error }}</div>
      <div v-else-if="!items.length" class="empty-state">
        No recommendations available.
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
              <StatusPill tone="info">{{ formatNumber(item.request_count) }} requests</StatusPill>
            </div>
            <dl class="meta-grid">
              <div><dt>ASIN</dt><dd>{{ item.book.asin }}</dd></div>
              <div><dt>Authors</dt><dd>{{ joinList(item.book.authors) }}</dd></div>
              <div><dt>Narrators</dt><dd>{{ joinList(item.book.narrators) }}</dd></div>
              <div><dt>Series</dt><dd>{{ joinList(item.book.series) }}</dd></div>
              <div><dt>Release</dt><dd>{{ formatDate(item.book.release_date) }}</dd></div>
              <div><dt>Length</dt><dd>{{ formatDuration(item.book.runtime_length_min) }}</dd></div>
            </dl>
          </div>
        </article>
      </div>
    </PageCard>
  </div>
</template>
