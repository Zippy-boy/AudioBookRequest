<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { useRouter } from "vue-router";
import PageCard from "../components/PageCard.vue";
import { login } from "../lib/api";

const router = useRouter();

const username = ref("");
const password = ref("");
const pending = ref(false);
const error = ref("");

const canSubmit = computed(() => Boolean(username.value.trim() && password.value));

watch([username, password], () => {
  if (error.value) {
    error.value = "";
  }
});

async function handleSubmit() {
  if (pending.value || !canSubmit.value) {
    return;
  }

  pending.value = true;
  error.value = "";

  try {
    await login(username.value.trim(), password.value);
    await router.replace("/dashboard");
  } catch (err) {
    error.value = err instanceof Error ? err.message : String(err);
  } finally {
    pending.value = false;
  }
}
</script>

<template>
  <main class="login-page">
    <section class="login-shell">
      <div class="login-brand">
        <div class="brand-mark">N</div>
        <div>
          <div class="brand-title">Narrarr</div>
          <div class="brand-subtitle">Audiobook control center</div>
        </div>
      </div>

      <PageCard
        title="Sign in"
        subtitle="Use your Narrarr credentials to unlock the dashboard and settings."
      >
        <form class="login-form" @submit.prevent="handleSubmit">
          <label class="field">
            <span class="field-label">Username</span>
            <input
              v-model="username"
              class="text-input"
              name="username"
              type="text"
              autocomplete="username"
              spellcheck="false"
              placeholder="Username"
            />
          </label>

          <label class="field">
            <span class="field-label">Password</span>
            <input
              v-model="password"
              class="text-input"
              name="password"
              type="password"
              autocomplete="current-password"
              placeholder="Password"
            />
          </label>

          <div v-if="error" class="inline-error">{{ error }}</div>

          <button class="primary-button login-submit" type="submit" :disabled="pending || !canSubmit">
            {{ pending ? "Signing in..." : "Sign in" }}
          </button>
        </form>

        <p class="login-hint muted small">
          Your session key is stored in this browser and used for protected API requests.
        </p>
      </PageCard>
    </section>
  </main>
</template>
