<script setup lang="ts">
import { computed, watch } from "vue";
import { useRoute, useRouter } from "vue-router";
import ShellLayout from "./components/ShellLayout.vue";
import ToastStack from "./components/ToastStack.vue";
import { isAuthenticated } from "./lib/api";

const route = useRoute();
const router = useRouter();

const showShell = computed(() => route.meta.hideShell !== true);
const authenticated = computed(() => isAuthenticated());

watch([authenticated, () => route.meta.requiresAuth], ([isAuth, requiresAuth]) => {
  if (!isAuth && requiresAuth !== false && route.path !== "/login" && route.path !== "/setup") {
    void router.replace("/login");
  }
});
</script>

<template>
  <ShellLayout v-if="showShell">
    <router-view />
  </ShellLayout>
  <router-view v-else />
  <ToastStack />
</template>
