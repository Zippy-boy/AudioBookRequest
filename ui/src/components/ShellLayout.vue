<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { RouterLink, useRoute, useRouter } from "vue-router";
import { isAuthenticated, logout } from "../lib/api";

const route = useRoute();
const router = useRouter();
const navOpen = ref(false);
const canLogout = computed(() => isAuthenticated());

const navItems = [
  { label: "Dashboard", to: "/dashboard" },
  { label: "Search", to: "/search" },
  { label: "Requests", to: "/requests" },
  { label: "Downloads", to: "/downloads" },
  { label: "Recommendations", to: "/recommendations" },
  { label: "Settings", to: "/settings" },
];

const title = computed(() => {
  const active = navItems.find((item) => item.to === route.path);
  return active?.label ?? "Narrarr";
});

async function handleLogout() {
  logout();
  await router.push("/login");
}

watch(
  () => route.fullPath,
  () => {
    navOpen.value = false;
  }
);
</script>

<template>
  <div class="shell">
    <aside class="sidebar" :class="{ open: navOpen }">
      <div class="brand">
        <div class="brand-mark">N</div>
        <div>
          <div class="brand-title">Narrarr</div>
          <div class="brand-subtitle">Audiobook control center</div>
        </div>
      </div>

      <nav class="nav">
        <RouterLink
          v-for="item in navItems"
          :key="item.to"
          :to="item.to"
          class="nav-link"
          active-class="active"
        >
          {{ item.label }}
        </RouterLink>
      </nav>
    </aside>

    <div class="shell-main">
      <header class="topbar">
        <button class="menu-button" type="button" @click="navOpen = !navOpen" aria-label="Toggle navigation">
          Menu
        </button>
        <div>
          <div class="eyebrow">Control Panel</div>
          <h1>{{ title }}</h1>
        </div>
        <div class="header-meta">
          <button v-if="canLogout" class="ghost-button" type="button" @click="handleLogout">
            Log out
          </button>
        </div>
      </header>

      <main class="content">
        <slot />
      </main>
    </div>
  </div>
</template>
