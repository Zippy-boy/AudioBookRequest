<script setup lang="ts">
defineProps<{
  tone?: "neutral" | "success" | "warning" | "danger" | "info";
}>();
</script>

<template>
  <span class="pill" :class="tone ?? 'neutral'">
    <slot />
  </span>
</template>
