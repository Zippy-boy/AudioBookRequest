<script setup lang="ts">
import { useToasts } from "../lib/toast";

const { items, remove } = useToasts();
</script>

<template>
  <div class="toast-stack" aria-live="polite" aria-atomic="true">
    <button
      v-for="toast in items"
      :key="toast.id"
      class="toast"
      :class="toast.type"
      type="button"
      @click="remove(toast.id)"
    >
      {{ toast.message }}
    </button>
  </div>
</template>
