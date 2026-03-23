<script setup lang="ts">
import { ref, onMounted } from "vue";
import { api } from "../lib/api";

const props = defineProps<{
  modelValue: string;
}>();

const emit = defineEmits<{
  (e: "update:modelValue", value: string): void;
}>();

interface BrowseResponse {
  current_path: string;
  parent_path: string | null;
  directories: string[];
}

const showPicker = ref(false);
const currentPath = ref("");
const directories = ref<string[]>([]);
const parentPath = ref<string | null>(null);
const loading = ref(false);

async function browse(path: string) {
  loading.value = true;
  try {
    const res = await api.post<BrowseResponse>("/system/browse", { path });
    currentPath.value = res.current_path;
    parentPath.value = res.parent_path;
    directories.value = res.directories;
  } catch (error) {
    console.error("Failed to browse directories", error);
  } finally {
    loading.value = false;
  }
}

function openPicker() {
  showPicker.value = true;
  browse(props.modelValue);
}

function selectCurrent() {
  emit("update:modelValue", currentPath.value);
  showPicker.value = false;
}

function navigateTo(dir: string) {
  const separator = currentPath.value.includes("\\") ? "\\" : "/";
  const newPath = currentPath.value.endsWith(separator) 
    ? currentPath.value + dir 
    : currentPath.value + separator + dir;
  browse(newPath);
}

function navigateUp() {
  if (parentPath.value) {
    browse(parentPath.value);
  }
}
</script>

<template>
  <div class="folder-picker-container">
    <div class="input-with-button">
      <input
        :value="modelValue"
        class="text-input compact-input"
        type="text"
        @input="$emit('update:modelValue', ($event.target as HTMLInputElement).value)"
        placeholder="/path/to/folder"
      />
      <button class="ghost-button" type="button" @click="openPicker">
        Browse
      </button>
    </div>

    <div v-if="showPicker" class="picker-overlay" @click.self="showPicker = false">
      <div class="picker-dialog">
        <div class="picker-header">
          <h3>Select Folder</h3>
          <button class="close-button" @click="showPicker = false">&times;</button>
        </div>
        
        <div class="picker-path">
          <span class="muted">Path:</span> {{ currentPath }}
        </div>

        <div class="picker-body" :class="{ 'is-loading': loading }">
          <div v-if="parentPath !== null" class="dir-item parent-dir" @click="navigateUp">
            <span class="dir-icon">⬆️</span>
            <span class="dir-name">.. (Parent Directory)</span>
          </div>
          
          <div v-for="dir in directories" :key="dir" class="dir-item" @click="navigateTo(dir)">
            <span class="dir-icon">📁</span>
            <span class="dir-name">{{ dir }}</span>
          </div>

          <div v-if="!loading && directories.length === 0 && parentPath === null" class="empty-state">
            No directories found or access denied.
          </div>
        </div>

        <div class="picker-footer">
          <button class="ghost-button" @click="showPicker = false">Cancel</button>
          <button class="primary-button" @click="selectCurrent">Select This Folder</button>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.folder-picker-container {
  width: 100%;
}

.input-with-button {
  display: flex;
  gap: 0.35rem;
}

.picker-overlay {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0, 0, 0, 0.75);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
  padding: 1rem;
}

.picker-dialog {
  background: #1a222c;
  border: 1px solid var(--border);
  border-radius: 0.5rem;
  width: 100%;
  max-width: 500px;
  max-height: 80vh;
  display: flex;
  flex-direction: column;
  box-shadow: 0 10px 25px rgba(0, 0, 0, 0.5);
}

.picker-header {
  padding: 0.75rem 1rem;
  border-bottom: 1px solid var(--border);
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.picker-header h3 {
  margin: 0;
  font-size: 1rem;
}

.close-button {
  background: none;
  border: none;
  color: var(--text-muted);
  font-size: 1.5rem;
  cursor: pointer;
}

.picker-path {
  padding: 0.5rem 1rem;
  background: #131921;
  font-family: monospace;
  font-size: 0.8rem;
  border-bottom: 1px solid var(--border);
  word-break: break-all;
}

.picker-body {
  flex: 1;
  overflow-y: auto;
  padding: 0.5rem;
  min-height: 200px;
}

.dir-item {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.5rem;
  border-radius: 0.25rem;
  cursor: pointer;
  transition: background 0.1s;
}

.dir-item:hover {
  background: rgba(255, 255, 255, 0.05);
}

.dir-icon {
  font-size: 1.1rem;
}

.dir-name {
  font-size: 0.9rem;
}

.empty-state {
  padding: 2rem;
  text-align: center;
  color: var(--text-muted);
  font-size: 0.9rem;
}

.picker-footer {
  padding: 0.75rem 1rem;
  border-top: 1px solid var(--border);
  display: flex;
  justify-content: flex-end;
  gap: 0.5rem;
}

.is-loading {
  opacity: 0.5;
  pointer-events: none;
}
</style>
