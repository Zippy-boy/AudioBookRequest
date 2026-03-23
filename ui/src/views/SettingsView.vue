<script setup lang="ts">
import { computed, onMounted, reactive } from "vue";
import PageCard from "../components/PageCard.vue";
import StatusPill from "../components/StatusPill.vue";
import SettingsSectionCard from "../components/settings/SettingsSectionCard.vue";
import FolderPicker from "../components/FolderPicker.vue";
import { api } from "../lib/api";
import {
  completeActionOptions,
  loginTypeOptions,
  parseIntegerInput,
  parseRangeInput,
  toAudiobookshelfForm,
  toDownloadClientForm,
  toDownloadForm,
  toMediaManagementForm,
  toProwlarrForm,
  toSecurityForm,
  toUrlSearchParams,
  type AccountApiKeySummary,
  type AccountCreateApiKeyResponse,
  type AudiobookshelfSettingsResponse,
  type DownloadClientSettingsResponse,
  type DownloadSettingsResponse,
  type MediaManagementSettingsResponse,
  type ProwlarrSettingsResponse,
  type SectionTone,
  type SecuritySettingsResponse,
} from "../lib/settings";
import { useToasts } from "../lib/toast";

const { push } = useToasts();

type SectionState = {
  loading: boolean;
  saving: boolean;
  loaded: boolean;
  error: string;
  baseline: string;
};

function createState(): SectionState {
  return {
    loading: false,
    saving: false,
    loaded: false,
    error: "",
    baseline: "",
  };
}

function snapshotForm(value: unknown) {
  return JSON.stringify(value);
}

function isDirty(section: SectionState & { form: unknown }) {
  return section.loaded && snapshotForm(section.form) !== section.baseline;
}

function markSynced(section: SectionState & { form: unknown }) {
  section.loaded = true;
  section.baseline = snapshotForm(section.form);
}

function sectionTone(section: SectionState & { form: unknown }): SectionTone {
  if (section.error) {
    return "danger";
  }
  if (section.loading || section.saving) {
    return "warning";
  }
  if (isDirty(section)) {
    return "info";
  }
  return section.loaded ? "success" : "neutral";
}

function sectionStatus(section: SectionState & { form: unknown }) {
  if (section.saving) {
    return "Saving";
  }
  if (section.loading) {
    return "Loading";
  }
  if (section.error) {
    return "Error";
  }
  if (isDirty(section)) {
    return "Unsaved";
  }
  return section.loaded ? "Loaded" : "Idle";
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}

const prowlarr = reactive(
  Object.assign(createState(), {
    response: null as ProwlarrSettingsResponse | null,
    form: toProwlarrForm(),
  })
);

const download = reactive(
  Object.assign(createState(), {
    response: null as DownloadSettingsResponse | null,
    form: toDownloadForm(),
  })
);

const downloadClient = reactive(
  Object.assign(createState(), {
    response: null as DownloadClientSettingsResponse | null,
    form: toDownloadClientForm(),
  })
);

const mediaManagement = reactive(
  Object.assign(createState(), {
    response: null as MediaManagementSettingsResponse | null,
    form: toMediaManagementForm(),
  })
);

const audiobookshelf = reactive(
  Object.assign(createState(), {
    response: null as AudiobookshelfSettingsResponse | null,
    form: toAudiobookshelfForm(),
  })
);

const security = reactive(
  Object.assign(createState(), {
    response: null as SecuritySettingsResponse | null,
    form: toSecurityForm(),
  })
);

const accountKeys = reactive(
  Object.assign(createState(), {
    items: [] as AccountApiKeySummary[],
    newName: "",
    latestKey: "",
    latestName: "",
  })
);

const securityLoginOptions = computed(() => {
  const current = String(security.form.loginType);
  const hasCurrent = loginTypeOptions.some((option) => option.value === current);
  if (hasCurrent) {
    return loginTypeOptions;
  }
  return [{ value: current, label: `${current} (current)` }, ...loginTypeOptions];
});

const downloadClientActionOptions = computed(() => {
  const current = String(downloadClient.form.qbitCompleteAction);
  const hasCurrent = completeActionOptions.includes(
    current as (typeof completeActionOptions)[number]
  );
  const baseOptions = completeActionOptions.map((value) => ({ value, label: value }));
  if (hasCurrent) {
    return baseOptions;
  }
  return [{ value: current, label: `${current} (current)` }, ...baseOptions];
});

const absLibraryOptions = computed(() => {
  const libraries = audiobookshelf.response?.abs_libraries ?? [];
  const current = audiobookshelf.form.libraryId;
  if (current && !libraries.some((library) => library.id === current)) {
    return [{ id: current, name: `${current} (current)` }, ...libraries];
  }
  return libraries;
});

const prowlarrStatusText = computed(() => {
  if (prowlarr.saving) return "Saving";
  if (prowlarr.loading) return "Loading";
  if (prowlarr.error) return "Error";
  if (!prowlarr.loaded) return "Idle";
  if (isDirty(prowlarr)) return "Unsaved";
  const state = prowlarr.response?.indexers?.state;
  if (state === "ok") return "Connected";
  if (state === "missingUrlKey" || state === "failedFetch") return "Unavailable";
  return "Loaded";
});

const prowlarrTone = computed<SectionTone>(() => {
  if (prowlarr.saving || prowlarr.loading) return "warning";
  if (prowlarr.error) return "danger";
  if (isDirty(prowlarr)) return "info";
  const state = prowlarr.response?.indexers?.state;
  if (state === "ok") return "success";
  if (state === "missingUrlKey" || state === "failedFetch") return "danger";
  return prowlarr.loaded ? "info" : "neutral";
});

const absStatusText = computed(() => {
  if (audiobookshelf.saving) return "Saving";
  if (audiobookshelf.loading) return "Loading";
  if (audiobookshelf.error) return "Error";
  if (!audiobookshelf.loaded) return "Idle";
  if (isDirty(audiobookshelf)) return "Unsaved";
  const hasBase = Boolean(audiobookshelf.form.baseUrl.trim());
  const hasToken = Boolean(audiobookshelf.form.apiToken.trim());
  const libraryCount = audiobookshelf.response?.abs_libraries.length ?? 0;
  if (!hasBase || !hasToken) return "Missing config";
  if (libraryCount > 0) return "Connected";
  return "Unavailable";
});

const absTone = computed<SectionTone>(() => {
  if (audiobookshelf.saving || audiobookshelf.loading) return "warning";
  if (audiobookshelf.error) return "danger";
  if (isDirty(audiobookshelf)) return "info";
  const hasBase = Boolean(audiobookshelf.form.baseUrl.trim());
  const hasToken = Boolean(audiobookshelf.form.apiToken.trim());
  const libraryCount = audiobookshelf.response?.abs_libraries.length ?? 0;
  if (!hasBase || !hasToken) return "neutral";
  if (libraryCount > 0) return "success";
  return "danger";
});

const accountKeysStatusText = computed(() => {
  if (accountKeys.saving) return "Saving";
  if (accountKeys.loading) return "Loading";
  if (accountKeys.error) return "Error";
  return accountKeys.loaded ? "Loaded" : "Idle";
});

const accountKeysTone = computed<SectionTone>(() => {
  if (accountKeys.saving || accountKeys.loading) return "warning";
  if (accountKeys.error) return "danger";
  return accountKeys.loaded ? "success" : "neutral";
});

async function loadProwlarr(notify = false) {
  prowlarr.loading = true;
  prowlarr.error = "";
  try {
    const response = await api.get<ProwlarrSettingsResponse>("/settings/prowlarr");
    prowlarr.response = response;
    Object.assign(prowlarr.form, toProwlarrForm(response));
    markSynced(prowlarr);
    if (notify) {
      push("Prowlarr reloaded", "success");
    }
  } catch (error) {
    prowlarr.error = errorMessage(error);
    if (notify) {
      push(`Prowlarr: ${prowlarr.error}`, "error");
    }
  } finally {
    prowlarr.loading = false;
  }
}

async function saveProwlarr() {
  prowlarr.saving = true;
  prowlarr.error = "";
  try {
    await api.put<void>("/settings/prowlarr/base-url", {
      base_url: prowlarr.form.baseUrl.trim(),
    });
    await api.put<void>("/settings/prowlarr/api-key", {
      api_key: prowlarr.form.apiKey,
    });
    await api.put<void>("/settings/prowlarr/default-language", {
      language: prowlarr.form.defaultLanguage.trim(),
    });
    await api.put<void>("/settings/prowlarr/search-template", {
      template: prowlarr.form.searchTemplate,
    });
    markSynced(prowlarr);
    push("Prowlarr saved", "success");
  } catch (error) {
    prowlarr.error = errorMessage(error);
    push(`Prowlarr: ${prowlarr.error}`, "error");
  } finally {
    prowlarr.saving = false;
  }
}

async function loadDownload(notify = false) {
  download.loading = true;
  download.error = "";
  try {
    const response = await api.get<DownloadSettingsResponse>("/settings/download");
    download.response = response;
    Object.assign(download.form, toDownloadForm(response));
    markSynced(download);
    if (notify) {
      push("Download reloaded", "success");
    }
  } catch (error) {
    download.error = errorMessage(error);
    if (notify) {
      push(`Download: ${download.error}`, "error");
    }
  } finally {
    download.loading = false;
  }
}

async function saveDownload() {
  download.saving = true;
  download.error = "";
  try {
    await api.patch<void>("/settings/download", {
      auto_download: download.form.autoDownload,
      flac_range: parseRangeInput(download.form.flacFrom, download.form.flacTo, "FLAC range"),
      m4b_range: parseRangeInput(download.form.m4bFrom, download.form.m4bTo, "M4B range"),
      mp3_range: parseRangeInput(download.form.mp3From, download.form.mp3To, "MP3 range"),
      unknown_audio_range: parseRangeInput(
        download.form.unknownAudioFrom,
        download.form.unknownAudioTo,
        "Unknown audio range"
      ),
      unknown_range: parseRangeInput(
        download.form.unknownFrom,
        download.form.unknownTo,
        "Unknown range"
      ),
      min_seeders: parseIntegerInput(download.form.minSeeders, "Minimum seeders", { min: 0 }),
      name_ratio: parseIntegerInput(download.form.nameRatio, "Name ratio", { min: 0 }),
      title_ratio: parseIntegerInput(download.form.titleRatio, "Title ratio", { min: 0 }),
    });
    markSynced(download);
    push("Download saved", "success");
  } catch (error) {
    download.error = errorMessage(error);
    push(`Download: ${download.error}`, "error");
  } finally {
    download.saving = false;
  }
}

async function loadDownloadClient(notify = false) {
  downloadClient.loading = true;
  downloadClient.error = "";
  try {
    const response = await api.get<DownloadClientSettingsResponse>("/settings/download-client");
    downloadClient.response = response;
    Object.assign(downloadClient.form, toDownloadClientForm(response));
    markSynced(downloadClient);
    if (notify) {
      push("Download client reloaded", "success");
    }
  } catch (error) {
    downloadClient.error = errorMessage(error);
    if (notify) {
      push(`Download client: ${downloadClient.error}`, "error");
    }
  } finally {
    downloadClient.loading = false;
  }
}

async function saveDownloadClient() {
  downloadClient.saving = true;
  downloadClient.error = "";
  try {
    await api.patch<void>("/settings/download-client", {
      qbit_host: downloadClient.form.qbitHost.trim(),
      qbit_port: parseIntegerInput(downloadClient.form.qbitPort, "qBittorrent port", {
        min: 1,
        max: 65535,
      }),
      qbit_user: downloadClient.form.qbitUser.trim(),
      qbit_pass: downloadClient.form.qbitPass,
      qbit_category: downloadClient.form.qbitCategory.trim(),
      qbit_save_path: downloadClient.form.qbitSavePath.trim(),
      qbit_enabled: downloadClient.form.qbitEnabled,
      qbit_complete_action: String(downloadClient.form.qbitCompleteAction).trim(),
    });
    markSynced(downloadClient);
    push("Download client saved", "success");
  } catch (error) {
    downloadClient.error = errorMessage(error);
    push(`Download client: ${downloadClient.error}`, "error");
  } finally {
    downloadClient.saving = false;
  }
}

async function loadMediaManagement(notify = false) {
  mediaManagement.loading = true;
  mediaManagement.error = "";
  try {
    const response = await api.get<MediaManagementSettingsResponse>("/settings/media-management");
    mediaManagement.response = response;
    Object.assign(mediaManagement.form, toMediaManagementForm(response));
    markSynced(mediaManagement);
    if (notify) {
      push("Media management reloaded", "success");
    }
  } catch (error) {
    mediaManagement.error = errorMessage(error);
    if (notify) {
      push(`Media management: ${mediaManagement.error}`, "error");
    }
  } finally {
    mediaManagement.loading = false;
  }
}

async function saveMediaManagement() {
  mediaManagement.saving = true;
  mediaManagement.error = "";
  try {
    await api.patch<void>("/settings/media-management", {
      library_path: mediaManagement.form.libraryPath.trim(),
      folder_pattern: mediaManagement.form.folderPattern.trim(),
      file_pattern: mediaManagement.form.filePattern.trim(),
      use_series_folders: mediaManagement.form.useSeriesFolders,
      use_hardlinks: mediaManagement.form.useHardlinks,
      review_before_import: mediaManagement.form.reviewBeforeImport,
    });
    markSynced(mediaManagement);
    push("Media management saved", "success");
  } catch (error) {
    mediaManagement.error = errorMessage(error);
    push(`Media management: ${mediaManagement.error}`, "error");
  } finally {
    mediaManagement.saving = false;
  }
}

async function loadAudiobookshelf(notify = false) {
  audiobookshelf.loading = true;
  audiobookshelf.error = "";
  try {
    const response = await api.get<AudiobookshelfSettingsResponse>("/settings/audiobookshelf");
    audiobookshelf.response = response;
    Object.assign(audiobookshelf.form, toAudiobookshelfForm(response));
    markSynced(audiobookshelf);
    if (notify) {
      push("Audiobookshelf reloaded", "success");
    }
  } catch (error) {
    audiobookshelf.error = errorMessage(error);
    if (notify) {
      push(`Audiobookshelf: ${audiobookshelf.error}`, "error");
    }
  } finally {
    audiobookshelf.loading = false;
  }
}

async function saveAudiobookshelf() {
  audiobookshelf.saving = true;
  audiobookshelf.error = "";
  try {
    await api.put<void>(
      "/settings/audiobookshelf/base-url",
      toUrlSearchParams({
        base_url: audiobookshelf.form.baseUrl.trim(),
      })
    );
    await api.put<void>(
      "/settings/audiobookshelf/api-token",
      toUrlSearchParams({
        api_token: audiobookshelf.form.apiToken,
      })
    );
    await api.put<void>(
      "/settings/audiobookshelf/library",
      toUrlSearchParams({
        library_id: audiobookshelf.form.libraryId,
      })
    );
    await api.put<void>(
      "/settings/audiobookshelf/check-downloaded",
      toUrlSearchParams({
        check_downloaded: audiobookshelf.form.checkDownloaded,
      })
    );
    markSynced(audiobookshelf);
    push("Audiobookshelf saved", "success");
  } catch (error) {
    audiobookshelf.error = errorMessage(error);
    push(`Audiobookshelf: ${audiobookshelf.error}`, "error");
  } finally {
    audiobookshelf.saving = false;
  }
}

async function loadSecurity(notify = false) {
  security.loading = true;
  security.error = "";
  try {
    const response = await api.get<SecuritySettingsResponse>("/settings/security");
    security.response = response;
    Object.assign(security.form, toSecurityForm(response));
    markSynced(security);
    if (notify) {
      push("Security reloaded", "success");
    }
  } catch (error) {
    security.error = errorMessage(error);
    if (notify) {
      push(`Security: ${security.error}`, "error");
    }
  } finally {
    security.loading = false;
  }
}

async function saveSecurity() {
  security.saving = true;
  security.error = "";
  try {
    await api.patch<void>("/settings/security", {
      login_type: security.form.loginType,
      access_token_expiry: parseIntegerInput(
        security.form.accessTokenExpiry,
        "Access token expiry",
        { min: 1 }
      ),
      min_password_length: parseIntegerInput(
        security.form.minPasswordLength,
        "Minimum password length",
        { min: 1 }
      ),
    });
    markSynced(security);
    push("Security saved", "success");
  } catch (error) {
    security.error = errorMessage(error);
    push(`Security: ${security.error}`, "error");
  } finally {
    security.saving = false;
  }
}

async function loadAccountKeys(notify = false) {
  accountKeys.loading = true;
  accountKeys.error = "";
  try {
    accountKeys.items = await api.get<AccountApiKeySummary[]>("/settings/account/api-keys");
    accountKeys.loaded = true;
    if (notify) {
      push("API keys reloaded", "success");
    }
  } catch (error) {
    accountKeys.error = errorMessage(error);
    if (notify) {
      push(`API keys: ${accountKeys.error}`, "error");
    }
  } finally {
    accountKeys.loading = false;
  }
}

async function createAccountKey() {
  const name = accountKeys.newName.trim();
  if (!name) {
    push("API key name is required", "error");
    return;
  }
  accountKeys.saving = true;
  accountKeys.error = "";
  try {
    const created = await api.post<AccountCreateApiKeyResponse>("/settings/account/api-keys", {
      name,
    });
    accountKeys.newName = "";
    accountKeys.latestKey = created.key;
    accountKeys.latestName = created.name;
    await loadAccountKeys();
    push("API key created", "success");
  } catch (error) {
    accountKeys.error = errorMessage(error);
    push(`API keys: ${accountKeys.error}`, "error");
  } finally {
    accountKeys.saving = false;
  }
}

async function toggleAccountKey(id: string) {
  accountKeys.saving = true;
  accountKeys.error = "";
  try {
    await api.patch<void>(`/settings/account/api-keys/${id}/toggle`);
    await loadAccountKeys();
  } catch (error) {
    accountKeys.error = errorMessage(error);
    push(`API keys: ${accountKeys.error}`, "error");
  } finally {
    accountKeys.saving = false;
  }
}

async function deleteAccountKey(id: string) {
  accountKeys.saving = true;
  accountKeys.error = "";
  try {
    await api.delete<void>(`/settings/account/api-keys/${id}`);
    await loadAccountKeys();
    push("API key deleted", "success");
  } catch (error) {
    accountKeys.error = errorMessage(error);
    push(`API keys: ${accountKeys.error}`, "error");
  } finally {
    accountKeys.saving = false;
  }
}

async function copyLatestKey() {
  if (!accountKeys.latestKey) {
    return;
  }
  try {
    await navigator.clipboard.writeText(accountKeys.latestKey);
    push("API key copied", "success");
  } catch {
    push("Copy failed", "error");
  }
}

async function loadAll() {
  await Promise.all([
    loadProwlarr(),
    loadDownload(),
    loadDownloadClient(),
    loadMediaManagement(),
    loadAudiobookshelf(),
    loadSecurity(),
    loadAccountKeys(),
  ]);
}

onMounted(() => {
  void loadAll();
});
</script>

<template>
  <div class="page-stack">
    <PageCard title="Settings">
      <template #actions>
        <button class="ghost-button" type="button" @click="loadAll">
          Reload all
        </button>
      </template>
    </PageCard>

    <div class="settings-grid">
      <SettingsSectionCard
        title="Prowlarr"
        endpoint="/settings/prowlarr"
        :tone="prowlarrTone"
        :status-text="prowlarrStatusText"
        :error="prowlarr.error"
      >
        <template #actions>
          <button
            class="ghost-button"
            type="button"
            :disabled="prowlarr.loading || prowlarr.saving"
            @click="loadProwlarr(true)"
          >
            Reload
          </button>
          <button
            class="primary-button"
            type="submit"
            form="settings-prowlarr-form"
            :disabled="prowlarr.loading || prowlarr.saving || !isDirty(prowlarr)"
          >
            Save
          </button>
        </template>

        <form id="settings-prowlarr-form" class="settings-form" @submit.prevent="saveProwlarr">
          <div class="field-grid">
            <label class="field">
              <span>Base URL</span>
              <input
                v-model="prowlarr.form.baseUrl"
                class="text-input compact-input"
                type="text"
                autocomplete="off"
                :disabled="prowlarr.loading || prowlarr.saving"
              />
            </label>

            <label class="field">
              <span>API key</span>
              <input
                v-model="prowlarr.form.apiKey"
                class="text-input compact-input"
                type="password"
                autocomplete="off"
                :disabled="prowlarr.loading || prowlarr.saving"
              />
            </label>

            <label class="field">
              <span>Default language</span>
              <input
                v-model="prowlarr.form.defaultLanguage"
                class="text-input compact-input"
                type="text"
                autocomplete="off"
                :disabled="prowlarr.loading || prowlarr.saving"
              />
            </label>

            <label class="field field-wide">
              <span>Search template</span>
              <textarea
                v-model="prowlarr.form.searchTemplate"
                class="text-input compact-textarea"
                rows="2"
                :disabled="prowlarr.loading || prowlarr.saving"
              ></textarea>
            </label>
          </div>

          <div class="section-meta muted mono">
            <span>Categories {{ prowlarr.response?.selected_categories.length ?? 0 }}</span>
            <span>Indexers {{ prowlarr.response?.selected_indexers.length ?? 0 }}</span>
          </div>
        </form>
      </SettingsSectionCard>

      <SettingsSectionCard
        title="Download"
        endpoint="/settings/download"
        :tone="sectionTone(download)"
        :status-text="sectionStatus(download)"
        :error="download.error"
      >
        <template #actions>
          <button
            class="ghost-button"
            type="button"
            :disabled="download.loading || download.saving"
            @click="loadDownload(true)"
          >
            Reload
          </button>
          <button
            class="primary-button"
            type="submit"
            form="settings-download-form"
            :disabled="download.loading || download.saving || !isDirty(download)"
          >
            Save
          </button>
        </template>

        <form id="settings-download-form" class="settings-form" @submit.prevent="saveDownload">
          <label class="field checkbox-row field-wide">
            <input
              v-model="download.form.autoDownload"
              type="checkbox"
              :disabled="download.loading || download.saving"
            />
            <span>Auto download</span>
          </label>

          <div class="field field-wide">
            <span>Quality ranges</span>
            <div class="range-list">
              <div class="range-grid">
                <div class="range-label">FLAC</div>
                <input
                  v-model="download.form.flacFrom"
                  class="text-input compact-input"
                  type="number"
                  step="0.1"
                  inputmode="decimal"
                  :disabled="download.loading || download.saving"
                />
                <input
                  v-model="download.form.flacTo"
                  class="text-input compact-input"
                  type="number"
                  step="0.1"
                  inputmode="decimal"
                  :disabled="download.loading || download.saving"
                />
              </div>

              <div class="range-grid">
                <div class="range-label">M4B</div>
                <input
                  v-model="download.form.m4bFrom"
                  class="text-input compact-input"
                  type="number"
                  step="0.1"
                  inputmode="decimal"
                  :disabled="download.loading || download.saving"
                />
                <input
                  v-model="download.form.m4bTo"
                  class="text-input compact-input"
                  type="number"
                  step="0.1"
                  inputmode="decimal"
                  :disabled="download.loading || download.saving"
                />
              </div>

              <div class="range-grid">
                <div class="range-label">MP3</div>
                <input
                  v-model="download.form.mp3From"
                  class="text-input compact-input"
                  type="number"
                  step="0.1"
                  inputmode="decimal"
                  :disabled="download.loading || download.saving"
                />
                <input
                  v-model="download.form.mp3To"
                  class="text-input compact-input"
                  type="number"
                  step="0.1"
                  inputmode="decimal"
                  :disabled="download.loading || download.saving"
                />
              </div>

              <div class="range-grid">
                <div class="range-label">Unknown audio</div>
                <input
                  v-model="download.form.unknownAudioFrom"
                  class="text-input compact-input"
                  type="number"
                  step="0.1"
                  inputmode="decimal"
                  :disabled="download.loading || download.saving"
                />
                <input
                  v-model="download.form.unknownAudioTo"
                  class="text-input compact-input"
                  type="number"
                  step="0.1"
                  inputmode="decimal"
                  :disabled="download.loading || download.saving"
                />
              </div>

              <div class="range-grid">
                <div class="range-label">Unknown</div>
                <input
                  v-model="download.form.unknownFrom"
                  class="text-input compact-input"
                  type="number"
                  step="0.1"
                  inputmode="decimal"
                  :disabled="download.loading || download.saving"
                />
                <input
                  v-model="download.form.unknownTo"
                  class="text-input compact-input"
                  type="number"
                  step="0.1"
                  inputmode="decimal"
                  :disabled="download.loading || download.saving"
                />
              </div>
            </div>
          </div>

          <div class="field-grid">
            <label class="field">
              <span>Min seeders</span>
              <input
                v-model="download.form.minSeeders"
                class="text-input compact-input"
                type="number"
                step="1"
                inputmode="numeric"
                :disabled="download.loading || download.saving"
              />
            </label>

            <label class="field">
              <span>Name ratio</span>
              <input
                v-model="download.form.nameRatio"
                class="text-input compact-input"
                type="number"
                step="1"
                inputmode="numeric"
                :disabled="download.loading || download.saving"
              />
            </label>

            <label class="field">
              <span>Title ratio</span>
              <input
                v-model="download.form.titleRatio"
                class="text-input compact-input"
                type="number"
                step="1"
                inputmode="numeric"
                :disabled="download.loading || download.saving"
              />
            </label>
          </div>

          <div class="section-meta muted mono">
            <span>Indexer flags {{ download.response?.indexer_flags.length ?? 0 }}</span>
          </div>
        </form>
      </SettingsSectionCard>

      <SettingsSectionCard
        title="Download Client"
        endpoint="/settings/download-client"
        :tone="sectionTone(downloadClient)"
        :status-text="sectionStatus(downloadClient)"
        :error="downloadClient.error"
      >
        <template #actions>
          <button
            class="ghost-button"
            type="button"
            :disabled="downloadClient.loading || downloadClient.saving"
            @click="loadDownloadClient(true)"
          >
            Reload
          </button>
          <button
            class="primary-button"
            type="submit"
            form="settings-download-client-form"
            :disabled="downloadClient.loading || downloadClient.saving || !isDirty(downloadClient)"
          >
            Save
          </button>
        </template>

        <form
          id="settings-download-client-form"
          class="settings-form"
          @submit.prevent="saveDownloadClient"
        >
          <div class="field-grid">
            <label class="field">
              <span>Host</span>
              <input
                v-model="downloadClient.form.qbitHost"
                class="text-input compact-input"
                type="text"
                autocomplete="off"
                :disabled="downloadClient.loading || downloadClient.saving"
              />
            </label>

            <label class="field">
              <span>Port</span>
              <input
                v-model="downloadClient.form.qbitPort"
                class="text-input compact-input"
                type="number"
                step="1"
                inputmode="numeric"
                :disabled="downloadClient.loading || downloadClient.saving"
              />
            </label>

            <label class="field">
              <span>User</span>
              <input
                v-model="downloadClient.form.qbitUser"
                class="text-input compact-input"
                type="text"
                autocomplete="off"
                :disabled="downloadClient.loading || downloadClient.saving"
              />
            </label>

            <label class="field">
              <span>Password</span>
              <input
                v-model="downloadClient.form.qbitPass"
                class="text-input compact-input"
                type="password"
                autocomplete="off"
                :disabled="downloadClient.loading || downloadClient.saving"
              />
            </label>

            <label class="field">
              <span>Category</span>
              <input
                v-model="downloadClient.form.qbitCategory"
                class="text-input compact-input"
                type="text"
                autocomplete="off"
                :disabled="downloadClient.loading || downloadClient.saving"
              />
            </label>

            <label class="field field-wide">
              <span>Save path</span>
              <input
                v-model="downloadClient.form.qbitSavePath"
                class="text-input compact-input"
                type="text"
                autocomplete="off"
                :disabled="downloadClient.loading || downloadClient.saving"
              />
            </label>

            <label class="field">
              <span>Complete action</span>
              <select
                v-model="downloadClient.form.qbitCompleteAction"
                class="text-input compact-input"
                :disabled="downloadClient.loading || downloadClient.saving"
              >
                <option
                  v-for="option in downloadClientActionOptions"
                  :key="String(option.value)"
                  :value="option.value"
                >
                  {{ option.label }}
                </option>
              </select>
            </label>

            <label class="field checkbox-row">
              <input
                v-model="downloadClient.form.qbitEnabled"
                type="checkbox"
                :disabled="downloadClient.loading || downloadClient.saving"
              />
              <span>Enabled</span>
            </label>
          </div>
        </form>
      </SettingsSectionCard>

      <SettingsSectionCard
        title="Media Management"
        endpoint="/settings/media-management"
        :tone="sectionTone(mediaManagement)"
        :status-text="sectionStatus(mediaManagement)"
        :error="mediaManagement.error"
      >
        <template #actions>
          <button
            class="ghost-button"
            type="button"
            :disabled="mediaManagement.loading || mediaManagement.saving"
            @click="loadMediaManagement(true)"
          >
            Reload
          </button>
          <button
            class="primary-button"
            type="submit"
            form="settings-media-management-form"
            :disabled="mediaManagement.loading || mediaManagement.saving || !isDirty(mediaManagement)"
          >
            Save
          </button>
        </template>

        <form
          id="settings-media-management-form"
          class="settings-form"
          @submit.prevent="saveMediaManagement"
        >
          <div class="field-grid">
            <label class="field field-wide">
              <span>Library path</span>
              <input
                v-model="mediaManagement.form.libraryPath"
                class="text-input compact-input"
                type="text"
                autocomplete="off"
                :disabled="mediaManagement.loading || mediaManagement.saving"
              />
            </label>

            <label class="field">
              <span>Folder pattern</span>
              <input
                v-model="mediaManagement.form.folderPattern"
                class="text-input compact-input"
                type="text"
                autocomplete="off"
                :disabled="mediaManagement.loading || mediaManagement.saving"
              />
            </label>

            <label class="field">
              <span>File pattern</span>
              <input
                v-model="mediaManagement.form.filePattern"
                class="text-input compact-input"
                type="text"
                autocomplete="off"
                :disabled="mediaManagement.loading || mediaManagement.saving"
              />
            </label>

            <label class="field checkbox-row">
              <input
                v-model="mediaManagement.form.useSeriesFolders"
                type="checkbox"
                :disabled="mediaManagement.loading || mediaManagement.saving"
              />
              <span>Series folders</span>
            </label>

            <label class="field checkbox-row">
              <input
                v-model="mediaManagement.form.useHardlinks"
                type="checkbox"
                :disabled="mediaManagement.loading || mediaManagement.saving"
              />
              <span>Hardlinks</span>
            </label>

            <label class="field checkbox-row">
              <input
                v-model="mediaManagement.form.reviewBeforeImport"
                type="checkbox"
                :disabled="mediaManagement.loading || mediaManagement.saving"
              />
              <span>Review before import</span>
            </label>
          </div>
        </form>
      </SettingsSectionCard>

      <SettingsSectionCard
        title="Audiobookshelf"
        endpoint="/settings/audiobookshelf"
        :tone="absTone"
        :status-text="absStatusText"
        :error="audiobookshelf.error"
      >
        <template #actions>
          <button
            class="ghost-button"
            type="button"
            :disabled="audiobookshelf.loading || audiobookshelf.saving"
            @click="loadAudiobookshelf(true)"
          >
            Reload
          </button>
          <button
            class="primary-button"
            type="submit"
            form="settings-audiobookshelf-form"
            :disabled="audiobookshelf.loading || audiobookshelf.saving || !isDirty(audiobookshelf)"
          >
            Save
          </button>
        </template>

        <form
          id="settings-audiobookshelf-form"
          class="settings-form"
          @submit.prevent="saveAudiobookshelf"
        >
          <div class="field-grid">
            <label class="field">
              <span>Base URL</span>
              <input
                v-model="audiobookshelf.form.baseUrl"
                class="text-input compact-input"
                type="text"
                autocomplete="off"
                :disabled="audiobookshelf.loading || audiobookshelf.saving"
              />
            </label>

            <label class="field">
              <span>API token</span>
              <input
                v-model="audiobookshelf.form.apiToken"
                class="text-input compact-input"
                type="password"
                autocomplete="off"
                :disabled="audiobookshelf.loading || audiobookshelf.saving"
              />
            </label>

            <label class="field field-wide">
              <span>Library</span>
              <select
                v-model="audiobookshelf.form.libraryId"
                class="text-input compact-input"
                :disabled="audiobookshelf.loading || audiobookshelf.saving"
              >
                <option value="">Select a library</option>
                <option v-for="library in absLibraryOptions" :key="library.id" :value="library.id">
                  {{ library.name }}
                </option>
              </select>
            </label>

            <label class="field checkbox-row">
              <input
                v-model="audiobookshelf.form.checkDownloaded"
                type="checkbox"
                :disabled="audiobookshelf.loading || audiobookshelf.saving"
              />
              <span>Check downloaded</span>
            </label>
          </div>

          <div class="section-meta muted mono">
            <span>Libraries {{ audiobookshelf.response?.abs_libraries.length ?? 0 }}</span>
          </div>
        </form>
      </SettingsSectionCard>

      <SettingsSectionCard
        title="Security"
        endpoint="/settings/security"
        :tone="sectionTone(security)"
        :status-text="sectionStatus(security)"
        :error="security.error"
      >
        <template #actions>
          <button
            class="ghost-button"
            type="button"
            :disabled="security.loading || security.saving"
            @click="loadSecurity(true)"
          >
            Reload
          </button>
          <button
            class="primary-button"
            type="submit"
            form="settings-security-form"
            :disabled="security.loading || security.saving || !isDirty(security)"
          >
            Save
          </button>
        </template>

        <form id="settings-security-form" class="settings-form" @submit.prevent="saveSecurity">
          <div class="field-grid">
            <label class="field">
              <span>Login type</span>
              <select
                v-model="security.form.loginType"
                class="text-input compact-input"
                :disabled="security.loading || security.saving"
              >
                <option
                  v-for="option in securityLoginOptions"
                  :key="String(option.value)"
                  :value="option.value"
                >
                  {{ option.label }}
                </option>
              </select>
            </label>

            <label class="field">
              <span>Token expiry</span>
              <input
                v-model="security.form.accessTokenExpiry"
                class="text-input compact-input"
                type="number"
                step="1"
                inputmode="numeric"
                :disabled="security.loading || security.saving"
              />
            </label>

            <label class="field">
              <span>Min password length</span>
              <input
                v-model="security.form.minPasswordLength"
                class="text-input compact-input"
                type="number"
                step="1"
                inputmode="numeric"
                :disabled="security.loading || security.saving"
              />
            </label>
          </div>
        </form>
      </SettingsSectionCard>

      <SettingsSectionCard
        title="API Keys"
        endpoint="/settings/account/api-keys"
        :tone="accountKeysTone"
        :status-text="accountKeysStatusText"
        :error="accountKeys.error"
      >
        <template #actions>
          <button
            class="ghost-button"
            type="button"
            :disabled="accountKeys.loading || accountKeys.saving"
            @click="loadAccountKeys(true)"
          >
            Reload
          </button>
        </template>

        <form class="settings-form" @submit.prevent="createAccountKey">
          <div class="field-grid">
            <label class="field field-wide">
              <span>New key name</span>
              <input
                v-model="accountKeys.newName"
                class="text-input compact-input"
                type="text"
                autocomplete="off"
                :disabled="accountKeys.loading || accountKeys.saving"
                placeholder="mobile-app"
              />
            </label>
          </div>
          <div class="section-actions">
            <button
              class="primary-button"
              type="submit"
              :disabled="accountKeys.loading || accountKeys.saving || !accountKeys.newName.trim()"
            >
              Generate
            </button>
          </div>
        </form>

        <div v-if="accountKeys.latestKey" class="key-reveal">
          <div class="field">
            <span>{{ accountKeys.latestName }} (copy now, shown once)</span>
            <input class="text-input compact-input mono" :value="accountKeys.latestKey" readonly />
          </div>
          <div class="section-actions">
            <button class="ghost-button" type="button" @click="copyLatestKey">Copy</button>
          </div>
        </div>

        <div class="table-list" v-if="accountKeys.items.length">
          <div class="table-row key-table-row table-head">
            <span>Name</span>
            <span>Status</span>
            <span>Actions</span>
          </div>
          <div v-for="item in accountKeys.items" :key="item.id" class="table-row key-table-row">
            <span class="mono">{{ item.name }}</span>
            <span>
              <StatusPill :tone="item.enabled ? 'success' : 'neutral'">
                {{ item.enabled ? "Enabled" : "Disabled" }}
              </StatusPill>
            </span>
            <span class="row-actions">
              <button
                class="ghost-button"
                type="button"
                :disabled="accountKeys.loading || accountKeys.saving"
                @click="toggleAccountKey(item.id)"
              >
                {{ item.enabled ? "Disable" : "Enable" }}
              </button>
              <button
                class="ghost-button"
                type="button"
                :disabled="accountKeys.loading || accountKeys.saving"
                @click="deleteAccountKey(item.id)"
              >
                Delete
              </button>
            </span>
          </div>
        </div>
      </SettingsSectionCard>
    </div>
  </div>
</template>

<style scoped>
.settings-form {
  display: grid;
  gap: 0.5rem;
}

.field-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 0.45rem;
}

.field {
  display: grid;
  gap: 0.25rem;
  min-width: 0;
}

.field > span {
  color: var(--text-muted);
  font-size: 0.67rem;
  text-transform: uppercase;
  letter-spacing: 0.08em;
}

.field-wide {
  grid-column: 1 / -1;
}

.compact-input,
.compact-textarea {
  width: 100%;
  min-width: 0;
  border: 1px solid var(--border-strong);
  border-radius: 0.35rem;
  background: #0f151c;
  padding: 0.36rem 0.5rem;
  font-size: 0.83rem;
}

.compact-textarea {
  resize: vertical;
  min-height: 4rem;
}

.compact-input:disabled,
.compact-textarea:disabled {
  opacity: 0.7;
  cursor: not-allowed;
}

.checkbox-row {
  display: flex;
  align-items: center;
  gap: 0.45rem;
  min-height: 32px;
}

.checkbox-row > span {
  text-transform: none;
  letter-spacing: 0;
  font-size: 0.83rem;
  color: var(--text);
}

.checkbox-row input[type="checkbox"] {
  width: 14px;
  height: 14px;
  margin: 0;
}

.range-list {
  display: grid;
  gap: 0.35rem;
}

.range-grid {
  display: grid;
  grid-template-columns: 88px minmax(0, 1fr) minmax(0, 1fr);
  gap: 0.35rem;
  align-items: center;
}

.range-label {
  color: var(--text-muted);
  font-size: 0.75rem;
  line-height: 1;
}

.section-meta {
  display: flex;
  gap: 0.6rem;
  flex-wrap: wrap;
  font-size: 0.72rem;
}

.section-actions {
  display: flex;
  justify-content: flex-end;
  gap: 0.35rem;
}

.key-reveal {
  border: 1px solid var(--border);
  border-radius: 0.35rem;
  padding: 0.45rem;
  display: grid;
  gap: 0.45rem;
}

.key-table-row {
  grid-template-columns: 1.6fr 0.8fr 1.2fr;
}

.row-actions {
  display: flex;
  gap: 0.35rem;
}

@media (max-width: 720px) {
  .field-grid,
  .range-grid {
    grid-template-columns: 1fr;
  }

  .field-wide {
    grid-column: auto;
  }

  .section-actions {
    justify-content: stretch;
  }

  .row-actions {
    flex-direction: column;
  }
}
</style>
