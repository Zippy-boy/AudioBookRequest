<script setup lang="ts">
import { computed, onMounted, reactive, ref } from "vue";
import { useRouter } from "vue-router";
import StatusPill from "../components/StatusPill.vue";
import { api, completeSetup, getAuthStatus, getSetupStatus, initializeAuth, login } from "../lib/api";
import {
  completeActionOptions,
  loginTypeOptions,
  parseIntegerInput,
  toAudiobookshelfForm,
  toDownloadClientForm,
  toMediaManagementForm,
  toProwlarrForm,
  toUrlSearchParams,
  type AudiobookshelfSettingsResponse,
  type DownloadClientSettingsResponse,
  type LoginTypeChoice,
  type MediaManagementSettingsResponse,
  type ProwlarrSettingsResponse,
  type SectionTone,
} from "../lib/settings";
import { useToasts } from "../lib/toast";

type StepId = "auth" | "prowlarr" | "download-client" | "media-management" | "audiobookshelf" | "review";

const router = useRouter();
const { push } = useToasts();

function snapshot(value: unknown) {
  return JSON.stringify(value);
}

function createState(form: any) {
  return reactive({ loading: false, saving: false, loaded: false, error: "", baseline: "", form });
}

function markSynced(section: any) {
  section.loaded = true;
  section.baseline = snapshot(section.form);
}

function isDirty(section: any) {
  return section.loaded && snapshot(section.form) !== section.baseline;
}

function sectionStatus(section: any) {
  if (section.saving) return "Saving";
  if (section.loading) return "Loading";
  if (section.error) return "Error";
  if (isDirty(section)) return "Unsaved";
  return section.loaded ? "Loaded" : "Pending";
}

function sectionTone(section: any): SectionTone {
  if (section.saving || section.loading) return "warning";
  if (section.error) return "danger";
  if (isDirty(section)) return "info";
  return section.loaded ? "success" : "neutral";
}

function clearValidation(errors: Record<string, string>) {
  Object.keys(errors).forEach((key) => (errors[key] = ""));
}

function errorText(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}

function textOrDash(value: string) {
  return value.trim() ? value : "Not set";
}

function hiddenOrDash(value: string) {
  return value.trim() ? "[hidden]" : "Not set";
}

const authStatus = reactive({
  loading: true,
  error: "",
  initialized: false,
  loginType: "" as string | null,
  forceLoginType: "" as string | null,
});

const auth = reactive({
  saving: false,
  error: "",
  form: {
    loginType: (loginTypeOptions[1]?.value ?? loginTypeOptions[0]?.value ?? "forms") as LoginTypeChoice,
    username: "",
    password: "",
    confirmPassword: "",
  },
});

const authErrors = reactive({ loginType: "", username: "", password: "", confirmPassword: "" });
const prowlarr = createState(toProwlarrForm());
const prowlarrErrors = reactive({ baseUrl: "", apiKey: "", defaultLanguage: "", searchTemplate: "" });
const downloadClient = createState(toDownloadClientForm());
const downloadClientErrors = reactive({ qbitHost: "", qbitPort: "", qbitCategory: "", qbitSavePath: "", qbitCompleteAction: "" });
const mediaManagement = createState(toMediaManagementForm());
const mediaManagementErrors = reactive({ libraryPath: "", folderPattern: "", filePattern: "" });
const audiobookshelf = reactive({
  loading: false,
  saving: false,
  loaded: false,
  error: "",
  baseline: "",
  response: null as AudiobookshelfSettingsResponse | null,
  form: toAudiobookshelfForm(),
});
const audiobookshelfErrors = reactive({ baseUrl: "", apiToken: "", libraryId: "" });

const currentStepIndex = ref(0);
const authVisible = computed(() => !authStatus.initialized);

const absLibraryOptions = computed(() => {
  const libraries = audiobookshelf.response?.abs_libraries ?? [];
  const current = audiobookshelf.form.libraryId;
  if (current && !libraries.some((library) => library.id === current)) {
    return [{ id: current, name: `${current} (current)` }, ...libraries];
  }
  return libraries;
});

const downloadClientActionOptions = computed(() => {
  const current = String(downloadClient.form.qbitCompleteAction ?? "");
  const base = completeActionOptions.map((value) => ({ value, label: value }));
  if (completeActionOptions.includes(current as (typeof completeActionOptions)[number])) {
    return base;
  }
  return current ? [{ value: current, label: `${current} (current)` }, ...base] : base;
});

const steps = computed(() => {
  const list: Array<{ id: StepId; title: string; description: string; statusText: string; tone: SectionTone; complete: boolean }> = [];
  if (authVisible.value) {
    list.push({
      id: "auth",
      title: "Auth",
      description: "Initialize the first administrator account",
      statusText: authStatus.loading ? "Loading" : authStatus.error ? "Error" : authStatus.initialized ? "Initialized" : "Pending",
      tone: authStatus.loading ? "warning" : authStatus.error ? "danger" : authStatus.initialized ? "success" : "info",
      complete: authStatus.initialized,
    });
  }
  list.push(
    { id: "prowlarr", title: "Prowlarr", description: "Indexer base URL, API key, and template", statusText: sectionStatus(prowlarr), tone: sectionTone(prowlarr), complete: prowlarr.loaded && !isDirty(prowlarr) && !prowlarr.error },
    { id: "download-client", title: "Download Client", description: "qBittorrent connection and save path", statusText: sectionStatus(downloadClient), tone: sectionTone(downloadClient), complete: downloadClient.loaded && !isDirty(downloadClient) && !downloadClient.error },
    { id: "media-management", title: "Media", description: "Library path and import behavior", statusText: sectionStatus(mediaManagement), tone: sectionTone(mediaManagement), complete: mediaManagement.loaded && !isDirty(mediaManagement) && !mediaManagement.error },
    { id: "audiobookshelf", title: "Audiobookshelf", description: "Base URL, token, and library selection", statusText: sectionStatus(audiobookshelf), tone: sectionTone(audiobookshelf), complete: audiobookshelf.loaded && !isDirty(audiobookshelf) && !audiobookshelf.error },
    { id: "review", title: "Review", description: "Confirm the final configuration", statusText: canFinish.value ? "Ready" : "Review", tone: canFinish.value ? "success" : "warning", complete: canFinish.value }
  );
  return list;
});

const currentStep = computed(() => steps.value[Math.min(currentStepIndex.value, Math.max(steps.value.length - 1, 0))]);
const currentStepNumber = computed(() => Math.min(currentStepIndex.value, Math.max(steps.value.length - 1, 0)) + 1);

const canFinish = computed(() => {
  return authStatus.initialized &&
    !prowlarr.loading && !prowlarr.saving && !prowlarr.error && !isDirty(prowlarr) &&
    !downloadClient.loading && !downloadClient.saving && !downloadClient.error && !isDirty(downloadClient) &&
    !mediaManagement.loading && !mediaManagement.saving && !mediaManagement.error && !isDirty(mediaManagement) &&
    !audiobookshelf.loading && !audiobookshelf.saving && !audiobookshelf.error && !isDirty(audiobookshelf);
});

const currentStepBusy = computed(() => {
  switch (currentStep.value?.id) {
    case "auth": return authStatus.loading || auth.saving;
    case "prowlarr": return prowlarr.loading || prowlarr.saving;
    case "download-client": return downloadClient.loading || downloadClient.saving;
    case "media-management": return mediaManagement.loading || mediaManagement.saving;
    case "audiobookshelf": return audiobookshelf.loading || audiobookshelf.saving;
    default: return false;
  }
});

const primaryLabel = computed(() => currentStep.value?.id === "review" ? "Finish setup" : currentStep.value?.id === "auth" ? "Initialize & next" : "Save & next");
const primaryDisabled = computed(() => currentStep.value?.id === "review" ? !canFinish.value : currentStepBusy.value);

const reviewItems = computed(() => [
  {
    id: "auth",
    title: "Authentication",
    statusText: authStatus.initialized ? "Initialized" : "Pending",
    tone: authStatus.initialized ? "success" : "warning",
    rows: authStatus.initialized
      ? [{ label: "Login type", value: String(authStatus.loginType ?? auth.form.loginType) }, { label: "Admin user", value: textOrDash(auth.form.username) }]
      : [{ label: "Login type", value: String(auth.form.loginType) }, { label: "Admin user", value: textOrDash(auth.form.username) }],
    editIndex: authVisible.value ? steps.value.findIndex((step) => step.id === "auth") : null,
  },
  { id: "prowlarr", title: "Prowlarr", statusText: sectionStatus(prowlarr), tone: sectionTone(prowlarr), rows: [{ label: "Base URL", value: textOrDash(prowlarr.form.baseUrl) }, { label: "API key", value: hiddenOrDash(prowlarr.form.apiKey) }, { label: "Language", value: textOrDash(prowlarr.form.defaultLanguage) }, { label: "Search template", value: textOrDash(prowlarr.form.searchTemplate) }], editIndex: steps.value.findIndex((step) => step.id === "prowlarr") },
  { id: "download-client", title: "Download Client", statusText: sectionStatus(downloadClient), tone: sectionTone(downloadClient), rows: [{ label: "Host", value: textOrDash(downloadClient.form.qbitHost) }, { label: "Port", value: textOrDash(downloadClient.form.qbitPort) }, { label: "Category", value: textOrDash(downloadClient.form.qbitCategory) }, { label: "Save path", value: textOrDash(downloadClient.form.qbitSavePath) }, { label: "Complete action", value: String(downloadClient.form.qbitCompleteAction) }, { label: "Enabled", value: downloadClient.form.qbitEnabled ? "Yes" : "No" }], editIndex: steps.value.findIndex((step) => step.id === "download-client") },
  { id: "media-management", title: "Media Management", statusText: sectionStatus(mediaManagement), tone: sectionTone(mediaManagement), rows: [{ label: "Library path", value: textOrDash(mediaManagement.form.libraryPath) }, { label: "Folder pattern", value: textOrDash(mediaManagement.form.folderPattern) }, { label: "File pattern", value: textOrDash(mediaManagement.form.filePattern) }, { label: "Series folders", value: mediaManagement.form.useSeriesFolders ? "Yes" : "No" }, { label: "Hardlinks", value: mediaManagement.form.useHardlinks ? "Yes" : "No" }, { label: "Review before import", value: mediaManagement.form.reviewBeforeImport ? "Yes" : "No" }], editIndex: steps.value.findIndex((step) => step.id === "media-management") },
  { id: "audiobookshelf", title: "Audiobookshelf", statusText: sectionStatus(audiobookshelf), tone: sectionTone(audiobookshelf), rows: [{ label: "Base URL", value: textOrDash(audiobookshelf.form.baseUrl) }, { label: "API token", value: hiddenOrDash(audiobookshelf.form.apiToken) }, { label: "Library", value: absLibraryOptions.value.find((library) => library.id === audiobookshelf.form.libraryId)?.name ?? textOrDash(audiobookshelf.form.libraryId) }, { label: "Check downloaded", value: audiobookshelf.form.checkDownloaded ? "Yes" : "No" }], editIndex: steps.value.findIndex((step) => step.id === "audiobookshelf") },
]);

function canJumpTo(index: number) {
  return index <= currentStepIndex.value;
}

function jumpToStep(index: number) {
  if (canJumpTo(index)) currentStepIndex.value = index;
}

function nextStep() {
  currentStepIndex.value = Math.min(currentStepIndex.value + 1, steps.value.length - 1);
}

function clearAuthErrors() { clearValidation(authErrors); }
function clearProwlarrErrors() { clearValidation(prowlarrErrors); }
function clearDownloadClientErrors() { clearValidation(downloadClientErrors); }
function clearMediaManagementErrors() { clearValidation(mediaManagementErrors); }
function clearAudiobookshelfErrors() { clearValidation(audiobookshelfErrors); }

function validateAuthStep() {
  clearAuthErrors();
  let valid = true;
  if (!auth.form.loginType) { authErrors.loginType = "Choose a login type."; valid = false; }
  if (!auth.form.username.trim()) { authErrors.username = "Username is required."; valid = false; }
  if (!auth.form.password) { authErrors.password = "Password is required."; valid = false; }
  if (!auth.form.confirmPassword) { authErrors.confirmPassword = "Confirm the password."; valid = false; }
  if (auth.form.password && auth.form.confirmPassword && auth.form.password !== auth.form.confirmPassword) { authErrors.confirmPassword = "Passwords must match."; valid = false; }
  return valid;
}

function validateProwlarrStep() {
  clearProwlarrErrors();
  let valid = true;
  if (!prowlarr.form.baseUrl.trim()) { prowlarrErrors.baseUrl = "Base URL is required."; valid = false; }
  if (!prowlarr.form.apiKey.trim()) { prowlarrErrors.apiKey = "API key is required."; valid = false; }
  return valid;
}

function validateDownloadClientStep() {
  clearDownloadClientErrors();
  let valid = true;
  if (!downloadClient.form.qbitHost.trim()) { downloadClientErrors.qbitHost = "Host is required."; valid = false; }
  try { parseIntegerInput(downloadClient.form.qbitPort, "qBittorrent port", { min: 1, max: 65535 }); } catch (error) { downloadClientErrors.qbitPort = errorText(error); valid = false; }
  if (!downloadClient.form.qbitSavePath.trim()) { downloadClientErrors.qbitSavePath = "Save path is required."; valid = false; }
  if (!String(downloadClient.form.qbitCompleteAction).trim()) { downloadClientErrors.qbitCompleteAction = "Choose a complete action."; valid = false; }
  return valid;
}

function validateMediaManagementStep() {
  clearMediaManagementErrors();
  let valid = true;
  if (!mediaManagement.form.libraryPath.trim()) { mediaManagementErrors.libraryPath = "Library path is required."; valid = false; }
  if (!mediaManagement.form.folderPattern.trim()) { mediaManagementErrors.folderPattern = "Folder pattern is required."; valid = false; }
  if (!mediaManagement.form.filePattern.trim()) { mediaManagementErrors.filePattern = "File pattern is required."; valid = false; }
  return valid;
}

function validateAudiobookshelfStep() {
  clearAudiobookshelfErrors();
  let valid = true;
  if (!audiobookshelf.form.baseUrl.trim()) { audiobookshelfErrors.baseUrl = "Base URL is required."; valid = false; }
  if (!audiobookshelf.form.apiToken.trim()) { audiobookshelfErrors.apiToken = "API token is required."; valid = false; }
  if (!audiobookshelf.form.libraryId.trim()) { audiobookshelfErrors.libraryId = "Choose a library."; valid = false; }
  return valid;
}

async function loadAuthStatus() {
  authStatus.loading = true;
  authStatus.error = "";
  try {
    const status = await getAuthStatus();
    authStatus.initialized = status.initialized;
    authStatus.loginType = status.login_type;
    authStatus.forceLoginType = status.force_login_type;
    if (!authStatus.initialized) {
      auth.form.loginType = (status.force_login_type ?? status.login_type ?? auth.form.loginType) as LoginTypeChoice;
    }
  } catch (error) {
    authStatus.error = errorText(error);
  } finally {
    authStatus.loading = false;
  }
}

async function loadProwlarr() {
  prowlarr.loading = true;
  prowlarr.error = "";
  try {
    const response = await api.get<ProwlarrSettingsResponse>("/settings/prowlarr");
    Object.assign(prowlarr.form, toProwlarrForm(response));
    markSynced(prowlarr);
  } catch (error) {
    prowlarr.error = errorText(error);
  } finally {
    prowlarr.loading = false;
  }
}

async function loadDownloadClient() {
  downloadClient.loading = true;
  downloadClient.error = "";
  try {
    const response = await api.get<DownloadClientSettingsResponse>("/settings/download-client");
    Object.assign(downloadClient.form, toDownloadClientForm(response));
    markSynced(downloadClient);
  } catch (error) {
    downloadClient.error = errorText(error);
  } finally {
    downloadClient.loading = false;
  }
}

async function loadMediaManagement() {
  mediaManagement.loading = true;
  mediaManagement.error = "";
  try {
    const response = await api.get<MediaManagementSettingsResponse>("/settings/media-management");
    Object.assign(mediaManagement.form, toMediaManagementForm(response));
    markSynced(mediaManagement);
  } catch (error) {
    mediaManagement.error = errorText(error);
  } finally {
    mediaManagement.loading = false;
  }
}

async function loadAudiobookshelf() {
  audiobookshelf.loading = true;
  audiobookshelf.error = "";
  try {
    const response = await api.get<AudiobookshelfSettingsResponse>("/settings/audiobookshelf");
    Object.assign(audiobookshelf.form, toAudiobookshelfForm(response));
    audiobookshelf.response = response;
    markSynced(audiobookshelf);
  } catch (error) {
    audiobookshelf.error = errorText(error);
  } finally {
    audiobookshelf.loading = false;
  }
}

async function saveAuthStep() {
  if (!validateAuthStep()) return;
  auth.saving = true;
  auth.error = "";
  try {
    await initializeAuth({
      login_type: String(auth.form.loginType),
      username: auth.form.username.trim(),
      password: auth.form.password,
      confirm_password: auth.form.confirmPassword,
    });
    await login(auth.form.username.trim(), auth.form.password);
    authStatus.initialized = true;
    authStatus.loginType = String(auth.form.loginType);
    push("Authentication initialized", "success");
    currentStepIndex.value = 0;
  } catch (error) {
    const message = errorText(error);
    if (message.includes("Already initialized")) {
      authStatus.initialized = true;
      push("Authentication was already initialized", "info");
      currentStepIndex.value = 0;
      return;
    }
    auth.error = message;
    push(`Auth: ${message}`, "error");
  } finally {
    auth.saving = false;
  }
}
async function saveProwlarrStep() {
  if (!validateProwlarrStep()) return;
  prowlarr.saving = true;
  prowlarr.error = "";
  try {
    await api.put<void>("/settings/prowlarr/base-url", { base_url: prowlarr.form.baseUrl.trim() });
    await api.put<void>("/settings/prowlarr/api-key", { api_key: prowlarr.form.apiKey.trim() });
    await api.put<void>("/settings/prowlarr/default-language", { language: prowlarr.form.defaultLanguage.trim() });
    await api.put<void>("/settings/prowlarr/search-template", { template: prowlarr.form.searchTemplate });
    markSynced(prowlarr);
    push("Prowlarr saved", "success");
    nextStep();
  } catch (error) {
    prowlarr.error = errorText(error);
    push(`Prowlarr: ${prowlarr.error}`, "error");
  } finally {
    prowlarr.saving = false;
  }
}

async function saveDownloadClientStep() {
  if (!validateDownloadClientStep()) return;
  downloadClient.saving = true;
  downloadClient.error = "";
  try {
    await api.patch<void>("/settings/download-client", {
      qbit_host: downloadClient.form.qbitHost.trim(),
      qbit_port: parseIntegerInput(downloadClient.form.qbitPort, "qBittorrent port", { min: 1, max: 65535 }),
      qbit_user: downloadClient.form.qbitUser.trim(),
      qbit_pass: downloadClient.form.qbitPass,
      qbit_category: downloadClient.form.qbitCategory.trim(),
      qbit_save_path: downloadClient.form.qbitSavePath.trim(),
      qbit_enabled: downloadClient.form.qbitEnabled,
      qbit_complete_action: String(downloadClient.form.qbitCompleteAction).trim(),
    });
    markSynced(downloadClient);
    push("Download client saved", "success");
    nextStep();
  } catch (error) {
    downloadClient.error = errorText(error);
    push(`Download client: ${downloadClient.error}`, "error");
  } finally {
    downloadClient.saving = false;
  }
}

async function saveMediaManagementStep() {
  if (!validateMediaManagementStep()) return;
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
    nextStep();
  } catch (error) {
    mediaManagement.error = errorText(error);
    push(`Media management: ${mediaManagement.error}`, "error");
  } finally {
    mediaManagement.saving = false;
  }
}

async function saveAudiobookshelfStep() {
  if (!validateAudiobookshelfStep()) return;
  audiobookshelf.saving = true;
  audiobookshelf.error = "";
  try {
    await api.put<void>("/settings/audiobookshelf/base-url", toUrlSearchParams({ base_url: audiobookshelf.form.baseUrl.trim() }));
    await api.put<void>("/settings/audiobookshelf/api-token", toUrlSearchParams({ api_token: audiobookshelf.form.apiToken }));
    await api.put<void>("/settings/audiobookshelf/library", toUrlSearchParams({ library_id: audiobookshelf.form.libraryId }));
    await api.put<void>("/settings/audiobookshelf/check-downloaded", toUrlSearchParams({ check_downloaded: audiobookshelf.form.checkDownloaded }));
    markSynced(audiobookshelf);
    push("Audiobookshelf saved", "success");
    nextStep();
  } catch (error) {
    audiobookshelf.error = errorText(error);
    push(`Audiobookshelf: ${audiobookshelf.error}`, "error");
  } finally {
    audiobookshelf.saving = false;
  }
}

async function finishSetup() {
  if (!canFinish.value) {
    push("Resolve the remaining setup changes before finishing.", "error");
    return;
  }
  try {
    await completeSetup();
    await getSetupStatus(true);
    push("Setup complete", "success");
    await router.replace("/settings");
  } catch (error) {
    const message = errorText(error);
    push(`Setup complete: ${message}`, "error");
  }
}

async function handlePrimaryAction() {
  switch (currentStep.value?.id) {
    case "auth":
      await saveAuthStep();
      break;
    case "prowlarr":
      await saveProwlarrStep();
      break;
    case "download-client":
      await saveDownloadClientStep();
      break;
    case "media-management":
      await saveMediaManagementStep();
      break;
    case "audiobookshelf":
      await saveAudiobookshelfStep();
      break;
    case "review":
      await finishSetup();
      break;
  }
}

function handleBack() {
  if (currentStepIndex.value > 0) currentStepIndex.value -= 1;
}

async function loadAll() {
  await Promise.all([loadAuthStatus(), loadProwlarr(), loadDownloadClient(), loadMediaManagement(), loadAudiobookshelf()]);
}

onMounted(() => {
  void loadAll();
});
</script>

<template>
  <main class="setup-page">
    <section class="setup-shell">
      <header class="setup-hero">
        <div class="login-brand">
          <div class="brand-mark">N</div>
          <div>
            <div class="brand-title">Narrarr</div>
            <div class="brand-subtitle">Setup wizard</div>
          </div>
        </div>
        <div class="setup-hero-copy">
          <StatusPill :tone="authStatus.initialized ? 'success' : 'info'">
            {{ authStatus.initialized ? 'Re-run setup' : 'First-time setup' }}
          </StatusPill>
          <p>Initialize authentication, configure integrations, then review everything before jumping into the app.</p>
        </div>
      </header>

      <div v-if="authStatus.initialized" class="setup-banner">
        Authentication is already initialized. The auth step is skipped automatically.
      </div>
      <div v-if="authStatus.error" class="inline-error">Auth status: {{ authStatus.error }}</div>

      <section class="wizard-card">
        <nav class="wizard-stepper" aria-label="Setup steps">
          <button
            v-for="(step, index) in steps"
            :key="step.id"
            type="button"
            class="stepper-step"
            :class="{ active: index === currentStepIndex, complete: index < currentStepIndex }"
            :disabled="index > currentStepIndex"
            @click="jumpToStep(index)"
          >
            <span class="stepper-index">{{ index + 1 }}</span>
            <span class="stepper-copy">
              <span class="stepper-title">{{ step.title }}</span>
              <span class="stepper-desc">{{ step.description }}</span>
            </span>
            <StatusPill :tone="step.tone">{{ step.statusText }}</StatusPill>
          </button>
        </nav>

        <div class="wizard-body">
          <div class="wizard-head">
            <div>
              <div class="eyebrow">Step {{ currentStepNumber }} of {{ steps.length }}</div>
              <h1>{{ currentStep?.title }}</h1>
              <p>{{ currentStep?.description }}</p>
            </div>
            <StatusPill :tone="currentStep?.tone ?? 'neutral'">{{ currentStep?.statusText }}</StatusPill>
          </div>

          <div v-if="currentStep?.id === 'auth'" class="step-panel">
            <p class="step-intro">Create the first administrator account, and choose the login mode the server should use.</p>
            <div v-if="auth.error" class="inline-error">{{ auth.error }}</div>
            <form class="setup-form" @submit.prevent="handlePrimaryAction">
              <div class="field-grid">
                <label class="field">
                  <span>Login type</span>
                  <select v-model="auth.form.loginType" class="text-input compact-input" :disabled="authStatus.loading || auth.saving || Boolean(authStatus.forceLoginType)">
                    <option v-for="option in loginTypeOptions" :key="option.value" :value="option.value">{{ option.label }}</option>
                  </select>
                  <small v-if="authStatus.forceLoginType" class="field-note">Login type is locked to {{ authStatus.forceLoginType }} by server config.</small>
                  <small v-if="authErrors.loginType" class="field-error">{{ authErrors.loginType }}</small>
                </label>
                <label class="field">
                  <span>Admin username</span>
                  <input v-model="auth.form.username" class="text-input compact-input" type="text" autocomplete="username" spellcheck="false" :disabled="authStatus.loading || auth.saving" />
                  <small v-if="authErrors.username" class="field-error">{{ authErrors.username }}</small>
                </label>
                <label class="field">
                  <span>Password</span>
                  <input v-model="auth.form.password" class="text-input compact-input" type="password" autocomplete="new-password" :disabled="authStatus.loading || auth.saving" />
                  <small v-if="authErrors.password" class="field-error">{{ authErrors.password }}</small>
                </label>
                <label class="field">
                  <span>Confirm password</span>
                  <input v-model="auth.form.confirmPassword" class="text-input compact-input" type="password" autocomplete="new-password" :disabled="authStatus.loading || auth.saving" />
                  <small v-if="authErrors.confirmPassword" class="field-error">{{ authErrors.confirmPassword }}</small>
                </label>
              </div>
            </form>
          </div>

          <div v-else-if="currentStep?.id === 'prowlarr'" class="step-panel">
            <p class="step-intro">Connect Prowlarr so the app can manage indexer and search setup.</p>
            <div v-if="prowlarr.error" class="inline-error">{{ prowlarr.error }}</div>
            <form class="setup-form" @submit.prevent="handlePrimaryAction">
              <div class="field-grid">
                <label class="field">
                  <span>Base URL</span>
                  <input v-model="prowlarr.form.baseUrl" class="text-input compact-input" type="text" autocomplete="off" :disabled="prowlarr.loading || prowlarr.saving" />
                  <small v-if="prowlarrErrors.baseUrl" class="field-error">{{ prowlarrErrors.baseUrl }}</small>
                </label>
                <label class="field">
                  <span>API key</span>
                  <input v-model="prowlarr.form.apiKey" class="text-input compact-input" type="password" autocomplete="off" :disabled="prowlarr.loading || prowlarr.saving" />
                  <small v-if="prowlarrErrors.apiKey" class="field-error">{{ prowlarrErrors.apiKey }}</small>
                </label>
                <label class="field">
                  <span>Default language</span>
                  <input v-model="prowlarr.form.defaultLanguage" class="text-input compact-input" type="text" autocomplete="off" :disabled="prowlarr.loading || prowlarr.saving" />
                  <small v-if="prowlarrErrors.defaultLanguage" class="field-error">{{ prowlarrErrors.defaultLanguage }}</small>
                </label>
                <label class="field field-wide">
                  <span>Search template</span>
                  <textarea v-model="prowlarr.form.searchTemplate" class="text-input compact-textarea" rows="2" :disabled="prowlarr.loading || prowlarr.saving"></textarea>
                  <small v-if="prowlarrErrors.searchTemplate" class="field-error">{{ prowlarrErrors.searchTemplate }}</small>
                </label>
              </div>
            </form>
          </div>
          <div v-else-if="currentStep?.id === 'download-client'" class="step-panel">
            <p class="step-intro">Point Narrarr at your download client so completed downloads go to the right place.</p>
            <div v-if="downloadClient.error" class="inline-error">{{ downloadClient.error }}</div>
            <form class="setup-form" @submit.prevent="handlePrimaryAction">
              <div class="field-grid">
                <label class="field">
                  <span>Host</span>
                  <input v-model="downloadClient.form.qbitHost" class="text-input compact-input" type="text" autocomplete="off" :disabled="downloadClient.loading || downloadClient.saving" />
                  <small v-if="downloadClientErrors.qbitHost" class="field-error">{{ downloadClientErrors.qbitHost }}</small>
                </label>
                <label class="field">
                  <span>Port</span>
                  <input v-model="downloadClient.form.qbitPort" class="text-input compact-input" type="number" step="1" inputmode="numeric" :disabled="downloadClient.loading || downloadClient.saving" />
                  <small v-if="downloadClientErrors.qbitPort" class="field-error">{{ downloadClientErrors.qbitPort }}</small>
                </label>
                <label class="field">
                  <span>User</span>
                  <input v-model="downloadClient.form.qbitUser" class="text-input compact-input" type="text" autocomplete="off" :disabled="downloadClient.loading || downloadClient.saving" />
                  <small class="field-note">Optional.</small>
                </label>
                <label class="field">
                  <span>Password</span>
                  <input v-model="downloadClient.form.qbitPass" class="text-input compact-input" type="password" autocomplete="off" :disabled="downloadClient.loading || downloadClient.saving" />
                  <small class="field-note">Optional.</small>
                </label>
                <label class="field">
                  <span>Category</span>
                  <input v-model="downloadClient.form.qbitCategory" class="text-input compact-input" type="text" autocomplete="off" :disabled="downloadClient.loading || downloadClient.saving" />
                  <small v-if="downloadClientErrors.qbitCategory" class="field-error">{{ downloadClientErrors.qbitCategory }}</small>
                </label>
                <label class="field field-wide">
                  <span>Save path</span>
                  <input v-model="downloadClient.form.qbitSavePath" class="text-input compact-input" type="text" autocomplete="off" :disabled="downloadClient.loading || downloadClient.saving" />
                  <small v-if="downloadClientErrors.qbitSavePath" class="field-error">{{ downloadClientErrors.qbitSavePath }}</small>
                </label>
                <label class="field">
                  <span>Complete action</span>
                  <select v-model="downloadClient.form.qbitCompleteAction" class="text-input compact-input" :disabled="downloadClient.loading || downloadClient.saving">
                    <option v-for="option in downloadClientActionOptions" :key="option.value" :value="option.value">{{ option.label }}</option>
                  </select>
                  <small v-if="downloadClientErrors.qbitCompleteAction" class="field-error">{{ downloadClientErrors.qbitCompleteAction }}</small>
                </label>
                <label class="field checkbox-row">
                  <input v-model="downloadClient.form.qbitEnabled" type="checkbox" :disabled="downloadClient.loading || downloadClient.saving" />
                  <span>Enabled</span>
                </label>
              </div>
            </form>
          </div>

          <div v-else-if="currentStep?.id === 'media-management'" class="step-panel">
            <p class="step-intro">Tell Narrarr how to organize files in the media library.</p>
            <div v-if="mediaManagement.error" class="inline-error">{{ mediaManagement.error }}</div>
            <form class="setup-form" @submit.prevent="handlePrimaryAction">
              <div class="field-grid">
                <label class="field field-wide">
                  <span>Library path</span>
                  <input v-model="mediaManagement.form.libraryPath" class="text-input compact-input" type="text" autocomplete="off" :disabled="mediaManagement.loading || mediaManagement.saving" />
                  <small v-if="mediaManagementErrors.libraryPath" class="field-error">{{ mediaManagementErrors.libraryPath }}</small>
                </label>
                <label class="field">
                  <span>Folder pattern</span>
                  <input v-model="mediaManagement.form.folderPattern" class="text-input compact-input" type="text" autocomplete="off" :disabled="mediaManagement.loading || mediaManagement.saving" />
                  <small v-if="mediaManagementErrors.folderPattern" class="field-error">{{ mediaManagementErrors.folderPattern }}</small>
                </label>
                <label class="field">
                  <span>File pattern</span>
                  <input v-model="mediaManagement.form.filePattern" class="text-input compact-input" type="text" autocomplete="off" :disabled="mediaManagement.loading || mediaManagement.saving" />
                  <small v-if="mediaManagementErrors.filePattern" class="field-error">{{ mediaManagementErrors.filePattern }}</small>
                </label>
                <label class="field checkbox-row">
                  <input v-model="mediaManagement.form.useSeriesFolders" type="checkbox" :disabled="mediaManagement.loading || mediaManagement.saving" />
                  <span>Series folders</span>
                </label>
                <label class="field checkbox-row">
                  <input v-model="mediaManagement.form.useHardlinks" type="checkbox" :disabled="mediaManagement.loading || mediaManagement.saving" />
                  <span>Hardlinks</span>
                </label>
                <label class="field checkbox-row">
                  <input v-model="mediaManagement.form.reviewBeforeImport" type="checkbox" :disabled="mediaManagement.loading || mediaManagement.saving" />
                  <span>Review before import</span>
                </label>
              </div>
            </form>
          </div>

          <div v-else-if="currentStep?.id === 'audiobookshelf'" class="step-panel">
            <p class="step-intro">Connect Audiobookshelf and choose the library Narrarr should target.</p>
            <div v-if="audiobookshelf.error" class="inline-error">{{ audiobookshelf.error }}</div>
            <form class="setup-form" @submit.prevent="handlePrimaryAction">
              <div class="field-grid">
                <label class="field">
                  <span>Base URL</span>
                  <input v-model="audiobookshelf.form.baseUrl" class="text-input compact-input" type="text" autocomplete="off" :disabled="audiobookshelf.loading || audiobookshelf.saving" />
                  <small v-if="audiobookshelfErrors.baseUrl" class="field-error">{{ audiobookshelfErrors.baseUrl }}</small>
                </label>
                <label class="field">
                  <span>API token</span>
                  <input v-model="audiobookshelf.form.apiToken" class="text-input compact-input" type="password" autocomplete="off" :disabled="audiobookshelf.loading || audiobookshelf.saving" />
                  <small v-if="audiobookshelfErrors.apiToken" class="field-error">{{ audiobookshelfErrors.apiToken }}</small>
                </label>
                <label class="field field-wide">
                  <span>Library</span>
                  <select v-model="audiobookshelf.form.libraryId" class="text-input compact-input" :disabled="audiobookshelf.loading || audiobookshelf.saving">
                    <option value="">Select a library</option>
                    <option v-for="library in absLibraryOptions" :key="library.id" :value="library.id">{{ library.name }}</option>
                  </select>
                  <small v-if="audiobookshelfErrors.libraryId" class="field-error">{{ audiobookshelfErrors.libraryId }}</small>
                </label>
                <label class="field checkbox-row">
                  <input v-model="audiobookshelf.form.checkDownloaded" type="checkbox" :disabled="audiobookshelf.loading || audiobookshelf.saving" />
                  <span>Check downloaded</span>
                </label>
              </div>
            </form>
          </div>

          <div v-else class="step-panel review-panel">
            <p class="step-intro">Review the setup summary below, then finish to go to your settings page.</p>
            <div class="review-list">
              <article v-for="section in reviewItems" :key="section.id" class="review-card">
                <div class="review-head">
                  <div>
                    <h3>{{ section.title }}</h3>
                    <p>{{ section.statusText }}</p>
                  </div>
                  <div class="review-actions">
                    <StatusPill :tone="section.tone">{{ section.statusText }}</StatusPill>
                    <button v-if="section.editIndex !== null" class="ghost-button" type="button" @click="jumpToStep(section.editIndex)">Edit</button>
                  </div>
                </div>
                <dl class="review-grid">
                  <div v-for="row in section.rows" :key="row.label">
                    <dt>{{ row.label }}</dt>
                    <dd>{{ row.value }}</dd>
                  </div>
                </dl>
              </article>
            </div>
            <div v-if="!canFinish" class="inline-error">Some steps still need attention before setup can finish.</div>
          </div>
        </div>

        <footer class="wizard-actions">
          <button class="ghost-button" type="button" :disabled="currentStepIndex === 0 || currentStepBusy" @click="handleBack">Back</button>
          <button class="primary-button" type="button" :disabled="primaryDisabled" @click="handlePrimaryAction">{{ primaryLabel }}</button>
        </footer>
      </section>
    </section>
  </main>
</template>

<style scoped>
.setup-page {
  min-height: 100vh;
  padding: 1rem;
  display: grid;
  place-items: start center;
  background: radial-gradient(1000px 420px at 50% 0%, rgba(78, 161, 255, 0.14), transparent 58%), linear-gradient(180deg, rgba(21, 27, 34, 0.9), rgba(13, 17, 23, 0.98));
}
.setup-shell { width: min(100%, 1100px); display: grid; gap: 0.8rem; }
.setup-hero { display: flex; align-items: center; justify-content: space-between; gap: 1rem; }
.setup-hero-copy { display: grid; justify-items: end; gap: 0.35rem; max-width: 520px; text-align: right; }
.setup-hero-copy p { margin: 0; color: var(--text-muted); font-size: 0.86rem; }
.setup-banner { border: 1px solid #2f6b45; border-radius: 0.45rem; background: #163020; color: #c6f3d2; padding: 0.55rem 0.7rem; font-size: 0.82rem; }
.wizard-card { border: 1px solid var(--border); border-radius: 0.7rem; background: linear-gradient(180deg, rgba(23, 30, 39, 0.98), rgba(17, 22, 30, 0.98)); overflow: hidden; }
.wizard-stepper { display: grid; gap: 0.35rem; padding: 0.75rem; border-bottom: 1px solid var(--border); }
.stepper-step { display: grid; grid-template-columns: 28px minmax(0, 1fr) auto; align-items: center; gap: 0.65rem; width: 100%; padding: 0.55rem 0.65rem; border-radius: 0.5rem; border: 1px solid transparent; background: #111722; text-align: left; }
.stepper-step:hover:not(:disabled) { border-color: #31404f; background: #131d29; }
.stepper-step.active { border-color: #335172; background: #162234; }
.stepper-step.complete .stepper-index { background: #183323; border-color: #2f6b45; color: #b0f0c0; }
.stepper-step:disabled { opacity: 1; cursor: default; }
.stepper-index { display: grid; place-items: center; width: 1.75rem; height: 1.75rem; border-radius: 999px; border: 1px solid var(--border-strong); background: #0d131a; font-size: 0.76rem; font-weight: 700; }
.stepper-copy { display: grid; gap: 0.1rem; min-width: 0; }
.stepper-title { font-size: 0.86rem; font-weight: 700; }
.stepper-desc { color: var(--text-muted); font-size: 0.73rem; }
.wizard-body { padding: 0.9rem; }
.wizard-head { display: flex; justify-content: space-between; gap: 0.85rem; align-items: start; margin-bottom: 0.75rem; }
.wizard-head h1 { margin: 0.08rem 0 0; font-size: 1.05rem; }
.wizard-head p, .step-intro { margin: 0.18rem 0 0; color: var(--text-muted); font-size: 0.84rem; }
.step-panel { display: grid; gap: 0.7rem; }
.setup-form { display: grid; gap: 0.7rem; }
.field-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 0.5rem; }
.field { display: grid; gap: 0.25rem; min-width: 0; }
.field > span { color: var(--text-muted); font-size: 0.67rem; text-transform: uppercase; letter-spacing: 0.08em; }
.field-note, .field-error { font-size: 0.72rem; }
.field-note { color: var(--text-muted); }
.field-error { color: #ffbbb8; }
.field-wide { grid-column: 1 / -1; }
.compact-input, .compact-textarea { width: 100%; min-width: 0; border: 1px solid var(--border-strong); border-radius: 0.35rem; background: #0f151c; padding: 0.38rem 0.5rem; font-size: 0.83rem; }
.compact-textarea { resize: vertical; min-height: 4rem; }
.checkbox-row { display: flex; align-items: center; gap: 0.45rem; min-height: 32px; }
.checkbox-row > span { text-transform: none; letter-spacing: 0; font-size: 0.83rem; color: var(--text); }
.checkbox-row input[type="checkbox"] { width: 14px; height: 14px; margin: 0; }
.review-list { display: grid; gap: 0.55rem; }
.review-card { border: 1px solid var(--border); border-radius: 0.5rem; background: #111722; padding: 0.7rem; }
.review-head { display: flex; justify-content: space-between; gap: 0.7rem; align-items: start; margin-bottom: 0.55rem; }
.review-head h3 { margin: 0; font-size: 0.9rem; }
.review-head p { margin: 0.08rem 0 0; color: var(--text-muted); font-size: 0.75rem; }
.review-actions { display: flex; align-items: center; gap: 0.45rem; }
.review-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 0.45rem 0.8rem; margin: 0; }
.review-grid div { min-width: 0; }
.review-grid dt { color: var(--text-muted); font-size: 0.66rem; text-transform: uppercase; letter-spacing: 0.08em; }
.review-grid dd { margin: 0.08rem 0 0; font-size: 0.82rem; word-break: break-word; }
.wizard-actions { display: flex; justify-content: space-between; gap: 0.5rem; padding: 0.8rem 0.9rem 0.9rem; border-top: 1px solid var(--border); }
.primary-button, .ghost-button { min-width: 120px; }
@media (max-width: 840px) {
  .setup-hero, .wizard-head, .review-head { flex-direction: column; align-items: stretch; }
  .setup-hero-copy { justify-items: start; text-align: left; }
  .field-grid, .review-grid { grid-template-columns: 1fr; }
  .wizard-actions { flex-direction: column; }
  .primary-button, .ghost-button { width: 100%; }
  .stepper-step { grid-template-columns: 28px minmax(0, 1fr); }
  .stepper-step .pill { grid-column: 2 / -1; justify-self: start; }
}
</style>
