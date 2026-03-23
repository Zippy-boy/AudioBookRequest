export interface UiConfig {
  apiBaseUrl: string;
  apiKey?: string;
}

export interface AuthStatusResponse {
  initialized: boolean;
  login_type: string | null;
  force_login_type: string | null;
}

export interface SetupStatusResponse {
  setup_required: boolean;
  setup_complete: boolean;
  force_setup: boolean;
}

type AuthMode = "none" | "config" | "user";

type ConfigSource =
  | "envTarget"
  | "queryOverride"
  | "storageOverride"
  | "proxiedConfig"
  | "devFallback"
  | "storedConfig";

type RequestOptions = Omit<RequestInit, "body"> & {
  body?: BodyInit | Record<string, unknown> | null;
  auth?: boolean;
};

const configState: {
  current: UiConfig | null;
} = {
  current: null,
};

const API_KEY_STORAGE_KEY = "narrarr.ui.apiKey";
const USER_API_KEY_STORAGE_KEY = "narrarr.ui.userApiKey";
const API_BASE_STORAGE_KEY = "narrarr.ui.apiBaseUrl";
const API_BASE_OVERRIDE_STORAGE_KEY = "narrarr.ui.backendBaseUrlOverride";
const DEV_BACKEND_BASE_QUERY_PARAM = "backendBaseUrl";
const DEV_BACKEND_BASE_QUERY_PARAM_ALIASES = ["apiBaseUrl"];

const userApiKeyState = {
  current: "",
};

function normalizeBaseUrl(baseUrl: string) {
  const trimmed = baseUrl.trim();
  if (!trimmed) {
    return "";
  }
  if (trimmed === "/") {
    return "/";
  }
  return trimmed.replace(/\/+$/, "");
}

function normalizeBackendBaseUrl(baseUrl: string) {
  const normalized = normalizeBaseUrl(baseUrl);
  if (!normalized) {
    return "";
  }
  if (/\/api$/i.test(normalized)) {
    const withoutApi = normalized.replace(/\/api$/i, "");
    return withoutApi || "/";
  }
  return normalized;
}

function buildConfigUrl(baseUrl: string) {
  const backendBaseUrl = normalizeBackendBaseUrl(baseUrl);
  if (!backendBaseUrl) {
    return "";
  }
  const suffix = "/ui/config.json";
  if (backendBaseUrl === "/") {
    return suffix;
  }
  return `${backendBaseUrl}${suffix}`;
}

function readStoredApiKey() {
  try {
    return window.localStorage.getItem(API_KEY_STORAGE_KEY) ?? "";
  } catch {
    return "";
  }
}

function readStoredUserApiKey() {
  try {
    return window.localStorage.getItem(USER_API_KEY_STORAGE_KEY) ?? "";
  } catch {
    return "";
  }
}

userApiKeyState.current = readStoredUserApiKey();

function readStoredApiBaseUrl() {
  try {
    return window.localStorage.getItem(API_BASE_STORAGE_KEY) ?? "";
  } catch {
    return "";
  }
}

function readStoredBackendBaseUrlOverride() {
  try {
    return window.localStorage.getItem(API_BASE_OVERRIDE_STORAGE_KEY) ?? "";
  } catch {
    return "";
  }
}

function readDevBackendBaseUrlOverride() {
  if (!import.meta.env.DEV || typeof window === "undefined") {
    return "";
  }
  try {
    const params = new URLSearchParams(window.location.search);
    const value =
      params.get(DEV_BACKEND_BASE_QUERY_PARAM) ??
      DEV_BACKEND_BASE_QUERY_PARAM_ALIASES.map((name) => params.get(name)).find(Boolean) ??
      "";
    return normalizeBackendBaseUrl(value);
  } catch {
    return "";
  }
}

function storeApiKey(apiKey: string) {
  try {
    if (apiKey) {
      window.localStorage.setItem(API_KEY_STORAGE_KEY, apiKey);
    } else {
      window.localStorage.removeItem(API_KEY_STORAGE_KEY);
    }
  } catch {
    // Ignore storage failures; the key still stays in memory.
  }
}

function storeUserApiKey(apiKey: string) {
  try {
    if (apiKey) {
      window.localStorage.setItem(USER_API_KEY_STORAGE_KEY, apiKey);
    } else {
      window.localStorage.removeItem(USER_API_KEY_STORAGE_KEY);
    }
  } catch {
    // Ignore storage failures; the key still stays in memory.
  }
}

function storeApiBaseUrl(apiBaseUrl: string) {
  try {
    if (apiBaseUrl) {
      window.localStorage.setItem(API_BASE_STORAGE_KEY, apiBaseUrl);
    } else {
      window.localStorage.removeItem(API_BASE_STORAGE_KEY);
    }
  } catch {
    // Ignore storage failures; values still stay in memory.
  }
}

const setupStatusState = {
  current: null as SetupStatusResponse | null,
  promise: null as Promise<SetupStatusResponse> | null,
};

export function getUiConfig() {
  return configState.current;
}

function normalizeApiKeyResponse(value: unknown) {
  if (typeof value === "string") {
    return value.trim();
  }

  if (!value || typeof value !== "object") {
    return "";
  }

  const payload = value as Record<string, unknown>;
  const candidates = [
    payload.apiKey,
    payload.api_key,
    payload.access_token,
    payload.token,
    payload.key,
  ];

  for (const candidate of candidates) {
    if (typeof candidate === "string" && candidate.trim()) {
      return candidate.trim();
    }
  }

  return "";
}

async function parseConfigResponse(response: Response, sourceUrl: string) {
  if (!response.ok) {
    throw new Error(`Failed to load ${sourceUrl} (${response.status})`);
  }
  const contentType = response.headers.get("content-type") ?? "";
  if (!contentType.includes("application/json")) {
    throw new Error(`Invalid ${sourceUrl} response (expected JSON)`);
  }
  const config = (await response.json()) as UiConfig;
  return config;
}

async function fetchUiConfigs() {
  const candidates: Array<{ url: string; source: ConfigSource }> = [];
  const seen = new Set<string>();
  const addCandidate = (url: string, source: ConfigSource) => {
    const normalizedUrl = url.trim();
    if (!normalizedUrl || seen.has(normalizedUrl)) {
      return;
    }
    seen.add(normalizedUrl);
    candidates.push({ url: normalizedUrl, source });
  };

  if (import.meta.env.DEV) {
    const envTarget =
      normalizeBackendBaseUrl(
        (import.meta.env.VITE_API_PROXY_TARGET as string | undefined) ?? "",
      ) || "";
    if (envTarget) {
      addCandidate(buildConfigUrl(envTarget), "envTarget");
    }

    const queryOverride = readDevBackendBaseUrlOverride();
    if (queryOverride) {
      addCandidate(buildConfigUrl(queryOverride), "queryOverride");
    }

    const storedOverride = normalizeBackendBaseUrl(readStoredBackendBaseUrlOverride());
    if (storedOverride) {
      addCandidate(buildConfigUrl(storedOverride), "storageOverride");
    }
  }

  const configs: UiConfig[] = [];

  addCandidate("/ui/config.json", "proxiedConfig");

  if (import.meta.env.DEV) {
    addCandidate("http://127.0.0.1:8000/ui/config.json", "devFallback");
    addCandidate("http://127.0.0.1:8001/ui/config.json", "devFallback");
    addCandidate("http://localhost:8000/ui/config.json", "devFallback");
    addCandidate("http://localhost:8001/ui/config.json", "devFallback");
  }

  for (const candidate of candidates) {
    try {
      const response = await fetch(candidate.url, { cache: "no-store" });
      const config = await parseConfigResponse(response, candidate.url);
      configs.push(config);
    } catch (error) {
      void error;
    }
  }

  return configs;
}

function getConfiguredApiKey() {
  return configState.current?.apiKey ?? readStoredApiKey();
}

async function isConfigValid(config: UiConfig) {
  const apiBaseUrl = normalizeBaseUrl(config.apiBaseUrl || "");
  if (!apiBaseUrl) {
    return false;
  }
  try {
    const response = await fetch(`${apiBaseUrl}/auth/status`, {
      cache: "no-store",
    });
    return response.ok;
  } catch {
    return false;
  }
}

export async function loadUiConfig() {
  const remoteConfigs = await fetchUiConfigs();
  const freshBaseUrls = new Set<string>();

  for (const config of remoteConfigs) {
    const normalized: UiConfig = {
      apiBaseUrl: normalizeBaseUrl(config.apiBaseUrl || "/api"),
      apiKey: config.apiKey || "",
    };
    if (normalized.apiBaseUrl) {
      freshBaseUrls.add(normalized.apiBaseUrl);
    }
    if (await isConfigValid(normalized)) {
      configState.current = normalized;
      if (normalized.apiKey) {
        storeApiKey(normalized.apiKey);
      }
      storeApiBaseUrl(normalized.apiBaseUrl);
      return normalized;
    }
  }

  const storedApiBaseUrl = normalizeBaseUrl(readStoredApiBaseUrl());
  const storedApiKey = readStoredApiKey();
  if (storedApiBaseUrl && !freshBaseUrls.has(storedApiBaseUrl)) {
    const storedConfig: UiConfig = {
      apiBaseUrl: storedApiBaseUrl,
      apiKey: storedApiKey,
    };
    if (await isConfigValid(storedConfig)) {
      configState.current = storedConfig;
      if (storedConfig.apiKey) {
        storeApiKey(storedConfig.apiKey);
      }
      storeApiBaseUrl(storedConfig.apiBaseUrl);
      return storedConfig;
    }
  }

  throw new Error(
    "Failed to load a valid UI API configuration from the backend, proxy target, or stored settings",
  );
}

export function setApiKey(apiKey: string) {
  setUserApiKey(apiKey);
}

export function getApiKey() {
  return getApiKeyForRequest();
}

export function getUserApiKey() {
  return userApiKeyState.current;
}

export function isAuthenticated() {
  return Boolean(userApiKeyState.current);
}

export function setUserApiKey(apiKey: string) {
  userApiKeyState.current = apiKey.trim();
  storeUserApiKey(userApiKeyState.current);
}

export function clearUserApiKey() {
  userApiKeyState.current = "";
  storeUserApiKey("");
}

export function logout() {
  clearUserApiKey();
}

function getApiKeyForRequest() {
  return userApiKeyState.current || getConfiguredApiKey();
}

export async function login(username: string, password: string) {
  const response = await request<unknown>(
    "/auth/login",
    {
      method: "POST",
      auth: false,
      body: { username, password },
    },
  );
  const apiKey = normalizeApiKeyResponse(response);
  if (!apiKey) {
    throw new Error("Login succeeded, but the backend did not return an API key");
  }
  setUserApiKey(apiKey);
  return apiKey;
}

export function getAuthStatus() {
  return api.get<AuthStatusResponse>("/auth/status", { auth: false });
}

export function initializeAuth(body: {
  login_type: string;
  username: string;
  password: string;
  confirm_password: string;
}) {
  return api.post<void>("/auth/initialize", body, { auth: false });
}

async function fetchSetupStatus() {
  return request<SetupStatusResponse>("/setup", { method: "GET" });
}

export async function getSetupStatus(force = false) {
  if (!force && setupStatusState.current) {
    return setupStatusState.current;
  }
  if (!force && setupStatusState.promise) {
    return setupStatusState.promise;
  }
  const promise = fetchSetupStatus().finally(() => {
    setupStatusState.promise = null;
  });
  setupStatusState.promise = promise;
  const status = await promise;
  setupStatusState.current = status;
  return status;
}

export async function completeSetup() {
  await request<void>("/setup/complete", { method: "POST" });
  setupStatusState.current = {
    setup_required: false,
    setup_complete: true,
    force_setup: setupStatusState.current?.force_setup ?? false,
  };
  return setupStatusState.current;
}

function buildUrl(path: string) {
  const config = configState.current;
  if (!config) {
    throw new Error("UI config has not been loaded");
  }
  const base = normalizeBaseUrl(config.apiBaseUrl);
  const suffix = path.startsWith("/") ? path : `/${path}`;
  return `${base}${suffix}`;
}

let refreshPromise: Promise<UiConfig> | null = null;

async function refreshUiConfig() {
  if (!refreshPromise) {
    refreshPromise = loadUiConfig().finally(() => {
      refreshPromise = null;
    });
  }
  return refreshPromise;
}

async function request<T>(path: string, options: RequestOptions = {}) {
  const config = configState.current;
  if (!config) {
    throw new Error("UI config has not been loaded");
  }

  const headers = new Headers(options.headers);
  const authMode: AuthMode = options.auth === false
    ? "none"
    : userApiKeyState.current
      ? "user"
      : config.apiKey
        ? "config"
        : "none";

  const apiKey = authMode === "user"
    ? userApiKeyState.current
    : authMode === "config"
      ? config.apiKey
      : "";

  if (authMode !== "none" && apiKey && !headers.has("Authorization")) {
    headers.set("Authorization", `Bearer ${apiKey}`);
  }

  let body = options.body;
  if (
    body &&
    typeof body === "object" &&
    !(body instanceof FormData) &&
    !(body instanceof URLSearchParams)
  ) {
    headers.set("Content-Type", "application/json");
    body = JSON.stringify(body);
  }

  let response = await fetch(buildUrl(path), {
    ...options,
    headers,
    body,
  });
  if (response.status === 401) {
    if (authMode === "user") {
      clearUserApiKey();
      throw new Error(
        `Your saved session was rejected when calling ${normalizeBaseUrl(
          config.apiBaseUrl,
        )}${path.startsWith("/") ? path : `/${path}`}. Please log in again.`,
      );
    }

    if (authMode === "config") {
      try {
        await refreshUiConfig();
      } catch (error) {
        const currentBaseUrl = normalizeBaseUrl(
          configState.current?.apiBaseUrl || config.apiBaseUrl,
        );
        const refreshMessage =
          error instanceof Error ? ` Config refresh failed: ${error.message}` : "";
        throw new Error(
          `Unauthorized when calling ${currentBaseUrl}${path.startsWith("/") ? path : `/${path}`}. ` +
            `The UI could not refresh its config for ${currentBaseUrl}.${refreshMessage}`,
        );
      }
      const retryConfig = configState.current;
      if (retryConfig?.apiKey) {
        headers.set("Authorization", `Bearer ${retryConfig.apiKey}`);
        response = await fetch(buildUrl(path), {
          ...options,
          headers,
          body,
        });
      }
      if (response.status === 401) {
        const currentBaseUrl = normalizeBaseUrl(
          configState.current?.apiBaseUrl || config.apiBaseUrl,
        );
        throw new Error(
          `Unauthorized when calling ${currentBaseUrl}${path.startsWith("/") ? path : `/${path}`}. ` +
            `The UI refreshed its config, but the backend at ${currentBaseUrl} still rejected the API key. ` +
            `Check VITE_API_PROXY_TARGET or the narrarr.ui.backendBaseUrlOverride localStorage key, and confirm the backend on ${currentBaseUrl} is the intended target.`,
        );
      }
    }
  }

  if (response.status === 204) {
    return undefined as T;
  }

  const contentType = response.headers.get("content-type") ?? "";
  const payload = contentType.includes("application/json")
    ? await response.json()
    : await response.text();

  if (!response.ok) {
    const detail =
      typeof payload === "string"
        ? payload
        : (payload as { detail?: string; message?: string })?.detail ??
          (payload as { detail?: string; message?: string })?.message ??
          `Request failed with status ${response.status}`;
    throw new Error(detail);
  }

  return payload as T;
}

export const api = {
  get<T>(path: string, options?: RequestOptions) {
    return request<T>(path, { ...options, method: "GET" });
  },
  post<T>(path: string, body?: RequestOptions["body"], options?: RequestOptions) {
    return request<T>(path, { ...options, method: "POST", body });
  },
  put<T>(path: string, body?: RequestOptions["body"], options?: RequestOptions) {
    return request<T>(path, { ...options, method: "PUT", body });
  },
  patch<T>(path: string, body?: RequestOptions["body"], options?: RequestOptions) {
    return request<T>(path, { ...options, method: "PATCH", body });
  },
  delete<T>(path: string, options?: RequestOptions) {
    return request<T>(path, { ...options, method: "DELETE" });
  },
};
