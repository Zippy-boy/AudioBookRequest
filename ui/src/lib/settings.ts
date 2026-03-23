export type SectionTone = "neutral" | "success" | "warning" | "danger" | "info";

export interface ProwlarrSettingsResponse {
  base_url: string;
  api_key: string;
  selected_categories: number[];
  selected_indexers: number[];
  default_language: string;
  search_template: string;
  indexers?: {
    state?: "ok" | "missingUrlKey" | "failedFetch" | string;
    error?: string | null;
  };
}

export interface ProwlarrForm {
  baseUrl: string;
  apiKey: string;
  defaultLanguage: string;
  searchTemplate: string;
}

export function toProwlarrForm(data?: Partial<ProwlarrSettingsResponse> | null): ProwlarrForm {
  return {
    baseUrl: data?.base_url ?? "",
    apiKey: data?.api_key ?? "",
    defaultLanguage: data?.default_language ?? "",
    searchTemplate: data?.search_template ?? "",
  };
}

export interface DownloadRange {
  from_kbits: number;
  to_kbits: number;
}

export interface DownloadSettingsResponse {
  auto_download: boolean;
  flac_range: DownloadRange;
  m4b_range: DownloadRange;
  mp3_range: DownloadRange;
  unknown_audio_range: DownloadRange;
  unknown_range: DownloadRange;
  min_seeders: number;
  name_ratio: number;
  title_ratio: number;
  indexer_flags: Array<{ flag: string; score: number }>;
}

export interface DownloadForm {
  autoDownload: boolean;
  flacFrom: string;
  flacTo: string;
  m4bFrom: string;
  m4bTo: string;
  mp3From: string;
  mp3To: string;
  unknownAudioFrom: string;
  unknownAudioTo: string;
  unknownFrom: string;
  unknownTo: string;
  minSeeders: string;
  nameRatio: string;
  titleRatio: string;
}

function rangeToStrings(range?: DownloadRange | null) {
  return {
    from: range ? String(range.from_kbits) : "",
    to: range ? String(range.to_kbits) : "",
  };
}

export function toDownloadForm(data?: Partial<DownloadSettingsResponse> | null): DownloadForm {
  const flac = rangeToStrings(data?.flac_range);
  const m4b = rangeToStrings(data?.m4b_range);
  const mp3 = rangeToStrings(data?.mp3_range);
  const unknownAudio = rangeToStrings(data?.unknown_audio_range);
  const unknown = rangeToStrings(data?.unknown_range);

  return {
    autoDownload: data?.auto_download ?? false,
    flacFrom: flac.from,
    flacTo: flac.to,
    m4bFrom: m4b.from,
    m4bTo: m4b.to,
    mp3From: mp3.from,
    mp3To: mp3.to,
    unknownAudioFrom: unknownAudio.from,
    unknownAudioTo: unknownAudio.to,
    unknownFrom: unknown.from,
    unknownTo: unknown.to,
    minSeeders: data?.min_seeders === undefined ? "" : String(data.min_seeders),
    nameRatio: data?.name_ratio === undefined ? "" : String(data.name_ratio),
    titleRatio: data?.title_ratio === undefined ? "" : String(data.title_ratio),
  };
}

export interface DownloadClientSettingsResponse {
  qbit_base_url: string;
  qbit_user: string;
  qbit_pass: string;
  qbit_category: string;
  qbit_save_path: string;
  qbit_enabled: boolean;
  qbit_complete_action: string;
}

export const completeActionOptions = ["copy", "move", "hardlink"] as const;
export type CompleteAction = (typeof completeActionOptions)[number];

export interface DownloadClientForm {
  qbitBaseUrl: string;
  qbitUser: string;
  qbitPass: string;
  qbitCategory: string;
  qbitSavePath: string;
  qbitEnabled: boolean;
  qbitCompleteAction: CompleteAction | string;
}

export function toDownloadClientForm(
  data?: Partial<DownloadClientSettingsResponse> | null
): DownloadClientForm {
  return {
    qbitBaseUrl: data?.qbit_base_url ?? "",
    qbitUser: data?.qbit_user ?? "",
    qbitPass: data?.qbit_pass ?? "",
    qbitCategory: data?.qbit_category ?? "",
    qbitSavePath: data?.qbit_save_path ?? "",
    qbitEnabled: data?.qbit_enabled ?? false,
    qbitCompleteAction: data?.qbit_complete_action ?? "copy",
  };
}

export interface MediaManagementSettingsResponse {
  library_path: string;
  folder_pattern: string;
  file_pattern: string;
  use_series_folders: boolean;
  use_hardlinks: boolean;
  review_before_import: boolean;
}

export interface MediaManagementForm {
  libraryPath: string;
  folderPattern: string;
  filePattern: string;
  useSeriesFolders: boolean;
  useHardlinks: boolean;
  reviewBeforeImport: boolean;
}

export function toMediaManagementForm(
  data?: Partial<MediaManagementSettingsResponse> | null
): MediaManagementForm {
  return {
    libraryPath: data?.library_path ?? "",
    folderPattern: data?.folder_pattern ?? "",
    filePattern: data?.file_pattern ?? "",
    useSeriesFolders: data?.use_series_folders ?? false,
    useHardlinks: data?.use_hardlinks ?? false,
    reviewBeforeImport: data?.review_before_import ?? false,
  };
}

export interface AudiobookshelfLibrary {
  id: string;
  name: string;
}

export interface AudiobookshelfSettingsResponse {
  abs_base_url: string;
  abs_api_token: string;
  abs_library_id: string;
  abs_check_downloaded: boolean;
  abs_libraries: AudiobookshelfLibrary[];
}

export interface AudiobookshelfForm {
  baseUrl: string;
  apiToken: string;
  libraryId: string;
  checkDownloaded: boolean;
}

export function toAudiobookshelfForm(
  data?: Partial<AudiobookshelfSettingsResponse> | null
): AudiobookshelfForm {
  return {
    baseUrl: data?.abs_base_url ?? "",
    apiToken: data?.abs_api_token ?? "",
    libraryId: data?.abs_library_id ?? "",
    checkDownloaded: data?.abs_check_downloaded ?? false,
  };
}

export type LoginTypeChoice = "basic" | "forms" | "oidc" | "none";

export const loginTypeOptions: Array<{ value: LoginTypeChoice; label: string }> = [
  { value: "basic", label: "basic" },
  { value: "forms", label: "forms" },
  { value: "oidc", label: "oidc" },
  { value: "none", label: "none" },
];

export interface SecuritySettingsResponse {
  login_type: LoginTypeChoice | string;
  access_token_expiry: number;
  min_password_length: number;
}

export interface SecurityForm {
  loginType: LoginTypeChoice | string;
  accessTokenExpiry: string;
  minPasswordLength: string;
}

export function toSecurityForm(data?: Partial<SecuritySettingsResponse> | null): SecurityForm {
  return {
    loginType: data?.login_type ?? "basic",
    accessTokenExpiry:
      data?.access_token_expiry === undefined ? "" : String(data.access_token_expiry),
    minPasswordLength:
      data?.min_password_length === undefined ? "" : String(data.min_password_length),
  };
}

export interface AccountApiKeySummary {
  id: string;
  name: string;
  enabled: boolean;
}

export interface AccountCreateApiKeyResponse {
  name: string;
  key: string;
}

export function toUrlSearchParams(
  values: Record<string, string | number | boolean | null | undefined>
) {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(values)) {
    if (value === null || value === undefined) {
      continue;
    }
    params.set(key, String(value));
  }
  return params;
}

export function parseDecimalInput(
  value: string | number,
  label: string,
  options: { min?: number; max?: number } = {}
) {
  const trimmed = String(value).trim();
  const parsed = Number(trimmed);
  if (!trimmed || Number.isNaN(parsed)) {
    throw new Error(`${label} must be a number`);
  }
  if (options.min !== undefined && parsed < options.min) {
    throw new Error(`${label} must be at least ${options.min}`);
  }
  if (options.max !== undefined && parsed > options.max) {
    throw new Error(`${label} must be at most ${options.max}`);
  }
  return parsed;
}

export function parseIntegerInput(
  value: string | number,
  label: string,
  options: { min?: number; max?: number } = {}
) {
  const parsed = parseDecimalInput(value, label, options);
  if (!Number.isInteger(parsed)) {
    throw new Error(`${label} must be an integer`);
  }
  return parsed;
}

export function parseRangeInput(fromValue: string, toValue: string, label: string) {
  const from = parseDecimalInput(fromValue, `${label} minimum`, { min: 0 });
  const to = parseDecimalInput(toValue, `${label} maximum`, { min: 0 });
  if (from > to) {
    throw new Error(`${label} minimum cannot exceed maximum`);
  }
  return {
    from_kbits: from,
    to_kbits: to,
  };
}
