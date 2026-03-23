export function formatNumber(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "0";
  }
  return new Intl.NumberFormat("en-US").format(value);
}

export function formatDate(value: string | Date | null | undefined) {
  if (!value) {
    return "-";
  }
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  return new Intl.DateTimeFormat("en-US", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date);
}

export function formatDuration(minutes: number | null | undefined) {
  if (minutes === null || minutes === undefined) {
    return "-";
  }
  const hours = Math.floor(minutes / 60);
  const remainingMinutes = minutes % 60;
  if (!hours) {
    return `${remainingMinutes}m`;
  }
  return `${hours}h ${remainingMinutes.toString().padStart(2, "0")}m`;
}

export function joinList(values: unknown) {
  if (!Array.isArray(values) || values.length === 0) {
    return "-";
  }
  return values.join(", ");
}
