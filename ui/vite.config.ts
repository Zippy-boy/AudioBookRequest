import { defineConfig, loadEnv } from "vite";
import vue from "@vitejs/plugin-vue";

function normalizeBackendBaseUrl(baseUrl: string) {
  const trimmed = baseUrl.trim();
  if (!trimmed) {
    return "";
  }
  if (trimmed === "/") {
    return "/";
  }
  const withoutTrailingSlash = trimmed.replace(/\/+$/, "");
  if (/\/api$/i.test(withoutTrailingSlash)) {
    const withoutApi = withoutTrailingSlash.replace(/\/api$/i, "");
    return withoutApi || "/";
  }
  return withoutTrailingSlash;
}

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const proxyTarget =
    normalizeBackendBaseUrl(
      process.env.VITE_API_PROXY_TARGET || env.VITE_API_PROXY_TARGET || "http://127.0.0.1:8000",
    ) || "http://127.0.0.1:8000";

  return {
    plugins: [vue()],
    base: "/ui/",
    server: {
      proxy: {
        "/api": {
          target: proxyTarget,
          changeOrigin: true,
        },
        "/ui/config.json": {
          target: proxyTarget,
          changeOrigin: true,
        },
      },
    },
  };
});
