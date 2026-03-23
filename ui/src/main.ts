import { createApp } from "vue";
import App from "./App.vue";
import { router } from "./router";
import { loadUiConfig } from "./lib/api";
import "./style.css";

async function bootstrap() {
  await loadUiConfig();
  const app = createApp(App);
  app.use(router);
  app.mount("#app");
}

bootstrap().catch((error) => {
  console.error("Failed to start UI", error);
  const el = document.getElementById("app");
  if (el) {
    el.innerHTML = `
      <div style="font-family: system-ui, sans-serif; padding: 2rem; color: #b42318;">
        <h1 style="margin: 0 0 0.5rem;">Narrarr UI failed to start</h1>
        <p style="margin: 0;">${String(error instanceof Error ? error.message : error)}</p>
      </div>
    `;
  }
});
