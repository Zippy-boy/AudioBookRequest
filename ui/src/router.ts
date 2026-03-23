import { createRouter, createWebHistory } from "vue-router";
import { getSetupStatus, getAuthStatus, isAuthenticated } from "./lib/api";
import DashboardView from "./views/DashboardView.vue";
import DownloadsView from "./views/DownloadsView.vue";
import LoginView from "./views/LoginView.vue";
import RecommendationsView from "./views/RecommendationsView.vue";
import RequestsView from "./views/RequestsView.vue";
import SearchView from "./views/SearchView.vue";
import SettingsView from "./views/SettingsView.vue";
import SetupWizardView from "./views/SetupWizardView.vue";

export const router = createRouter({
  history: createWebHistory("/ui/"),
  routes: [
    { path: "/", redirect: "/dashboard" },
    {
      path: "/login",
      name: "login",
      component: LoginView,
      meta: { requiresAuth: false, hideShell: true },
    },
    {
      path: "/setup",
      name: "setup",
      component: SetupWizardView,
      meta: { requiresAuth: false, hideShell: true },
    },
    { path: "/dashboard", name: "dashboard", component: DashboardView },
    { path: "/search", name: "search", component: SearchView },
    { path: "/requests", name: "requests", component: RequestsView },
    { path: "/downloads", name: "downloads", component: DownloadsView },
    { path: "/recommendations", name: "recommendations", component: RecommendationsView },
    { path: "/settings", name: "settings", component: SettingsView },
  ],
});

router.beforeEach(async (to) => {
  try {
    const setupStatus = await getSetupStatus();
    if (setupStatus.setup_required) {
      return to.name === "setup" ? true : "/setup";
    }
    if (to.name === "setup") {
      return "/dashboard";
    }
  } catch {
    // Ignore setup status failures and fall back to auth flow.
  }

  const authenticated = isAuthenticated();
  if (to.meta.requiresAuth === false) {
    if (to.name === "login" && authenticated) {
      return "/dashboard";
    }
    return true;
  }
  if (authenticated) {
    return true;
  }

  try {
    const status = await getAuthStatus();
    if (!status.initialized) {
      return "/setup";
    }
  } catch {
    // If auth status is unavailable, fall back to the login page.
  }

  return "/login";
});
