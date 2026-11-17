import { defineConfig, devices } from "@playwright/test";

// The Compose dashboard gate (deploy/compose/dashboard-gate.sh) runs these
// against a live console: DKV_DASHBOARD_URL is its address, DKV_COMPOSE
// the docker compose command that controls the cluster.
export default defineConfig({
  testDir: "./e2e",
  timeout: 180_000,
  retries: 0,
  reporter: [["list"]],
  use: {
    baseURL: process.env.DKV_DASHBOARD_URL ?? "http://localhost:8080",
    trace: "retain-on-failure",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
});
