/// <reference types="vitest" />
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// In development (npm run dev) the API is proxied to a running console;
// DKV_CONSOLE names it (default 127.0.0.1:8080, as in the example config).
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: { "/api": `http://${process.env.DKV_CONSOLE ?? "127.0.0.1:8080"}` },
  },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["./src/setupTests.ts"],
    include: ["src/**/*.test.{ts,tsx}"],
  },
});
