import { defineConfig } from "@playwright/test";
export default defineConfig({
  testDir: "./test",
  fullyParallel: false,
  use: {
    baseURL: "http://127.0.0.1:4329",
    viewport: { width: 1440, height: 1000 },
  },
  webServer: {
    command: "pnpm dev --port 4329 --strictPort",
    url: "http://127.0.0.1:4329",
    reuseExistingServer: false,
  },
  reporter: "list",
});
