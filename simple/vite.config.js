import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const src = (p) => fileURLToPath(new URL(`../client/src/${p}`, import.meta.url));

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: [
      { find: /^@chlive\/client\/react$/, replacement: src("react.ts") },
      { find: /^@chlive\/client$/, replacement: src("index.ts") },
    ],
  },
  server: {
    port: 5174,
    proxy: {
      "/live": { target: "ws://localhost:8080", ws: true },
      // Redpanda HTTP proxy (pandaproxy): writes go straight to Kafka
      "/kafka": { target: "http://localhost:18082", rewrite: (p) => p.replace(/^\/kafka/, "") },
    },
  },
});
