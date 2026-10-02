import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const src = (p) => fileURLToPath(new URL(`../../client/src/${p}`, import.meta.url));

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
      "/live": { target: "ws://127.0.0.1:8080", ws: true },
      // Redpanda's HTTP proxy (dnvr `redpanda` process): writes go straight to Kafka
      "/kafka": { target: "http://127.0.0.1:8082", rewrite: (p) => p.replace(/^\/kafka/, "") },
    },
  },
});
