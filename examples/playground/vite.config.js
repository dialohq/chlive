import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The server and Redpanda's HTTP proxy, as the dnvr `playground` process gets them.
const server = new URL(process.env.CHLIVE_URL ?? "ws://127.0.0.1:8080/live");
const kafkaProxy = process.env.PROXY_URL ?? "http://127.0.0.1:8082";

const src = (p) => fileURLToPath(new URL(`../../client/src/${p}`, import.meta.url));

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: [
      { find: /^@chlive\/client\/react$/, replacement: src("react.ts") },
      { find: /^@chlive\/client\/cache$/, replacement: src("cache/index.ts") },
      { find: /^@chlive\/client$/, replacement: src("index.ts") },
    ],
  },
  // sqlite-wasm loads its .wasm next to itself; pre-bundling would break that
  optimizeDeps: { exclude: ["@sqlite.org/sqlite-wasm"] },
  worker: { format: "es" },
  server: {
    // also reached through `tailscale serve` (https://<machine>.<tailnet>.ts.net)
    allowedHosts: [".ts.net"],
    proxy: {
      "/live": { target: server.origin, ws: true },
      // Redpanda's HTTP proxy (dnvr `redpanda` process): writes go straight to Kafka
      "/kafka": { target: kafkaProxy, rewrite: (p) => p.replace(/^\/kafka/, "") },
    },
  },
});
