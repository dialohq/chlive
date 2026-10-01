import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const server = process.env.CHLIVE_SERVER ?? "http://127.0.0.1:8080";

export default defineConfig({
  plugins: [react()],
  resolve: {
    // Use the client sources directly so the demo needs no separate client build.
    alias: [
      { find: /^@chlive\/client\/react$/, replacement: fileURLToPath(new URL("../client/src/react.ts", import.meta.url)) },
      { find: /^@chlive\/client$/, replacement: fileURLToPath(new URL("../client/src/index.ts", import.meta.url)) },
    ],
  },
  server: {
    proxy: {
      "/live": { target: server.replace(/^http/, "ws"), ws: true },
      "/debug": { target: server },
    },
  },
});
