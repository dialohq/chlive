import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { LiveClient, defaultUrl } from "@chlive/client";
import { LiveClientProvider } from "@chlive/client/react";

import { App } from "./App.tsx";
import "./styles.css";

const client = new LiveClient({ url: defaultUrl() });

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <LiveClientProvider client={client}>
      <App />
    </LiveClientProvider>
  </StrictMode>,
);
