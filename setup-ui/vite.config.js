import path from "node:path"
import { fileURLToPath } from "node:url"

import { defineConfig } from "vite"
import { svelte } from "@sveltejs/vite-plugin-svelte"

const rootDir = fileURLToPath(new URL(".", import.meta.url))

const setupServerPort = process.env.REPLICORE_SETUP_PORT ?? "37210"

export default defineConfig({
  root: rootDir,
  plugins: [svelte()],
  server: {
    proxy: {
      "/setup": {
        target: `http://127.0.0.1:${setupServerPort}`,
        changeOrigin: true
      }
    }
  },
  build: {
    outDir: path.resolve(rootDir, "../dist/setup-ui"),
    emptyOutDir: true
  }
})
