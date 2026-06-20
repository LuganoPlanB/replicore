import path from "node:path"
import { fileURLToPath } from "node:url"

import { defineConfig } from "vite"
import { svelte } from "@sveltejs/vite-plugin-svelte"

const rootDir = fileURLToPath(new URL(".", import.meta.url))

export default defineConfig({
  root: rootDir,
  plugins: [svelte()],
  build: {
    outDir: path.resolve(rootDir, "../dist/setup-ui"),
    emptyOutDir: true
  }
})
