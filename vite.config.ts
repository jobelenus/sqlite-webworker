import { defineConfig } from "vite";
import pkg from "./package.json";

// Library build. Emits an ES module entry plus the two workers as separate
// assets in dist/. Runtime deps are externalized so consumers install them once.
export default defineConfig({
  define: {
    __LIB_VERSION__: JSON.stringify(pkg.version),
  },
  worker: {
    format: "es",
  },
  build: {
    lib: {
      // Three entry points:
      //  - main:      the main-thread client (init/createDb/dbExec/...).
      //  - db-worker: `defineDbWorker` + helpers, imported into a consumer's
      //               own DB worker so their SQL runs in the worker.
      //  - db-core:   raw sqlite + lock helpers for use inside that worker.
      entry: {
        "sqlite-webworker": "lib/main.ts",
        "db-worker": "lib/db-worker.ts",
        "db-core": "lib/db-core.ts",
      },
      formats: ["es"],
    },
    rollupOptions: {
      external: ["comlink", "ulid", "@sqlite.org/sqlite-wasm"],
      output: {
        // Keep shared chunks (db-core imported by db-worker) stable + named.
        chunkFileNames: "chunks/[name]-[hash].js",
      },
    },
  },
});
