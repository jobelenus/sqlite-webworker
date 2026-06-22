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
      entry: "lib/main.ts",
      formats: ["es"],
      fileName: "sqlite-webworker",
    },
    rollupOptions: {
      external: ["comlink", "ulid", "@sqlite.org/sqlite-wasm"],
    },
  },
});
