import { defineConfig } from "vite";
import pkg from "./package.json";

// Demo app build/serve. Roots at demo/ and reuses the library's version define
// so the worker spawning in lib/ resolves the same way it does when published.
export default defineConfig({
  root: "demo",
  define: {
    __LIB_VERSION__: JSON.stringify(pkg.version),
  },
  worker: {
    format: "es",
  },
  // @sqlite.org/sqlite-wasm ships its own .wasm and resolves it relative to its
  // JS. Vite's dep pre-bundler rewrites that path and the .wasm then 404s (Vite
  // serves index.html instead -> "Incorrect response MIME type / expected magic
  // word"). Excluding it from optimizeDeps keeps the package's own asset
  // resolution intact.
  optimizeDeps: {
    exclude: ["@sqlite.org/sqlite-wasm"],
  },
  server: {
    // The demo lives in demo/ but imports the library source from ../lib.
    fs: { allow: [".."] },
    // OPFS persistence needs SharedArrayBuffer, which requires the page to be
    // cross-origin isolated. Without these the worker falls back to a transient
    // in-memory DB (data lost on reload).
    headers: {
      "Cross-Origin-Opener-Policy": "same-origin",
      "Cross-Origin-Embedder-Policy": "require-corp",
    },
  },
  preview: {
    headers: {
      "Cross-Origin-Opener-Policy": "same-origin",
      "Cross-Origin-Embedder-Policy": "require-corp",
    },
  },
  build: {
    outDir: "../demo-dist",
    emptyOutDir: true,
  },
});
