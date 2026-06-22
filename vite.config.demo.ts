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
  server: {
    // The demo lives in demo/ but imports the library source from ../lib.
    fs: { allow: [".."] },
  },
  build: {
    outDir: "../demo-dist",
    emptyOutDir: true,
  },
});
