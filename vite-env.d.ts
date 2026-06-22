/// <reference types="vite/client" />
/// <reference types="vite/types/importMeta.d.ts" />

// Injected at build time via Vite `define` (see vite.config.ts).
declare const __LIB_VERSION__: string;

interface ImportMetaEnv {
  readonly DEBUG: string;
}
