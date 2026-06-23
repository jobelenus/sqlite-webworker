// The lib's default dedicated DB worker: leader-election plumbing + SQLite core,
// with no app-specific methods. Used when a consumer calls `init`/`createDb`
// without supplying their own DB worker. Drive it from the main thread via the
// generic `dbExec`. To own SQL inside the worker instead, build your own with
// `defineDbWorker` (see lib/db-worker.ts) and pass it to `createDb`.
import { defineDbWorker } from "./db-worker";

defineDbWorker();
