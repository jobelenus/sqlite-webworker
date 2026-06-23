# sqlite-webworker

SQLite (via [`@sqlite.org/sqlite-wasm`](https://github.com/sqlite/sqlite-wasm)) running in a
Web Worker, with a `SharedWorker` multiplexer that coordinates a single database leader across
all open tabs. Communication is over [Comlink](https://github.com/GoogleChromeLabs/comlink).

## Install

```sh
npm install @your-scope/sqlite-webworker
```

> Replace `@your-scope` with the actual npm scope this package is published under.

## What ships in the package

```
dist/
  sqlite-webworker.js        # main entry  (".")
  db-worker.js               # "/db-worker": defineDbWorker + helpers for your own DB worker
  db-core.js                 # "/db-core":   raw sqlite + lock helpers
  *.d.ts                     # types for each entry
  assets/
    webworker-*.js           # default per-tab DB worker  (self-contained)
    shared_webworker-*.js    # cross-tab SharedWorker      (self-contained)
    sqlite3-*.js             # sqlite-wasm runtime
```

`db-worker` / `db-core` are imported **into your own DB worker** (Option B in Usage) so your
SQL runs in the worker. The `assets/` workers are the default ones used when you don't supply
your own.

The two worker files are **standalone assets**. They are not inlined into
`sqlite-webworker.js`, because:

- A `SharedWorker` is keyed by `(origin, name, script-URL)`. If it were inlined as a
  `blob:` URL, every tab would get a *different* URL and therefore its own worker — which
  defeats the whole point of sharing one database across tabs.
- Shipping the workers as plain `.js` files means **you do not need a Vite (or any
  worker-aware) bundler** to consume this library. The files just need to be served at a
  known URL. See below.

At runtime the entry loads the workers from an **origin-absolute** path:

```js
new SharedWorker(new URL("/assets/shared_webworker-<hash>.js", import.meta.url), ...)
```

So the worker files must be reachable at `https://your-app/assets/...`.

---

## Setup

### If you are NOT using a Vite bundler (the default path) — copy the assets

Your bundler will not know about the worker files (the references are intentionally opaque,
so no bundler is required). Copy them into whatever directory your server serves at
`/assets`:

```sh
cp -R node_modules/@your-scope/sqlite-webworker/dist/assets ./public/assets
```

Most setups serve a `public/` (or `static/`, `www/`, `dist/`) directory at the site root, so
`public/assets/...` becomes `https://your-app/assets/...` — which is exactly where the
library looks.

Re-run this copy whenever you upgrade the package (the hashed filenames change between
versions). A simple way is a `postinstall` script in your `package.json`:

```json
{
  "scripts": {
    "postinstall": "cp -R node_modules/@your-scope/sqlite-webworker/dist/assets ./public/assets"
  }
}
```

> The assets must end up at the `/assets/` path on the origin that loads them. If your app is
> served from a sub-path or a CDN, place the files so that `/<your-base>/assets/...` resolves.

### If you ARE using Vite — let Vite copy the assets for you

You can skip the manual copy and let Vite move the worker assets into your build output using
[`vite-plugin-static-copy`](https://github.com/sapphi-red/vite-plugin-static-copy):

```sh
npm install -D vite-plugin-static-copy
```

```ts
// vite.config.ts
import { defineConfig } from "vite";
import { viteStaticCopy } from "vite-plugin-static-copy";

export default defineConfig({
  plugins: [
    viteStaticCopy({
      targets: [
        {
          src: "node_modules/@your-scope/sqlite-webworker/dist/assets/*",
          dest: "assets",
        },
      ],
    }),
  ],
});
```

This copies the workers into `dist/assets/` on every build, so they are served at
`/assets/...` with no manual step and no stale files after an upgrade.

---

## Usage

There are two ways to use the library. Both boot a per-tab DB worker, join the `SharedWorker`
multiplexer, and run leader election so exactly **one** tab owns the single OPFS connection;
every call is routed to that leader.

### Option A — generic: run SQL from the main thread

```ts
import { init, dbExec } from "@your-scope/sqlite-webworker";

await init({ dbName: "my-database", userPk: userPrimaryKey });

await dbExec({ sql: "CREATE TABLE IF NOT EXISTS todo (id INTEGER PRIMARY KEY, text TEXT)" });
const rows = await dbExec({
  sql: "SELECT id, text FROM todo",
  rowMode: "array",
  returnValue: "resultRows",
});
```

### Option B — own your SQL in a worker (no SQL on the main thread)

Build your **own** DB worker with `defineDbWorker`. Put `CREATE TABLE` in `setup` and your
queries in `methods`; they execute in the elected leader worker, off the main thread.

```ts
// todos.worker.ts
import { defineDbWorker, dbExec, pushToTabs } from "@your-scope/sqlite-webworker/db-worker";

const listAll = (): [number, string][] =>
  dbExec({
    sql: "SELECT id, text FROM todo ORDER BY id",
    rowMode: "array",
    returnValue: "resultRows",
  }) as [number, string][];

defineDbWorker({
  setup() {
    dbExec({ sql: "CREATE TABLE IF NOT EXISTS todo (id INTEGER PRIMARY KEY, text TEXT)" });
  },
  methods: {
    async addTodo(text: string): Promise<void> {
      dbExec({ sql: "INSERT INTO todo (text) VALUES (?)", bind: [text] });
      pushToTabs("todos", listAll()); // fan a snapshot out to all tabs
    },
    async listTodos(): Promise<[number, string][]> {
      return listAll();
    },
  },
});

export interface TodoApi {
  addTodo(text: string): Promise<void>;
  listTodos(): Promise<[number, string][]>;
}
```

```ts
// main thread — no SQL, just a typed api
import { createDb } from "@your-scope/sqlite-webworker";
import type { TodoApi } from "./todos.worker";

const { ready, api, subscribe } = createDb<TodoApi>({
  dbName: "my-database",
  userPk: userPrimaryKey,
  dbWorker: () => new Worker(new URL("./todos.worker.ts", import.meta.url), { type: "module" }),
});

await ready;
subscribe<[number, string][]>("todos", renderTodos); // live updates from any tab
await api.addTodo("buy milk");
```

The two `/db-worker` and `/db-core` subpath exports exist so your bundler can build that worker.
See `demo/counters.worker.ts` + `demo/counters.ts` for a complete working example.

## Requirements

- A browser with `SharedWorker`, `Worker` (module workers), `BroadcastChannel`, `Web Locks`,
  and OPFS support.
- The page must be served over a [secure context](https://developer.mozilla.org/en-US/docs/Web/Security/Secure_Contexts)
  (HTTPS or `localhost`).
- OPFS via `SAHPool` needs these COOP/COEP headers on your document:
  ```
  Cross-Origin-Opener-Policy: same-origin
  Cross-Origin-Embedder-Policy: require-corp
  ```

## Local development / demo

The repo ships a demo app under `demo/`:

```sh
npm run dev        # serve the demo
npm run build      # build the library into dist/
npm run build:demo # build the demo into demo-dist/
```
