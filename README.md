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
  sqlite-webworker.js        # the entry you import
  main.d.ts                  # types
  assets/
    webworker-*.js           # per-tab DB worker      (self-contained)
    shared_webworker-*.js    # cross-tab SharedWorker  (self-contained)
    sqlite3-*.js             # sqlite-wasm runtime
```

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

```ts
import { init, workerId } from "@your-scope/sqlite-webworker";

await init(
  "my-database", // database name (OPFS-backed)
  userPrimaryKey, // your app's user identifier
  workerId, // the per-tab id exported by this library
);
```

`init()` boots the per-tab worker, joins the `SharedWorker` multiplexer, and participates in
leader election so exactly one tab owns the database connection. It is safe to call once per
tab; repeated calls are ignored.

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
