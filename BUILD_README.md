# Releasing a new version

Steps to cut and publish a new version of `@your-scope/sqlite-webworker` to npm.

## How the build is wired (read once)

- **`npm run build`** = `vite build && tsc -p tsconfig.build.json`
  - `vite build` → library bundle + worker assets into `dist/` (see `vite.config.ts`).
  - `tsc -p tsconfig.build.json` → `.d.ts` type declarations into `dist/`.
- **`prepublishOnly`** runs `npm run build` automatically on `npm publish`, so the
  published `dist/` is always freshly built — you cannot publish a stale build.
- **`files: ["dist"]`** in `package.json` → only `dist/` is published. `lib/`, `demo/`,
  configs, and source are excluded.
- `dist/` is **git-ignored** and never committed; it is rebuilt on every publish.

### Why the version bump matters here

The package version is injected at build time as `__LIB_VERSION__` (via Vite `define`)
and used to **name the workers**: `multiplexer-<version>` and `db-<version>`. A new version
changes those names, which is how open tabs detect a new release and migrate to the fresh
`SharedWorker`. **Always bump the version before building/publishing** — shipping new code
under the same version means open tabs keep talking to the old worker until fully closed.

---

## Release checklist

### 1. Pre-flight

```sh
git checkout main
git pull
git status            # working tree must be clean
npm install           # ensure deps match the lockfile
```

### 2. Typecheck

```sh
npm run typecheck
```

Fix anything red before continuing.

### 3. (Optional) Sanity-check the build and the demo

```sh
npm run build         # produces dist/
npm run dev           # open the printed localhost URL, exercise the demo in 2 tabs
```

### 4. Bump the version

Pick the right level (semver):

```sh
npm version patch     # bug fix:        0.0.1 -> 0.0.2
npm version minor     # new feature:    0.0.1 -> 0.1.0
npm version major     # breaking change:0.0.1 -> 1.0.0
```

`npm version`:
- edits `version` in `package.json`,
- creates a commit for that change,
- creates a git tag `v<new-version>`.

> Requires a clean working tree (step 1). If you keep a CHANGELOG, update it **before**
> this step so it lands in the same commit.

### 5. Inspect what will be published

```sh
npm publish --dry-run     # lists the exact files in the tarball
```

Confirm the tarball contains:

```
dist/sqlite-webworker.js
dist/main.d.ts
dist/assets/webworker-*.js
dist/assets/shared_webworker-*.js
dist/assets/sqlite3-*.js
```

(`npm pack` writes the tarball to disk if you want to unpack and look closer.)

### 6. Publish

This is a **scoped** package (`@your-scope/...`). Scoped packages publish **private by
default**, which fails without a paid plan — so you must pass `--access public`:

```sh
npm publish --access public
```

- `prepublishOnly` rebuilds `dist/` first — no need to run `npm run build` yourself.
- Make sure you are logged in (`npm whoami`; if not, `npm login`) and that you own / can
  publish under the `@your-scope` scope.
- After the **first** successful publish, the package keeps its public access, so later
  releases can use a plain `npm publish` — but keeping `--access public` is harmless.

### 7. Push the commit and tag

```sh
git push
git push --tags
```

### 8. Verify it landed

```sh
npm view @your-scope/sqlite-webworker version       # should show the new version
```

Optionally smoke-test the published artifact in a scratch dir:

```sh
cd $(mktemp -d)
npm init -y >/dev/null
npm install @your-scope/sqlite-webworker
ls node_modules/@your-scope/sqlite-webworker/dist
```

---

## Consumer reminder

After upgrading, consumers who do **not** use a Vite bundler must re-copy the worker assets,
because the hashed filenames change between versions:

```sh
cp -R node_modules/@your-scope/sqlite-webworker/dist/assets ./public/assets
```

See `README.md` ("Setup") for the copy step and the Vite-based alternative.

---

## Quick reference

```sh
git checkout main && git pull && git status   # clean tree
npm install
npm run typecheck
npm version patch|minor|major                 # bump + commit + tag
npm publish --dry-run                          # inspect tarball
npm publish --access public                    # scoped → must be public; rebuilds dist/
git push && git push --tags
```
