import "./style.css";
import { init, dbExec, notifyTabs, subscribe, workerId } from "../lib/main";

// Channel name used to push counter updates from the leader worker to every tab.
const CHANNEL = "demo_counter";

type CounterRow = [slug: string, count: number];

const app = document.querySelector<HTMLDivElement>("#app")!;
app.innerHTML = `
  <h1>sqlite-webworker demo</h1>
  <p>
    Open this page in multiple tabs. Incrementing a slug in one tab updates the
    table in every tab &mdash; the leader worker pushes the change to all of them.
  </p>
  <p id="status"><em>booting&hellip;</em></p>
  <form id="counter-form">
    <input id="slug" name="slug" placeholder="slug" autocomplete="off" required />
    <button type="submit">Increment</button>
  </form>
  <table>
    <thead>
      <tr><th>slug</th><th>count</th></tr>
    </thead>
    <tbody id="rows"></tbody>
  </table>
`;

const tbody = document.querySelector<HTMLTableSectionElement>("#rows")!;
const form = document.querySelector<HTMLFormElement>("#counter-form")!;
const statusEl = document.querySelector<HTMLParagraphElement>("#status")!;

const setStatus = (msg: string) => {
  console.log("[demo]", msg);
  statusEl.innerHTML = msg;
};

const showError = (err: unknown) => {
  console.error(err);
  const msg = String(err instanceof Error ? err.message : err);
  statusEl.innerHTML = `<strong style="color:#c00">error:</strong> ${escapeHtml(msg)}`;
  tbody.innerHTML = `<tr><td colspan="2"><em>error: ${escapeHtml(msg)}</em></td></tr>`;
};

// Reject if a promise hasn't settled in `ms` — turns a silent leader-election
// hang into a visible error instead of a blank page.
const withTimeout = <T>(label: string, ms: number, p: Promise<T>): Promise<T> =>
  Promise.race([
    p,
    new Promise<T>((_, reject) =>
      setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms),
    ),
  ]);

const escapeHtml = (value: unknown) =>
  String(value).replace(
    /[&<>"']/g,
    (c) =>
      ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;",
      })[c]!,
  );

const render = (rows: CounterRow[]) => {
  tbody.innerHTML = rows.length
    ? rows
        .map(
          ([slug, count]) =>
            `<tr><td>${escapeHtml(slug)}</td><td>${count}</td></tr>`,
        )
        .join("")
    : `<tr><td colspan="2"><em>no counters yet</em></td></tr>`;
};

const readAllRows = async (): Promise<CounterRow[]> =>
  (await dbExec({
    sql: "SELECT slug, count FROM demo_counter ORDER BY slug",
    rowMode: "array",
    returnValue: "resultRows",
  })) as CounterRow[];

// Boots the worker, creates the table, and does the initial paint. The submit
// handler awaits this so the DB is guaranteed ready before any write.
async function boot() {
  // Boot the per-tab worker and join the cross-tab DB leader election.
  setStatus("<em>init: electing DB leader&hellip;</em>");
  await init("sqlite-webworker-demo", "demo-user", workerId);

  setStatus("<em>creating table&hellip;</em>");
  await withTimeout(
    "CREATE TABLE",
    8000,
    dbExec({
      sql: `CREATE TABLE IF NOT EXISTS demo_counter (
              slug TEXT PRIMARY KEY,
              count INTEGER NOT NULL DEFAULT 0
            )`,
    }),
  );

  // Re-render whenever the leader worker pushes a fresh snapshot to this tab.
  subscribe<CounterRow[]>(CHANNEL, render);

  // Initial paint from whatever is already persisted in OPFS.
  setStatus("<em>reading rows&hellip;</em>");
  render(await withTimeout("initial SELECT", 8000, readAllRows()));
  setStatus("ready");
}

const ready = boot().catch(showError);

// Attach the submit handler synchronously so the page never does a default
// form GET reload, even while the DB is still booting.
form.addEventListener("submit", (event) => {
  event.preventDefault();
  const slug = new FormData(form).get("slug")?.toString().trim();
  if (!slug) return;

  void ready
    .then(async () => {
      await dbExec({
        sql: `INSERT INTO demo_counter (slug, count) VALUES (?, 1)
              ON CONFLICT(slug) DO UPDATE SET count = count + 1`,
        bind: [slug],
      });

      // Leader worker pushes the updated table to every tab's main thread
      // (this tab included), which triggers the `subscribe` listener above.
      await notifyTabs(CHANNEL, await readAllRows());
    })
    .catch(showError);
});
