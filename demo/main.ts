import "./style.css";
import { init, dbExec, notifyTabs, subscribe, workerId } from "../lib/main";

// Channel name used to push counter updates from the leader worker to every tab.
const CHANNEL = "sqlite_demo_counter";

type CounterRow = [slug: string, count: number];

const app = document.querySelector<HTMLDivElement>("#app")!;
app.innerHTML = `
  <h1>sqlite-webworker demo</h1>
  <p>
    Open this page in multiple tabs. Incrementing a slug in one tab updates the
    table in every tab &mdash; the leader worker pushes the change to all of them.
  </p>
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

const escapeHtml = (value: string) =>
  value.replace(
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
    sql: "SELECT slug, count FROM sqlite_demo_counter ORDER BY slug",
    rowMode: "array",
    returnValue: "resultRows",
  })) as CounterRow[];

async function main() {
  // Boot the per-tab worker and join the cross-tab DB leader election.
  await init("sqlite-webworker-demo", "demo-user", workerId);

  await dbExec({
    sql: `CREATE TABLE IF NOT EXISTS sqlite_demo_counter (
            slug TEXT PRIMARY KEY,
            count INTEGER NOT NULL DEFAULT 0
          )`,
  });

  // Re-render whenever the leader worker pushes a fresh snapshot to this tab.
  subscribe<CounterRow[]>(CHANNEL, render);

  // Initial paint from whatever is already persisted in OPFS.
  render(await readAllRows());

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const slug = new FormData(form).get("slug")?.toString().trim();
    if (!slug) return;

    await dbExec({
      sql: `INSERT INTO sqlite_demo_counter (slug, count) VALUES (?, 1)
            ON CONFLICT(slug) DO UPDATE SET count = count + 1`,
      bind: [slug],
    });

    // Leader worker pushes the updated table to every tab's main thread
    // (this tab included), which triggers the `subscribe` listener above.
    await notifyTabs(CHANNEL, await readAllRows());

    form.reset();
    form.querySelector<HTMLInputElement>("#slug")!.focus();
  });
}

void main();
