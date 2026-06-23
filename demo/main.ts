import "./style.css";
import {
  initCounters,
  readCounters,
  increment,
  subscribeCounters,
  type CounterRow,
} from "./counters";

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
            `<tr><td>${escapeHtml(slug)}</td><td>${escapeHtml(count)}</td></tr>`,
        )
        .join("")
    : `<tr><td colspan="2"><em>no counters yet</em></td></tr>`;
};

const showError = (err: unknown) => {
  console.error(err);
  const msg = String(err instanceof Error ? err.message : err);
  statusEl.innerHTML = `<strong style="color:#c00">error:</strong> ${escapeHtml(msg)}`;
};

async function boot() {
  await initCounters();
  subscribeCounters(render);
  render(await readCounters());
  statusEl.textContent = "ready";
}

const ready = boot().catch(showError);

// Attach synchronously so the page never does a default form GET reload, even
// while the DB is still booting.
form.addEventListener("submit", (event) => {
  event.preventDefault();
  const slug = new FormData(form).get("slug")?.toString().trim();
  if (!slug) return;

  void ready.then(() => increment(slug)).catch(showError);
});
