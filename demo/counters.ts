// Data layer for the demo. This module owns the lib (init + the single DB
// leader) and *every* SQL statement. main.ts never sees SQL; it only calls the
// high-level functions exported here.
//
// NOTE: this runs on the main thread, not in a SharedWorker. The library spawns
// its own SharedWorker + dedicated Worker and uses `window`, both of which only
// exist on the main thread, so the lib cannot be hosted inside another worker.
// This module is the equivalent "DB service" boundary for the app code.
import { init, dbExec, notifyTabs, subscribe, workerId } from "../lib/main";

// BroadcastChannel name the leader worker uses to push fresh snapshots to every
// tab. Not a SQL identifier — just a channel label.
const CHANNEL = "demo_counter";

// A row as the DB returns it: [slug, count].
export type CounterRow = [slug: string, count: number];

// Boot the lib (per-tab worker + cross-tab leader election) and ensure the
// table exists. Safe to call once on startup.
export async function initCounters(): Promise<void> {
  await init("sqlite-webworker-demo", "demo-user", workerId);

  await dbExec({
    sql: `CREATE TABLE IF NOT EXISTS demo_counter (
            slug TEXT PRIMARY KEY,
            count INTEGER NOT NULL DEFAULT 0
          )`,
  });
}

// Read the full table, ordered by slug.
export async function readCounters(): Promise<CounterRow[]> {
  return (await dbExec({
    sql: "SELECT slug, count FROM demo_counter ORDER BY slug",
    rowMode: "array",
    returnValue: "resultRows",
  })) as CounterRow[];
}

// Increment (or insert) the counter for `slug`, then push the updated table to
// every tab via the leader worker (this tab included).
export async function increment(slug: string): Promise<void> {
  await dbExec({
    sql: `INSERT INTO demo_counter (slug, count) VALUES (?, 1)
          ON CONFLICT(slug) DO UPDATE SET count = count + 1`,
    bind: [slug],
  });

  await notifyTabs(CHANNEL, await readCounters());
}

// Subscribe to cross-tab snapshots pushed by `increment`. Returns an
// unsubscribe function.
export function subscribeCounters(
  callback: (rows: CounterRow[]) => void,
): () => void {
  return subscribe<CounterRow[]>(CHANNEL, callback);
}
