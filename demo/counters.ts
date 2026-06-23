// Thin main-thread data layer. No SQL here — it boots the lib with the demo's
// custom DB worker and exposes a typed `api`. All SQL lives in counters.worker.ts.
import { createDb } from "../lib/main";
import type { CounterApi, CounterRow } from "./counters.worker";

export type { CounterRow };

// Channel the worker pushes snapshots on; subscribe to it for live updates.
const CHANNEL = "demo_counter";

const { ready, api, subscribe } = createDb<CounterApi>({
  dbName: "demo",
  userPk: "demo-user",
  dbWorker: () =>
    new Worker(new URL("./counters.worker.ts", import.meta.url), {
      type: "module",
    }),
});

// Boot the lib + cross-tab leader election. The table is created in the worker's
// `setup`, so there's nothing SQL to do here.
export async function initCounters(): Promise<void> {
  await ready;
}

// Read the full table (routed to the leader worker).
export const readCounters = (): Promise<CounterRow[]> => api.readCounters();

// Increment a slug (routed to the leader, which writes and fans out the update).
export const increment = (slug: string): Promise<void> => api.increment(slug);

// Subscribe to cross-tab snapshots the worker pushes after each write.
export function subscribeCounters(
  callback: (rows: CounterRow[]) => void,
): () => void {
  return subscribe<CounterRow[]>(CHANNEL, callback);
}
