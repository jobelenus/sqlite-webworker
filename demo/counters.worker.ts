// The demo's dedicated DB worker. ALL SQL lives here — CREATE TABLE in `setup`,
// queries in `methods`. The lib's multiplexer elects one of these (across tabs)
// as leader; only the leader holds the OPFS connection, and every call is routed
// to it. App code on the main thread never sees SQL.
import { defineDbWorker, dbExec, pushToTabs } from "../lib/db-worker";

// BroadcastChannel the leader uses to push fresh snapshots to every tab.
const CHANNEL = "demo_counter";

export type CounterRow = [slug: string, count: number];

const readAll = (): CounterRow[] =>
  dbExec({
    sql: "SELECT slug, count FROM demo_counter ORDER BY slug",
    rowMode: "array",
    returnValue: "resultRows",
  }) as CounterRow[];

defineDbWorker({
  // Runs in the leader after sqlite boots.
  setup() {
    dbExec({
      sql: `CREATE TABLE IF NOT EXISTS demo_counter (
              slug TEXT PRIMARY KEY,
              count INTEGER NOT NULL DEFAULT 0
            )`,
    });
  },
  methods: {
    async readCounters(): Promise<CounterRow[]> {
      return readAll();
    },
    async increment(slug: string): Promise<void> {
      dbExec({
        sql: `INSERT INTO demo_counter (slug, count) VALUES (?, 1)
              ON CONFLICT(slug) DO UPDATE SET count = count + 1`,
        bind: [slug],
      });
      // Fan the updated table out to every tab (this one included).
      pushToTabs(CHANNEL, readAll());
    },
  },
});

// The shape the main thread calls through `createDb<CounterApi>()`.
export interface CounterApi {
  readCounters(): Promise<CounterRow[]>;
  increment(slug: string): Promise<void>;
}
