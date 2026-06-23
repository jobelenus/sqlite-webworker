// Builds a dedicated DB worker that the lib's multiplexer can elect as leader.
// It wires the Web-Locks leader-election plumbing + the SQLite core, merges in
// your app-specific SQL methods, and exposes the result over Comlink.
//
// One of these runs per tab, but only the elected leader holds the live OPFS
// connection; the multiplexer routes every call to that leader. Put your
// CREATE TABLE in `setup` and your queries in `methods` — all SQL lives here,
// off the main thread.
import * as Comlink from "comlink";
import {
  initializeSQLite,
  closeDatabase,
  dbExec,
  dbRead,
  dbWrite,
  createLock,
  pushToTabs,
} from "./db-core";
import { FORCE_LEADER_ELECTION, WORKER_LOCK_KEY } from "./consts";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type BroadcastMessage = Record<string, any>;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type AppMethods = Record<string, (...args: any[]) => Promise<any>>;

export interface DbWorkerOptions<M extends AppMethods> {
  // Runs inside the leader after sqlite boots — create tables / set pragmas
  // here. Called every time this worker becomes leader; use
  // `CREATE TABLE IF NOT EXISTS`.
  setup?: () => void | Promise<void>;
  // Your app's SQL methods, exposed to the main thread (routed to the leader).
  methods?: M;
}

// The plumbing every DB worker must expose for the multiplexer to drive it.
export interface BaseWorkerInterface {
  dbExec: typeof dbExec;
  dbRead: typeof dbRead;
  dbWrite: typeof dbWrite;
  createLock: typeof createLock;
  getWorkerId(): Promise<string | undefined>;
  hasDbLock(): Promise<boolean>;
  initialize(
    dbName: string,
    gotLockPort: MessagePort,
    userPk: string,
    workerId: string,
  ): Promise<unknown>;
  receiveBroadcast(message: BroadcastMessage): Promise<void>;
  notifyTabs(channelName: string, message: unknown): Promise<void>;
}

export type DbWorker<M extends AppMethods> = BaseWorkerInterface & M;

// Re-export so app methods (inside a worker) can run queries / fan out updates.
export { dbExec, dbRead, dbWrite, createLock, pushToTabs };

export function defineDbWorker<M extends AppMethods>(
  opts: DbWorkerOptions<M> = {},
): void {
  let abortController: AbortController | undefined;
  let hasTheLock = false;
  let workerId: string | undefined;

  const forceLeaderElectionBroadcastChannel = new BroadcastChannel(
    FORCE_LEADER_ELECTION,
  );

  const base: BaseWorkerInterface = {
    dbExec,
    dbRead,
    dbWrite,
    createLock,
    async getWorkerId() {
      return workerId;
    },
    async hasDbLock() {
      return hasTheLock;
    },
    async initialize(
      dbName: string,
      gotLockPort: MessagePort,
      _userPk: string,
      _workerId: string,
    ) {
      workerId = _workerId;
      if (abortController) {
        abortController.abort();
      }

      abortController = new AbortController();
      return await navigator.locks.request(
        WORKER_LOCK_KEY,
        { mode: "exclusive", signal: abortController.signal },
        async () => {
          hasTheLock = true;
          await initializeSQLite(dbName);
          // Consumer schema/pragmas, run in the leader once it owns the DB.
          await opts.setup?.();

          gotLockPort.postMessage("lock acquired");
          return new Promise((resolve) => {
            forceLeaderElectionBroadcastChannel.onmessage = () => {
              abortController?.abort(FORCE_LEADER_ELECTION);
            };
            abortController?.signal.addEventListener("abort", () => {
              hasTheLock = false;
              closeDatabase();
              resolve(abortController?.signal.reason);
            });
          });
        },
      );
    },
    async receiveBroadcast(_message: BroadcastMessage) {},
    async notifyTabs(channelName: string, message: unknown) {
      pushToTabs(channelName, message);
    },
  };

  const iface = { ...base, ...(opts.methods ?? {}) } as DbWorker<M>;
  Comlink.expose(iface);
}
