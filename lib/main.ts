import * as Comlink from "comlink";
import { monotonicFactory } from "ulid";
import type { SharedInterface } from "./shared_webworker";
import type { BaseWorkerInterface } from "./db-worker";
import {
  DB_LOCK_ACQUIRED,
  FORCE_LEADER_ELECTION,
  SHARED_BROADCAST_CHANNEL_NAME,
} from "./consts";

const ulid = monotonicFactory(() => Math.random());
export const workerId = ulid();

// Shared workers are unique per *name*, not per code URL. The name is suffixed
// with the library version so a new build elects a fresh shared worker and tabs
// migrate to it (see the boot broadcast handler below). The script URL is a
// literal `new URL(..., import.meta.url)` so the bundler emits the worker as a
// separate asset.
const spawnSharedWorker = (name: string) =>
  new SharedWorker(new URL("./shared_webworker.ts", import.meta.url), {
    type: "module",
    name,
  });

let sharedWebWorkerName = `multiplexer-${__LIB_VERSION__}`;
let sharedWorker = spawnSharedWorker(sharedWebWorkerName);
let db: Comlink.Remote<SharedInterface> = Comlink.wrap(sharedWorker.port);
let lockAcquired = false;

// The dedicated DB worker (the thing that owns the OPFS connection when elected
// leader). Constructed in `init` from the factory the consumer supplies. Build
// it with `defineDbWorker` (the "@scope/sqlite-webworker/db-worker" export) so
// your SQL runs in the worker.
let tabWorker: Worker | undefined;
let tabDb: Comlink.Remote<BaseWorkerInterface> | undefined;

const onSharedWorkerBootBroadcastChannel = new BroadcastChannel(
  SHARED_BROADCAST_CHANNEL_NAME,
);

onSharedWorkerBootBroadcastChannel.onmessage = async (msg) => {
  if (!tabDb) return;
  const name = msg.data as string;
  if (name !== sharedWebWorkerName) {
    // This will ensure that the new shared worker is the one we use to
    // communicate with the various remotes if a new version of the shared
    // webworker code is detected. But, note that if the interface changes, this
    // tab will still have to be reloaded for that communication to work.
    db.unregisterRemote(workerId);
    sharedWorker = spawnSharedWorker(name);
    sharedWebWorkerName = name;
    db = Comlink.wrap(sharedWorker.port);
    db.registerRemote(workerId, Comlink.proxy(tabDb));
  } else {
    db.registerRemote(workerId, Comlink.proxy(tabDb));
  }

  await detectLockAcquiredByOtherTab();
};

const detectLockAcquiredByOtherTab = async () => {
  if (!tabDb) return;
  if (!(await tabDb.hasDbLock()) && !lockAcquired) {
    const currentLeaderId = await db.currentLeaderId();
    if (currentLeaderId) {
      lockAcquired = true;
    }
  }
};

const forceLeaderElectionBroadcastChannel = new BroadcastChannel(
  FORCE_LEADER_ELECTION,
);

window.onbeforeunload = () => {
  db.unregisterRemote(workerId);
};

const initialize = (
  dbName: string,
  messagePort: MessagePort,
  userPk: string,
  workerId: string,
) => {
  tabDb!
    .initialize(dbName, Comlink.proxy(messagePort), userPk, workerId)
    .then((result) => {
      if (typeof result === "string" && result === FORCE_LEADER_ELECTION) {
        initialize(dbName, messagePort, userPk, workerId);
      }
    });
};

export interface InitOptions {
  // The OPFS-backed database name.
  dbName: string;
  // Your app's user identifier.
  userPk: string;
  // Factory for the dedicated DB worker, built with `defineDbWorker`. One is
  // created per tab; the multiplexer elects a single leader that owns the OPFS
  // connection.
  dbWorker: () => Worker;
}

let ranInit = false;
export const init = async (opts: InitOptions): Promise<void> => {
  if (!ranInit) {
    ranInit = true;

    tabWorker = opts.dbWorker();
    tabDb = Comlink.wrap(tabWorker);

    const { port1, port2 } = new MessageChannel();
    // This message fires when the lock has been acquired for this tab
    port1.onmessage = () => {
      // Ensure we're registered. Register will set the remote!
      db.registerRemote(workerId, Comlink.proxy(tabDb!));
      lockAcquired = true;
      lockAcquiredBroadcastChannel.postMessage(workerId);
    };

    initialize(opts.dbName, port2, opts.userPk, workerId);
  }

  // If both tabs are refreshed at the same time, this can falsely indicate that
  // a tab has the lock, but that tab has actually been refreshed just *after*
  // this call, so *we* now have the lock.  adding 0.2 second timeout here
  // ensures that there is enough time for the lock to be resolved in a multitab
  // scenario before we begin cold start. (This only matters if 2+ tabs are
  // refreshed at more or less the same time, in the normal scenario we will
  // indicate lock acquisition via the broadcast channel)
  //
  const p = new Promise<void>((resolve) => {
    setTimeout(async () => {
      if (!lockAcquired) {
        await detectLockAcquiredByOtherTab();
      }
      resolve();
    }, 200);
  });
  await p;

  setTimeout(async () => {
    // If after 2 seconds total, we have not detected lock acquisition, try
    // and force a leader election
    if (!lockAcquired) {
      await detectLockAcquiredByOtherTab();
      if (!lockAcquired) {
        forceLeaderElectionBroadcastChannel.postMessage(FORCE_LEADER_ELECTION);
      }
    }
  }, 2000);
};

const lockAcquiredBroadcastChannel = new BroadcastChannel(DB_LOCK_ACQUIRED);
lockAcquiredBroadcastChannel.onmessage = (message) => {
  const lockTabId = message.data;
  if (lockTabId !== workerId) {
    lockAcquired = true;
  }
};

// Run SQL against the database. Routed to whichever tab currently holds the DB
// leader lock. Pass `rowMode`/`returnValue` to get result rows back (e.g.
// `{ sql, rowMode: "array", returnValue: "resultRows" }`); omit them for writes.
type DbExecOptions = Parameters<SharedInterface["dbExec"]>[0];
export const dbExec = (
  opts: Omit<DbExecOptions, "rowMode" | "returnValue"> &
    Partial<Pick<DbExecOptions, "rowMode" | "returnValue">>,
) => db.dbExec(opts as DbExecOptions);

// Call an app method defined in your custom DB worker. Routed to the current
// leader. Prefer the typed `api` proxy from `createDb` over calling this
// directly.
export const callLeader = <R = unknown>(
  method: string,
  ...args: unknown[]
): Promise<R> => db.callLeader(method, args) as Promise<R>;

// A typed proxy over `callLeader`: every property becomes a function whose call
// is forwarded to the same-named method on the leader DB worker. `T` is your
// worker's app-methods interface.
export function createApi<T extends object>(): Comlink.Remote<T> {
  return new Proxy(
    {},
    {
      get(_target, prop) {
        if (typeof prop !== "string") return undefined;
        return (...args: unknown[]) => db.callLeader(prop, args);
      },
    },
  ) as Comlink.Remote<T>;
}

// Ask the leader worker to push `message` to every tab on this origin. Pair with
// `subscribe` in each tab's main thread to react. The leader worker performs the
// post, so all main threads receive it (including the tab that called this).
export const notifyTabs = (channelName: string, message: unknown) =>
  db.notifyTabs(channelName, message);

// Listen for messages pushed via `notifyTabs` / `pushToTabs`. Returns an
// unsubscribe function.
export const subscribe = <T = unknown>(
  channelName: string,
  callback: (message: T) => void,
): (() => void) => {
  const channel = new BroadcastChannel(channelName);
  channel.onmessage = (event: MessageEvent) => callback(event.data as T);
  return () => channel.close();
};

// One-call setup tying everything together: boots the lib with your custom DB
// worker and hands back a typed `api` to call its methods. `TApi` is your
// worker's app-methods interface (export it from your worker module).
export interface CreateDbResult<TApi extends object> {
  ready: Promise<void>;
  api: Comlink.Remote<TApi>;
  workerId: string;
  notifyTabs: typeof notifyTabs;
  subscribe: typeof subscribe;
}

export function createDb<TApi extends object>(opts: {
  dbName: string;
  userPk: string;
  dbWorker: () => Worker;
}): CreateDbResult<TApi> {
  const ready = init({
    dbName: opts.dbName,
    userPk: opts.userPk,
    dbWorker: opts.dbWorker,
  });
  return {
    ready,
    api: createApi<TApi>(),
    workerId,
    notifyTabs,
    subscribe,
  };
}
