import * as Comlink from "comlink";
import { monotonicFactory } from "ulid";
import type { SharedInterface } from "./shared_webworker";
import type { WorkerInterface } from "./webworker";
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

const tabWorker = new Worker(new URL("./webworker.ts", import.meta.url), {
  type: "module",
  name: `db-${__LIB_VERSION__}`,
});
const tabDb: Comlink.Remote<WorkerInterface> = Comlink.wrap(tabWorker);

const onSharedWorkerBootBroadcastChannel = new BroadcastChannel(
  SHARED_BROADCAST_CHANNEL_NAME,
);

onSharedWorkerBootBroadcastChannel.onmessage = async (msg) => {
  const name = msg.data as string;
  if (name !== sharedWebWorkerName) {
    // This will ensure that the new shared worker is the one we use to
    // communicate with the various remotes if a new version of the shared
    // webworker code is detected. But, note that if the interface changes, this
    // tab will still have to be reloaded for that communication to work.

    // eslint-disable-next-line no-console
    db.unregisterRemote(workerId);
    sharedWorker = spawnSharedWorker(name);
    sharedWebWorkerName = name;
    db = Comlink.wrap(sharedWorker.port);
    db.registerRemote(workerId, Comlink.proxy(tabDb));
    // showCachedAppNotification();
  } else {
    db.registerRemote(workerId, Comlink.proxy(tabDb));
  }

  await detectLockAcquiredByOtherTab();
};

const detectLockAcquiredByOtherTab = async () => {
  if (!(await tabDb.hasDbLock()) && !lockAcquired) {
    const currentLeaderId = await db.currentLeaderId();
    if (currentLeaderId) {
      // eslint-disable-next-line no-console
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
  tabDb
    .initialize(dbName, Comlink.proxy(messagePort), userPk, workerId)
    .then((result) => {
      if (typeof result === "string" && result === FORCE_LEADER_ELECTION) {
        initialize(dbName, messagePort, userPk, workerId);
      }
    });
};

let ranInit = false;
export const init = async (
  dbName: string,
  userPk: string,
  workerId: string,
) => {
  if (!ranInit) {
    const { port1, port2 } = new MessageChannel();
    // This message fires when the lock has been acquired for this tab
    port1.onmessage = () => {
      // Ensure we're registered. Register will set the remote!
      db.registerRemote(workerId, Comlink.proxy(tabDb));
      // eslint-disable-next-line no-console
      lockAcquired = true;
      lockAcquiredBroadcastChannel.postMessage(workerId);
    };

    initialize(dbName, port2, userPk, workerId);

    ranInit = true;
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

// Ask the leader worker to push `message` to every tab on this origin. Pair with
// `subscribe` in each tab's main thread to react. The leader worker performs the
// post, so all main threads receive it (including the tab that called this).
export const notifyTabs = (channelName: string, message: unknown) =>
  db.notifyTabs(channelName, message);

// Listen for messages pushed via `notifyTabs`. Returns an unsubscribe function.
export const subscribe = <T = unknown>(
  channelName: string,
  callback: (message: T) => void,
): (() => void) => {
  const channel = new BroadcastChannel(channelName);
  channel.onmessage = (event: MessageEvent) => callback(event.data as T);
  return () => channel.close();
};
