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

// Shared workers are unique per *name*, not per code URL.
const spawnSharedWorker = (url: string, name: string) =>
  new SharedWorker(new URL(url, import.meta.url), {
    type: "module",
    name,
  });

const __SHARED_WEB_WORKER_URL__ = import.meta.env.SHARED_WEB_WORKER_URL;
const __SHARED_WORKER_HASH__ = import.meta.env.SHARED_WORKER_HASH;
const __WEB_WORKER_URL__ = import.meta.env.WEB_WORKER_URL;
const __WEB_WORKER_HASH__ = import.meta.env.WEB_WORKER_HASH;

let sharedWebWorkerName = `multiplexer-${__SHARED_WORKER_HASH__}`;
let sharedWorker = spawnSharedWorker(
  __SHARED_WEB_WORKER_URL__,
  sharedWebWorkerName,
);
let db: Comlink.Remote<SharedInterface> = Comlink.wrap(sharedWorker.port);
let lockAcquired = false;

const tabWorker = new Worker(new URL(__WEB_WORKER_URL__, import.meta.url), {
  type: "module",
  name: `db-${__WEB_WORKER_HASH__}`,
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
    sharedWorker = spawnSharedWorker(__WEB_WORKER_URL__, name);
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
