import * as Comlink from "comlink";
import {
  type ExecBaseOptions,
  type ExecReturnResultRowsOptions,
  type ExecRowModeArrayOptions,
  type FlexibleString,
  type SqlValue,
} from "@sqlite.org/sqlite-wasm";
import {
  DB_NOT_INIT_ERR,
  SHARED_BROADCAST_CHANNEL_NAME,
  type BroadcastMessage,
  type WorkerInterface,
} from "./webworker";

// Wait 5 seconds after we no longer have any remotes before terminating ourselves
const SHUTDOWN_DELAY_MS = 5000;

declare global {
  interface Window {
    onconnect?: (event: MessageEvent) => void;
  }
}

const onConnectBroadcast = new BroadcastChannel(SHARED_BROADCAST_CHANNEL_NAME);

const name = self.name;

const shutDownWebWorker = () => self.close();

self.onconnect = (event: MessageEvent) => {
  Comlink.expose(sharedInterface, event.ports[0]);
  onConnectBroadcast.postMessage(name);
};

const _DEBUG = parseInt(import.meta.env.DEBUG) ?? false;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function debug(...args: any | any[]) {
  // eslint-disable-next-line no-console
  if (_DEBUG) console.debug(...args);
}

let currentLeader: Comlink.Remote<WorkerInterface> | undefined;
let currentLeaderId: string | undefined;
const remotes: { [key: string]: Comlink.Remote<WorkerInterface> } = {};

const hasLeaderChannel = new MessageChannel();
const leaderChangedChannel = new MessageChannel();

const LEADER_CHANGED = "LEADER_CHANGED";

const failOnLeaderChange = (): Promise<never> => {
  return new Promise((_, reject) => {
    const onMessage = () => {
      // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors
      reject(LEADER_CHANGED);
    };
    leaderChangedChannel.port1.addEventListener("message", onMessage, {
      capture: false,
      passive: true,
      once: true,
    });
    leaderChangedChannel.port1.start();
  });
};

const getLeader = (): Promise<Comlink.Remote<WorkerInterface>> => {
  return new Promise((resolve, reject) => {
    if (currentLeader) {
      resolve(currentLeader);
    }
    hasLeaderChannel.port1.onmessage = () => {
      if (currentLeader) {
        resolve(currentLeader);
      } else {
        reject(new Error("Got remote message but no remote set"));
      }
    };
  });
};

const MAX_RETRIES = 250;

const sleep = (ms: number) =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

async function withLeader<R>(
  cb: (remote: Comlink.Remote<WorkerInterface>) => Promise<R>,
  retry?: number,
): Promise<R> {
  const remote = await getLeader();
  const retries = retry ?? 0;

  if (retries >= MAX_RETRIES) {
    throw new Error(
      "Retries exceeded attempting to perform query against database leader. Please refresh this tab.",
    );
  }

  // If the leader with the remote changes while a call is in progress, we need
  // to retry the call, now with the new remote. Otherwise, we will likely hang
  // forever.
  try {
    const result = await Promise.race([cb(remote), failOnLeaderChange()]);
    return result;
  } catch (err) {
    if (typeof err === "string" && err === LEADER_CHANGED) {
      debug("LEADER CHANGED MID REQUEST, rerunning callback", retries);
      return withLeader(cb, retries + 1);
    }
    if (err instanceof Error && err.message === DB_NOT_INIT_ERR) {
      debug("DB NOT INITIALIZED?", retries);
      await sleep(100);
      return withLeader(cb, retries + 1);
    }

    throw err;
  }
}

export type LeaderCallBackFn = (
  currentLeader: Comlink.Remote<WorkerInterface>,
) => void;
let leaderCallback: LeaderCallBackFn;

const sharedInterface = {
  setElectionCallbackFn(fn: LeaderCallBackFn) {
    leaderCallback = fn;
  },

  async broadcastMessage<B extends BroadcastMessage>(message: B) {
    Object.keys(remotes).forEach((remoteId) => {
      if (remoteId !== currentLeaderId) {
        try {
          remotes[remoteId]?.receiveBroadcast(message);
        } catch (err) {
          debug("failed to send to remote", remoteId, err);
        }
      }
    });
  },

  unregisterRemote(id: string) {
    debug("unregister remote in shared", id);
    if (currentLeaderId === id) {
      debug("tab with lock unregistered. no remote set");
      currentLeader = undefined;
    }
    delete remotes[id];

    if (Object.keys(remotes).length === 0) {
      // Just in case there is a race between closing a tab and a new one
      // loading, and this worker gets a new remote, don't shut down right away.
      // Double check after a few seconds.
      setTimeout(() => {
        if (Object.keys(remotes).length === 0) {
          shutDownWebWorker();
        }
      }, SHUTDOWN_DELAY_MS);
    }
  },
  async registerRemote(id: string, remote: Comlink.Remote<WorkerInterface>) {
    if (!remotes[id]) {
      debug("register remote in shared", id);
      remotes[id] = remote;
    }
    if (await remote.hasDbLock()) {
      await this.setLeader(id);
    }
  },

  async hasLeader() {
    return !!currentLeader;
  },

  async currentLeaderId() {
    return currentLeaderId;
  },

  async setLeader(remoteId: string) {
    debug("setting remote in shared web worker to", remoteId);

    currentLeader = remotes[remoteId];
    if (!currentLeader) {
      throw new Error(`remote ${remoteId} not registered`);
    }
    const leaderChanged = currentLeaderId !== remoteId;
    currentLeaderId = remoteId;

    if (leaderCallback) leaderCallback(currentLeader);

    hasLeaderChannel.port2.postMessage("got leader");
    if (leaderChanged) {
      debug("follow the leader");
      leaderChangedChannel.port2.postMessage("leader changed");
    }
  },

  async dbExec(
    opts: ExecBaseOptions &
      ExecRowModeArrayOptions &
      ExecReturnResultRowsOptions & {
        sql: FlexibleString;
      },
  ): Promise<SqlValue[][]> {
    return await withLeader(async (remote) => await remote.dbExec(opts));
  },
};

export const SharedInterface = typeof sharedInterface;
