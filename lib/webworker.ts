import sqlite3InitModule, {
  Database,
  type ExecBaseOptions,
  type ExecReturnResultRowsOptions,
  type ExecRowModeArrayOptions,
  type FlexibleString,
  type SAHPoolUtil,
  type Sqlite3Static,
  type SqlValue,
} from "@sqlite.org/sqlite-wasm";

export const SHARED_BROADCAST_CHANNEL_NAME = "SHAREDWORKER_BROADCAST";
export const FORCE_LEADER_ELECTION = "FORCE_LEADER_ELECTION";
export const DB_NOT_INIT_ERR = "DB_NOT_INIT";
export const WORKER_LOCK_KEY = "WORKER_LOCK";
export const NOROW = Symbol("NOROW");

let abortController: AbortController | undefined;

const forceLeaderElectionBroadcastChannel = new BroadcastChannel(
  FORCE_LEADER_ELECTION,
);

const error = console.error;
const _DEBUG = parseInt(import.meta.env.DEBUG) ?? false;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function debug(...args: any | any[]) {
  // eslint-disable-next-line no-console
  if (_DEBUG) console.debug(...args);
}
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function log(...args: any | any[]) {
  // eslint-disable-next-line no-console
  if (_DEBUG) console.log(...args);
}

// When you just expect one column and one row
const oneInOne = (rows: SqlValue[][]): SqlValue | typeof NOROW => {
  const first = rows[0];
  if (first) {
    const id = first[0];
    if (id || id === 0) return id;
  }
  return NOROW;
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyFn = (...args: any[]) => any;
class ReadWriteLock {
  name: string;
  readerCount: number;
  writeLockAcquired: boolean;

  constructor(name: string) {
    this.name = name;
    this.readerCount = 0;
    this.writeLockAcquired = false;
  }

  isWriteLockAcquired(): boolean {
    return this.writeLockAcquired;
  }

  async readLock(callback: AnyFn): Promise<SqlValue[][]> {
    return await navigator.locks.request(
      `${this.name}-reader`,
      { mode: "shared" },
      async () => {
        this.readerCount++;
        try {
          return await callback();
        } finally {
          this.readerCount--;
        }
      },
    );
  }

  async writeLock(callback: AnyFn): Promise<SqlValue[][]> {
    try {
      return await navigator.locks.request(
        `${this.name}-reader`,
        { mode: "exclusive" },
        async () => {
          return await navigator.locks.request(
            `${this.name}-writer`,
            { mode: "exclusive" },
            async () => {
              this.writeLockAcquired = true;
              return await callback();
            },
          );
        },
      );
    } finally {
      this.writeLockAcquired = false;
    }
  }

  async query() {
    const state = await navigator.locks.query();
    const readerLocks = state.held?.filter(
      (lock) => lock.name === `${this.name}-reader`,
    );
    const writerLocks = state.held?.filter(
      (lock) => lock.name === `${this.name}-writer`,
    );

    return {
      readers: readerLocks?.length ?? 0,
      writers: writerLocks?.length ?? 0,
      pending:
        state.pending?.filter(
          (lock) =>
            lock.name === `${this.name}-reader` ||
            lock.name === `${this.name}-writer`,
        ).length ?? 0,
    };
  }
}

let sqlite: Database | undefined;
let poolUtil: SAHPoolUtil | undefined;
const start = async (
  sqlite3: Sqlite3Static,
  dbName: string,
  performIntegrityCheck = true,
) => {
  if ("opfs" in sqlite3) {
    if (!poolUtil) {
      poolUtil = await sqlite3.installOpfsSAHPoolVfs({});
    } else if (poolUtil.isPaused()) {
      await poolUtil.unpauseVfs();
    }

    sqlite = new poolUtil.OpfsSAHPoolDb(`/${dbName}`);
    debug(
      `OPFS is available, created persisted database in SAH Pool VFS at ${sqlite.filename}`,
    );
  } else {
    sqlite = new sqlite3.oo1.DB(`/${dbName}`, "c");
    debug(
      `OPFS is not available, created transient database ${sqlite.filename}`,
    );
  }

  // Adding an integrity check for a corrupted SQLite
  // db file. In the case it fails, delete the file
  // and re-start this process
  if (performIntegrityCheck) {
    const integrity = sqlite.exec({
      sql: "PRAGMA quick_check",
      returnValue: "resultRows",
    });
    const ok = oneInOne(integrity);
    if (ok !== "ok") {
      log(`Integrity: failed`);
      sqlite.close();
      poolUtil?.unlink(`/${dbName}`);
      await start(sqlite3, dbName);
      return;
    }
  }

  sqlite.exec({ sql: "PRAGMA foreign_keys = ON;" });
};

export const initializeSQLite = async (dbName: string) => {
  try {
    const sqlite3 = await sqlite3InitModule();
    await start(sqlite3, dbName);
  } catch (err) {
    if (err instanceof Error) {
      error("Initialization error:", err.name, err.message);
    } else error("Initialization error:", err);
  }
};

const locks: Record<string, ReadWriteLock> = {};
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const dbRead = async (name: string, opts: any) => {
  const lock = locks[name];
  if (!lock) throw new Error("DB Lock not acquired");
  if (!sqlite) throw new Error(DB_NOT_INIT_ERR);
  return await lock.readLock(() => {
    return sqlite.exec(opts);
  });
};
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const dbWrite = async (db: Database, name: string, opts: any) => {
  const lock = locks[name];
  if (!lock) throw new Error("DB Lock not acquired");
  return await lock.writeLock(() => {
    return db.exec(opts);
  });
};

export const createLock = (name: string) => {
  locks[name] = new ReadWriteLock(name);
};

// Raw execution without any locks, beware...
export const dbExec = (
  opts: ExecBaseOptions &
    ExecRowModeArrayOptions &
    ExecReturnResultRowsOptions & {
      sql: FlexibleString;
    },
): SqlValue[][] => {
  if (!sqlite) {
    throw new Error(DB_NOT_INIT_ERR);
  }
  try {
    return sqlite.exec(opts);
  } catch (err) {
    error(err);
    return [[-1]];
  }
};

export type BroadcastMessage = Record<string, any>;

let hasTheLock = false;
const workerInterface = {
  dbExec,
  createLock,
  dbRead,
  dbWrite,
  async hasDbLock(): Promise<boolean> {
    return hasTheLock;
  },
  async initialize(dbName: string, gotLockPort: MessagePort, _userPk: string) {
    debug("waiting for lock in webworker");
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

        gotLockPort.postMessage("lock acquired");
        return new Promise((resolve) => {
          forceLeaderElectionBroadcastChannel.onmessage = () => {
            abortController?.abort(FORCE_LEADER_ELECTION);
          };
          abortController?.signal.addEventListener("abort", () => {
            hasTheLock = false;
            sqlite?.close();
            poolUtil?.pauseVfs();
            resolve(abortController?.signal.reason);
          });
        });
      },
    );
  },
  async receiveBroadcast(_message: BroadcastMessage) {},
} as const;

export type WorkerInterface = typeof workerInterface;
