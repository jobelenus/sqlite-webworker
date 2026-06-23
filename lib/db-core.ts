// Pure SQLite + locking core. No Comlink, no `self.*`, no top-level side
// effects beyond module state. Safe to import into any worker that wants to own
// the database connection (the lib's default DB worker, or a consumer's custom
// one built with `defineDbWorker`).
import sqlite3InitModule, {
  type Database,
  type ExecBaseOptions,
  type ExecReturnResultRowsOptions,
  type ExecRowModeArrayOptions,
  type FlexibleString,
  type SAHPoolUtil,
  type Sqlite3Static,
  type SqlValue,
} from "@sqlite.org/sqlite-wasm";
import { DB_NOT_INIT_ERR, NOROW } from "./consts";

export type { SqlValue, FlexibleString };

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

// Boot sqlite-wasm and open `dbName`. Call once, inside the worker that owns the
// connection, before any dbExec/dbRead/dbWrite.
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

// Close the connection and pause the VFS so another worker (the next leader) can
// open the single OPFS connection. Called when this worker loses the lock.
export const closeDatabase = () => {
  sqlite?.close();
  sqlite = undefined;
  poolUtil?.pauseVfs();
};

const locks: Record<string, ReadWriteLock> = {};
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const dbRead = async (name: string, opts: any) => {
  const lock = locks[name];
  if (!lock) throw new Error("DB Lock not acquired");
  return await lock.readLock(() => {
    if (!sqlite) throw new Error(DB_NOT_INIT_ERR);
    return sqlite.exec(opts);
  });
};
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const dbWrite = async (name: string, opts: any) => {
  const lock = locks[name];
  if (!lock) throw new Error("DB Lock not acquired");
  return await lock.writeLock(() => {
    if (!sqlite) throw new Error(DB_NOT_INIT_ERR);
    return sqlite.exec(opts);
  });
};

export const createLock = (name: string) => {
  locks[name] = new ReadWriteLock(name);
};

// Raw execution without any locks, beware... `rowMode`/`returnValue` are
// optional: pass them (e.g. `{ rowMode: "array", returnValue: "resultRows" }`)
// to get result rows back for reads; omit them for writes.
export type DbExecOptions = ExecBaseOptions &
  Partial<ExecRowModeArrayOptions & ExecReturnResultRowsOptions> & {
    sql: FlexibleString;
  };

export const dbExec = (opts: DbExecOptions): SqlValue[][] => {
  if (!sqlite) {
    throw new Error(DB_NOT_INIT_ERR);
  }
  return sqlite.exec(
    opts as ExecBaseOptions &
      ExecRowModeArrayOptions &
      ExecReturnResultRowsOptions & { sql: FlexibleString },
  );
};

// Push `message` to every tab on this origin via a BroadcastChannel. Runs in the
// worker, so the post originates from the worker context and reaches every
// tab's main thread (including the tab that triggered it). Use from inside your
// app methods after a write to fan out a fresh snapshot.
export const pushToTabs = (channelName: string, message: unknown) => {
  const channel = new BroadcastChannel(channelName);
  channel.postMessage(message);
  channel.close();
};
