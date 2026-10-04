import { mkdir, open, type FileHandle } from "node:fs/promises";
import { join } from "node:path";
import { homedir } from "node:os";
import { isDeepStrictEqual } from "node:util";
import { Effect, Schema, Semaphore } from "effect";
import { GoalHistoryError, type GoalHistory, type HistoryEntry } from "@aster/core";

const StoredHistoryEntry = Schema.Struct({
  seq: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
  at: Schema.String,
  requestId: Schema.optional(Schema.NonEmptyString),
  message: Schema.Unknown,
});

interface Index {
  /** Byte boundary for every sequence, including the final committed boundary. */
  offsets: number[];
  requests: Map<string, number>;
}

/** A single writer owns each journal. Indexes retain offsets, never transcript bodies. */
export const makeFileGoalHistory = (root = join(homedir(), ".aster", "goals")): GoalHistory => {
  const indexes = new Map<string, Index>();
  const locks = new Map<string, Semaphore.Semaphore>();
  const path = (goal: string) => {
    if (!/^[a-z0-9][a-z0-9-]*$/.test(goal)) throw new Error("Invalid Goal history ID");
    return join(root, goal, "history.jsonl");
  };
  const load = async (goal: string): Promise<Index> => {
    const cached = indexes.get(goal);
    if (cached) return cached;
    const file = path(goal);
    await mkdir(join(root, goal), { recursive: true, mode: 0o700 });
    const fd = await open(file, "a+", 0o600);
    const index: Index = { offsets: [0], requests: new Map() };
    let pending = Buffer.alloc(0),
      position = 0;
    try {
      const chunk = Buffer.alloc(64 * 1024);
      while (true) {
        const { bytesRead } = await fd.read(chunk, 0, chunk.length, position);
        if (!bytesRead) break;
        position += bytesRead;
        pending = Buffer.concat([pending, chunk.subarray(0, bytesRead)]);
        let end: number;
        while ((end = pending.indexOf(10)) >= 0) {
          const entry = Schema.decodeUnknownSync(StoredHistoryEntry)(
            JSON.parse(pending.subarray(0, end).toString("utf8")),
          );
          if (entry.seq !== index.offsets.length)
            throw new Error(`Goal history sequence mismatch: ${goal}`);
          if (entry.requestId !== undefined) {
            if (
              typeof entry.requestId !== "string" ||
              !entry.requestId ||
              index.requests.has(entry.requestId)
            )
              throw new Error(`Invalid Goal history request identity: ${goal}`);
            index.requests.set(entry.requestId, entry.seq);
          }
          index.offsets.push(index.offsets.at(-1)! + end + 1);
          pending = pending.subarray(end + 1);
        }
      }
      // Only an incomplete trailing append is recoverable; committed corruption is an error.
      if (pending.length) {
        await fd.truncate(index.offsets.at(-1)!);
        await fd.sync();
      }
    } finally {
      await fd.close();
    }
    indexes.set(goal, index);
    return index;
  };
  const operate = <A>(goal: string, use: (index: Index) => Promise<A>) =>
    Effect.suspend(() => {
      let lock = locks.get(goal);
      if (!lock) {
        lock = Semaphore.makeUnsafe(1);
        locks.set(goal, lock);
      }
      // Await an in-flight commit before releasing its lock, including during Scope shutdown.
      return lock.withPermit(
        Effect.tryPromise({
          try: async () => {
            try {
              return await use(await load(goal));
            } catch (cause) {
              indexes.delete(goal);
              throw cause;
            }
          },
          catch: (cause) => new GoalHistoryError({ cause }),
        }).pipe(Effect.uninterruptible),
      );
    });
  const readExactly = async (fd: FileHandle, size: number, position: number) => {
    const bytes = Buffer.alloc(size);
    let offset = 0;
    while (offset < size) {
      const { bytesRead } = await fd.read(bytes, offset, size - offset, position + offset);
      if (!bytesRead) throw new Error("Goal history was truncated outside its writer");
      offset += bytesRead;
    }
    return bytes;
  };
  return {
    count: (goal) => operate(goal, async (index) => index.offsets.length - 1),
    append: (goal, message, requestId) =>
      operate(goal, async (index) => {
        const previousSequence =
          requestId === undefined ? undefined : index.requests.get(requestId);
        if (previousSequence !== undefined) {
          const fd = await open(path(goal), "r");
          try {
            const bytes = await readExactly(
              fd,
              index.offsets[previousSequence]! - index.offsets[previousSequence - 1]!,
              index.offsets[previousSequence - 1]!,
            );
            const entry = Schema.decodeUnknownSync(StoredHistoryEntry)(
              JSON.parse(bytes.toString("utf8")),
            );
            if (!isDeepStrictEqual(entry.message, message))
              throw new Error("History request ID belongs to another message");
            return { ...entry, message: structuredClone(message) };
          } finally {
            await fd.close();
          }
        }
        const entry: HistoryEntry = {
          ...(requestId === undefined ? {} : { requestId }),
          seq: index.offsets.length,
          at: new Date().toISOString(),
          message,
        };
        const bytes = Buffer.from(JSON.stringify(entry) + "\n");
        const fd = await open(path(goal), "a", 0o600);
        try {
          await fd.writeFile(bytes);
          await fd.sync();
        } finally {
          await fd.close();
        }
        index.offsets.push(index.offsets.at(-1)! + bytes.length);
        if (requestId !== undefined) index.requests.set(requestId, entry.seq);
        return structuredClone(entry);
      }),
    read: (goal, options = {}) =>
      operate(goal, async (index) => {
        const count = index.offsets.length - 1;
        const start = Math.min(count, Math.max(0, Math.floor(options.after ?? 0)));
        const limit = Math.max(1, Math.min(Math.floor(options.limit ?? 100), 10000));
        const end = Math.min(count, Math.ceil(options.before ?? count + 1) - 1, start + limit);
        if (end <= start) return [];
        const fd = await open(path(goal), "r");
        try {
          const bytes = await readExactly(
            fd,
            index.offsets[end]! - index.offsets[start]!,
            index.offsets[start]!,
          );
          return bytes
            .toString("utf8")
            .trimEnd()
            .split("\n")
            .map((line) => JSON.parse(line) as HistoryEntry);
        } finally {
          await fd.close();
        }
      }),
  };
};
