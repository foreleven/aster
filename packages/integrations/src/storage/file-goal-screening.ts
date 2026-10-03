import { mkdir, open, type FileHandle } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import { Effect, Semaphore } from "effect";
import { GoalScreeningStore, GoalScreeningStoreError, type GoalScreeningRecord } from "@aster/core";

/** Append-only screening evidence; raw chat message batches are intentionally absent. */
export const makeFileGoalScreeningStore = (
  root = join(homedir(), ".aster", "evaluations"),
): GoalScreeningStore["Service"] => {
  const lock = Semaphore.makeUnsafe(1);
  const filePath = join(root, "goal-screening.jsonl");
  const append = (record: GoalScreeningRecord) =>
    Effect.tryPromise({
      try: async () => {
        await mkdir(root, { recursive: true, mode: 0o700 });
        const fd: FileHandle = await open(filePath, "a", 0o600);
        try {
          await fd.writeFile(`${JSON.stringify(record)}\n`, "utf8");
          await fd.sync();
        } finally {
          await fd.close();
        }
      },
      catch: (cause) =>
        new GoalScreeningStoreError({
          cause,
          message: cause instanceof Error ? cause.message : String(cause),
        }),
    }).pipe(Effect.uninterruptible);
  return { append: (record) => lock.withPermit(append(record)) };
};
