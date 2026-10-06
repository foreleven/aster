import { AgentConversations } from "@aster/agent";
import { Effect, Ref, Schema } from "effect";
import { ExecutionSession } from "./contracts.js";
import { TaskOutcome } from "../state/snapshot.js";

export const ExecutionCheckpoint = Schema.Struct({
  revision: Schema.Int,
  prompt: Schema.String,
  approved: Schema.Boolean,
  session: Schema.optional(ExecutionSession),
  deliveries: Schema.Array(
    Schema.Struct({
      requestId: Schema.String,
      roundId: Schema.String,
      kind: Schema.Literals(["instruction", "answer", "control"]),
      status: Schema.Literals(["sending", "accepted", "rejected", "unknown"]),
    }),
  ),
  outcome: Schema.optional(TaskOutcome),
});
export type ExecutionCheckpoint = typeof ExecutionCheckpoint.Type;
export const readExecutionCheckpoint = Effect.fn("TaskExecution.readCheckpoint")(function* (
  path: string,
) {
  const messages = yield* AgentConversations;
  const entry = (yield* messages.read(path).pipe(Effect.orDie)).findLast(
    (entry) => entry.kind === "task.execution",
  );
  return entry ? Schema.decodeUnknownSync(ExecutionCheckpoint)(entry.data) : undefined;
});
/** Private executor journal. Its caller serializes transitions, including submissions. */
export const executionCheckpoint = Effect.fn("TaskExecution.checkpoint")(function* (
  path: string,
  prompt: string,
) {
  const messages = yield* AgentConversations;
  const ref = yield* Ref.make<ExecutionCheckpoint>(
    (yield* readExecutionCheckpoint(path)) ?? {
      revision: 0,
      prompt,
      approved: false,
      deliveries: [],
    },
  );
  const read = Ref.get(ref);
  const save = Effect.fn("TaskExecution.commitCheckpoint")(function* (
    patch: Partial<ExecutionCheckpoint>,
  ) {
    const current = yield* read;
    const next = { ...current, ...patch, revision: current.revision + 1 };
    yield* messages
      .append(path, `execution:${next.revision}`, "task.execution", next)
      .pipe(Effect.orDie);
    yield* Ref.set(ref, next);
  }, Effect.uninterruptible);
  return { read, save };
});
