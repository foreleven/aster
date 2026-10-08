import type { StoredGoalInput, GoalSnapshot } from "./snapshot.js";
import { GoalReceipt } from "../protocol.js";
import type { GoalStore } from "./store.js";
import { AgentConversations } from "@aster/agent/harness";
import { RemainingAgentTurns } from "../../tasks/contracts.js";
import { GoalInputPayload } from "../contracts.js";
import { Effect, Schema } from "effect";
import { createHash } from "node:crypto";

export type ResolvedGoalInput = StoredGoalInput & {
  readonly payload: typeof GoalInputPayload.Type;
  readonly receivedAt: string;
};
export const goalInputId = (goal: string, kind: string, key: string) =>
  createHash("sha256")
    .update(JSON.stringify([goal, kind, key]))
    .digest("hex");
const GoalInputEntry = Schema.Struct({
  payload: GoalInputPayload,
  remainingAgentTurns: RemainingAgentTurns,
  retryOf: Schema.optional(Schema.String),
  receipt: Schema.optional(GoalReceipt),
});
const decodeInput = Schema.decodeUnknownEffect(GoalInputEntry);
const inputReference = (
  entry: { readonly id: number; readonly requestId: string },
  data: typeof GoalInputEntry.Type,
  active: boolean,
): StoredGoalInput => ({
  inputId: entry.requestId,
  entryId: entry.id,
  kind: data.payload._tag,
  remainingAgentTurns: data.remainingAgentTurns,
  status: active ? "pending" : "ignored",
  ...(data.retryOf ? { retryOf: data.retryOf } : {}),
});

/** Pi owns bodies; GoalState commits only input references and delivery state. */
export const goalInputs = (working: GoalStore, messages: AgentConversations["Service"]) => {
  const accept = Effect.fn("GoalInputs.accept")(function* (
    payload: typeof GoalInputPayload.Type,
    key: string,
    remainingAgentTurns: RemainingAgentTurns,
    patch: Partial<GoalSnapshot> = {},
    retryOf?: string,
  ) {
    const state = yield* working.read;
    const inputId = goalInputId(state.definition.slug, payload._tag, key);
    const prior = state.inputs.find((item) => item.inputId === inputId);
    const entry = yield* messages
      .append(`/goals/${state.definition.slug}`, inputId, "goal.input", {
        payload,
        remainingAgentTurns,
        ...(retryOf ? { retryOf } : {}),
        ...(patch.receipts?.length ? { receipt: patch.receipts.at(-1) } : {}),
      })
      .pipe(Effect.orDie);
    if (!prior) {
      const data = yield* decodeInput(entry.data).pipe(Effect.orDie);
      const input = inputReference(entry, data, state.status === "active");
      yield* working.save({ ...patch, inputs: [...state.inputs, input] }).pipe(Effect.orDie);
    } else yield* working.save(patch).pipe(Effect.orDie);
    return !prior;
  });
  const recover = Effect.fn("GoalInputs.recover")(function* () {
    const state = yield* working.read;
    const entries = yield* messages.read(`/goals/${state.definition.slug}`).pipe(Effect.orDie);
    const inputs = [...state.inputs];
    const receipts = [...state.receipts];
    for (const entry of entries) {
      if (entry.kind !== "goal.input") continue;
      const data = yield* decodeInput(entry.data).pipe(Effect.orDie);
      if (data.receipt && !receipts.some((item) => item.requestId === data.receipt!.requestId))
        receipts.push(data.receipt);
      if (inputs.some((input) => input.inputId === entry.requestId)) continue;
      inputs.push(inputReference(entry, data, state.status === "active"));
    }
    if (inputs.length !== state.inputs.length || receipts.length !== state.receipts.length)
      yield* working.save({ inputs, receipts }).pipe(Effect.orDie);
  });
  const resolve = Effect.fn("GoalInputs.resolve")(function* (input: StoredGoalInput) {
    const entry = yield* messages
      .get((yield* working.current).path, input.entryId)
      .pipe(Effect.orDie);
    const { payload } = yield* decodeInput(entry.data).pipe(Effect.orDie);
    return { ...input, payload, receivedAt: entry.at } satisfies ResolvedGoalInput;
  });
  return { accept, recover, resolve };
};
