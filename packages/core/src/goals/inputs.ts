import { createHash } from "node:crypto";
import { Effect, Match, Schema } from "effect";
import { ApplicationError, CausalChain, GoalInputPayload } from "@aster/api-contracts";
import { AgentConversations, type AgentMessage } from "@aster/agent";
import type { goalWorkingState } from "./working-state.js";
import { goalIntentMessage } from "./intent.js";
import { GoalReceipt } from "./protocol.js";
import type { GoalState } from "./state.js";

export const StoredGoalInput = Schema.Struct({
  inputId: Schema.String,
  goalSlug: Schema.String,
  ordinal: Schema.Int,
  receivedAt: Schema.String,
  entryId: Schema.Int,
  kind: Schema.String,
  status: Schema.Literals(["pending", "running", "completed", "failed", "unknown", "ignored"]),
  relevant: Schema.optional(Schema.Boolean),
  error: Schema.optional(Schema.String),
  retryOf: Schema.optional(Schema.String),
  causal: Schema.optional(CausalChain),
});
export type StoredGoalInput = typeof StoredGoalInput.Type;
export type ResolvedGoalInput = StoredGoalInput & {
  readonly payload: typeof GoalInputPayload.Type;
};
export const goalInputId = (goal: string, kind: string, key: string) =>
  createHash("sha256")
    .update(JSON.stringify([goal, kind, key]))
    .digest("hex");
export const resolveGoalInput = (messages: AgentConversations["Service"], input: StoredGoalInput) =>
  messages.get(`/goals/${input.goalSlug}`, input.entryId).pipe(
    Effect.flatMap((entry) =>
      Schema.decodeUnknownEffect(Schema.Struct({ payload: GoalInputPayload }))(entry.data),
    ),
    Effect.map(({ payload }): ResolvedGoalInput => ({ ...input, payload })),
    Effect.orDie,
  );
export const inputMessage = (input: ResolvedGoalInput): AgentMessage =>
  Match.value(input.payload).pipe(
    Match.tag("GoalIntent", ({ intent }) => goalIntentMessage(intent)),
    Match.tag("GoalStarted", () => ({
      role: "user" as const,
      content: "Begin pursuing the configured Goal now.",
      timestamp: Date.parse(input.receivedAt),
    })),
    Match.tag("UserInput", ({ text }) => ({
      role: "user" as const,
      content: text,
      timestamp: Date.parse(input.receivedAt),
    })),
    Match.orElse((payload) => ({
      role: "user" as const,
      content: `[Internal Goal evidence, not a user statement or authorization]\n${JSON.stringify(payload)}`,
      timestamp: Date.parse(input.receivedAt),
    })),
  );

/** Pi owns bodies; the mailbox commits only input references and delivery state. */
export const goalInputs = (
  working: ReturnType<typeof goalWorkingState>,
  messages: AgentConversations["Service"],
) => {
  const accept = Effect.fn("GoalInputs.accept")(function* (
    payload: typeof GoalInputPayload.Type,
    key: string,
    causal: CausalChain,
    patch: Partial<GoalState> = {},
    expectedRevision?: number,
    retryOf?: string,
  ) {
    const state = working.state();
    if (expectedRevision !== undefined && expectedRevision !== working.current().revision)
      return yield* new ApplicationError({
        kind: "conflict",
        message: "Goal revision changed; refresh before submitting",
      });
    const inputId = goalInputId(state.definition.slug, payload._tag, key);
    const prior = state.inputs.find((item) => item.inputId === inputId);
    const entry = yield* messages
      .append(`/goals/${state.definition.slug}`, inputId, "goal.input", {
        payload,
        causal,
        ...(retryOf ? { retryOf } : {}),
        ...(patch.receipts?.length ? { receipt: patch.receipts.at(-1) } : {}),
      })
      .pipe(Effect.orDie);
    if (!prior) {
      const input: StoredGoalInput = {
        inputId,
        goalSlug: state.definition.slug,
        ordinal: (state.inputs.at(-1)?.ordinal ?? 0) + 1,
        receivedAt: entry.at,
        entryId: entry.id,
        kind: payload._tag,
        causal,
        status: state.status === "active" ? "pending" : "ignored",
        ...(retryOf ? { retryOf } : {}),
      };
      yield* working
        .save({ ...patch, inputs: [...state.inputs, input] }, expectedRevision)
        .pipe(Effect.orDie);
    } else yield* working.save(patch).pipe(Effect.orDie);
    return !prior;
  });
  const recover = Effect.fn("GoalInputs.recover")(function* () {
    const state = working.state();
    const entries = yield* messages.read(`/goals/${state.definition.slug}`).pipe(Effect.orDie);
    const inputs = [...state.inputs];
    const receipts = [...state.receipts];
    for (const entry of entries) {
      if (entry.kind !== "goal.input") continue;
      const data = Schema.decodeUnknownSync(
        Schema.Struct({
          payload: GoalInputPayload,
          causal: CausalChain,
          retryOf: Schema.optional(Schema.String),
          receipt: Schema.optional(GoalReceipt),
        }),
      )(entry.data);
      if (data.receipt && !receipts.some((item) => item.requestId === data.receipt!.requestId))
        receipts.push(data.receipt);
      if (inputs.some((input) => input.inputId === entry.requestId)) continue;
      inputs.push({
        inputId: entry.requestId,
        goalSlug: state.definition.slug,
        ordinal: (inputs.at(-1)?.ordinal ?? 0) + 1,
        receivedAt: entry.at,
        entryId: entry.id,
        kind: data.payload._tag,
        causal: data.causal,
        status: state.status === "active" ? "pending" : "ignored",
        ...(data.retryOf ? { retryOf: data.retryOf } : {}),
      });
    }
    if (inputs.length !== state.inputs.length || receipts.length !== state.receipts.length)
      yield* working.save({ inputs, receipts }).pipe(Effect.orDie);
  });
  return { accept, recover };
};
