import { publicJson } from "../json.js";
import { GoalSnapshot } from "./state/snapshot.js";

import { AgentConversations } from "../agent/runtime/session.js";
import { Effect, Schema } from "effect";
import type { DurableContext } from "../context/store.js";
import { ApplicationError } from "../operations.js";
import { GoalInputPayload } from "./contracts.js";

/** Display metadata and the latest settled error are derived from canonical Goal state. */

export const goalTimeline: (
  registry: DurableContext["Service"],
  conversations: Pick<AgentConversations["Service"], "read">,
  slug: string,
  page?: { before?: number; limit?: number },
) => Effect.Effect<
  {
    messages: { id: number; role: "user" | "assistant"; text: string; at: string }[];
    total: number;
    nextBefore: number | null;
  },
  ApplicationError
> = Effect.fn("Goal.timeline")(function* (
  registry: DurableContext["Service"],
  conversations: Pick<AgentConversations["Service"], "read">,
  slug: string,
  page: { before?: number; limit?: number } = {},
): Effect.fn.Return<
  {
    messages: { id: number; role: "user" | "assistant"; text: string; at: string }[];
    total: number;
    nextBefore: number | null;
  },
  ApplicationError
> {
  if (!registry.get(`/goals/${slug}`))
    return yield* new ApplicationError({ kind: "not-found", message: "Goal not found" });
  const limit = page.limit ?? 30;
  if (
    !Number.isInteger(limit) ||
    limit < 1 ||
    limit > 100 ||
    (page.before !== undefined && (!Number.isInteger(page.before) || page.before < 1))
  )
    return yield* new ApplicationError({
      kind: "invalid-input",
      message: "Invalid conversation page",
    });
  const entries = yield* conversations
    .read(`/goals/${slug}`)
    .pipe(
      Effect.mapError(
        () => new ApplicationError({ kind: "unavailable", message: "Conversation unavailable" }),
      ),
    );
  const all = [];
  for (const entry of entries) {
    if (entry.kind === "goal.input") {
      const { payload } = Schema.decodeUnknownSync(Schema.Struct({ payload: GoalInputPayload }))(
        entry.data,
      );
      if (payload._tag === "UserInput")
        all.push({
          id: entry.id,
          role: "user" as const,
          text: payload.text,
          at: entry.at,
        });
    } else if (entry.kind === "goal.reply") {
      const data = Schema.decodeUnknownSync(
        Schema.Struct({ inputId: Schema.String, text: Schema.String }),
      )(entry.data);
      all.push({
        id: entry.id,
        role: "assistant" as const,
        text: data.text,
        at: entry.at,
      });
    }
  }
  const eligible = all.filter((message) => page.before === undefined || message.id < page.before);
  const messages = eligible.slice(-limit);
  return {
    messages,
    total: all.length,
    nextBefore: eligible.length > messages.length ? messages[0]!.id : null,
  };
});

export const goalQueryData = (path: string, state: GoalSnapshot, detail: boolean) =>
  publicJson({
    path,
    title: state.definition.title ?? state.definition.description,
    description: state.definition.description,
    status: state.status,
    summary: state.summary,
    ...(detail ? { tasks: state.tasks } : {}),
  });

export const goalBusinessState = (state: GoalSnapshot) => {
  const latest = state.inputs.findLast((input) =>
    ["completed", "failed", "unknown"].includes(input.status),
  );
  return {
    ...state.definition,
    title: state.definition.title ?? state.definition.description,
    status: state.status,
    summary: state.summary,
    tasks: state.tasks,
    ...(state.status === "active" &&
    latest?.status === "failed" &&
    !state.inputs.some((input) => ["pending", "running", "unknown"].includes(input.status))
      ? { retryableInputId: latest.inputId }
      : {}),
    ...(latest?.status !== "completed" && latest?.error ? { lastError: latest.error } : {}),
  };
};
