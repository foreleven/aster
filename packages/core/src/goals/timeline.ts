import {
  ApplicationError,
  GoalInputPayload,
  type GoalTimelinePage,
  type GoalConversationMessage,
} from "@aster/api-contracts";
import { AgentConversations } from "@aster/agent";
import { Effect, Schema } from "effect";
import type { ContextRegistry } from "../context/registry.js";

export const goalTimeline: (
  registry: ContextRegistry["Service"],
  conversations: AgentConversations["Service"],
  slug: string,
  page?: { before?: number; limit?: number },
) => Effect.Effect<GoalTimelinePage, ApplicationError> = Effect.fn("Goal.timeline")(function* (
  registry: ContextRegistry["Service"],
  conversations: AgentConversations["Service"],
  slug: string,
  page: { before?: number; limit?: number } = {},
) {
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
  const all: GoalConversationMessage[] = [];
  for (const entry of entries) {
    if (entry.kind === "goal.input") {
      const { payload } = Schema.decodeUnknownSync(Schema.Struct({ payload: GoalInputPayload }))(
        entry.data,
      );
      if (payload._tag === "UserInput")
        all.push({
          id: entry.id,
          inputId: entry.requestId,
          role: "user",
          text: payload.text,
          at: entry.at,
        });
    } else if (entry.kind === "goal.reply") {
      const data = Schema.decodeUnknownSync(
        Schema.Struct({ inputId: Schema.String, text: Schema.String }),
      )(entry.data);
      all.push({
        id: entry.id,
        inputId: data.inputId,
        role: "assistant",
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
