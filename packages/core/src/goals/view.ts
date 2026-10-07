import { GoalSnapshot } from "./state/snapshot.js";
import { type ContextViewPolicy, contextView } from "../context/definition.js";

import { Option, Schema, Effect } from "effect";
import type { ContextRegistry } from "../context/registry.js";
import { AgentConversations } from "@aster/agent";
import { ApplicationError } from "../operations.js";
import { GoalInputPayload } from "./contracts.js";

/** Display metadata and the latest settled error are derived from canonical Goal state. */
export const goalView: ContextViewPolicy = {
  matches: (path) => /^\/goals\/[^/]+$/.test(path),
  project: (record) => {
    const decoded = Schema.decodeUnknownOption(GoalSnapshot)(record.state);
    if (Option.isNone(decoded)) return undefined;
    const state = decoded.value;
    const latest = state.inputs.findLast((input) =>
      ["completed", "failed", "unknown"].includes(input.status),
    );
    return {
      path: record.path,
      revision: record.revision ?? 0,
      description: state.definition.description,
      state: {
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
      },
      messages: [],
      projection: { visibility: "public" },
    };
  },
};
export const goalsRootView = contextView({
  matches: (path) => path === "/goals",
  state: Schema.Struct({}),
});

export const goalTimeline = (
  registry: ContextRegistry["Service"],
  conversations: Pick<AgentConversations["Service"], "read">,
  slug: string,
  page: { before?: number; limit?: number } = {},
) =>
  Effect.gen(function* () {
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
  }).pipe(Effect.withSpan("Goal.timeline"));
