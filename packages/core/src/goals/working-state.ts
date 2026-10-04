import { DateTime, Effect, Schema } from "effect";
import { goalNotifications } from "../notifications/goal.js";
import type { AgentMessage } from "@aster/agent";
import type { GoalDefinition } from "../config/schema.js";
import type { ContextRegistry } from "../context/registry.js";
import { GoalState } from "./state.js";
import { completePrefix, contextSize, runtimeMessage, type GoalHistory } from "./history.js";

/** Mailbox-only writes; full history and the bounded public working window share one commit path. */
export const goalWorkingState = (
  registry: ContextRegistry["Service"],
  history: GoalHistory,
  goal: () => GoalDefinition,
  path: () => string,
  contextTokens: number,
) => {
  const current = () => registry.get(path())!;
  const state = () => Schema.decodeUnknownSync(GoalState)(current().state);
  const save = Effect.fn("Goal.save")(function* (patch: Partial<GoalState> = {}) {
    const snapshot = current();
    const previous = Schema.decodeUnknownSync(GoalState)(snapshot.state);
    const s = {
      ...previous,
      ...patch,
      historyCount: yield* history.count(goal().slug).pipe(Effect.orDie),
    };
    if (Object.hasOwn(patch, "lastError") && patch.lastError === undefined)
      delete (s as { lastError?: string }).lastError;
    if (Object.hasOwn(patch, "pendingRequestId") && patch.pendingRequestId === undefined)
      delete (s as { pendingRequestId?: string }).pendingRequestId;
    if (Object.hasOwn(patch, "pendingHandoff") && patch.pendingHandoff === undefined)
      delete s.pendingHandoff;
    if (Object.hasOwn(patch, "retryTurnId") && patch.retryTurnId === undefined)
      delete s.retryTurnId;
    if (Object.hasOwn(patch, "nextStep") && patch.nextStep === undefined) delete s.nextStep;
    // The public working window is bounded independently from the full feed on disk.
    const entries = yield* history
      .read(goal().slug, { after: s.historyThrough, limit: 200 })
      .pipe(Effect.orDie);
    const messages: AgentMessage[] = [];
    let size = 0;
    for (const entry of entries) {
      size += contextSize(entry.message);
      if (size > contextTokens) break;
      messages.push(entry.message);
    }
    return yield* registry
      .commit(
        {
          ...snapshot,
          state: {
            ...s,
            businessOutbox: goalNotifications({
              path: snapshot.path,
              revision: (snapshot.revision ?? 0) + 1,
              at: DateTime.formatIso(yield* DateTime.now),
              previous,
              next: s,
            }),
          },
          messages: messages.slice(0, completePrefix(messages)),
        },
        { expectedRevision: snapshot.revision ?? 0 },
      )
      .pipe(Effect.asVoid);
  });
  const append = Effect.fn("Goal.append")(function* (message: AgentMessage) {
    yield* history.append(goal().slug, message).pipe(Effect.orDie);
    yield* save();
  });
  const event = (text: string) => append(runtimeMessage(text));
  return { current, state, save, append, event };
};
