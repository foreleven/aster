import { Option, Schema } from "effect";
import { contextView } from "../context/view.js";
import type { ContextViewPolicy } from "../context/definition.js";
import { publicBusinessMessage } from "../reasoning/public-messages.js";
import { GoalState } from "./state.js";

/** Display metadata and the latest settled error are derived from canonical Goal state. */
export const goalView: ContextViewPolicy = {
  matches: (path) => /^\/goals\/[^/]+$/.test(path),
  project: (record) => {
    const decoded = Schema.decodeUnknownOption(GoalState)(record.state);
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
        ...(latest?.status !== "completed" && latest?.error ? { lastError: latest.error } : {}),
      },
      messages: record.messages.flatMap((message) => {
        const projected = publicBusinessMessage(message);
        return projected === undefined ? [] : [projected];
      }),
      projection: { version: 1, visibility: "public" },
    };
  },
};
export const goalsRootView = contextView({
  matches: (path) => path === "/goals",
  state: Schema.Struct({}),
});
