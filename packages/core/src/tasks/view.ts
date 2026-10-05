import { Schema } from "effect";
import { contextView } from "../context/view.js";
import { publicBusinessMessage } from "../reasoning/public-messages.js";
import { WritebackOperation, PreparedTask, RunResumption } from "@aster/api-contracts";
const optionalString = Schema.optional(Schema.String);
const view = contextView({
  state: Schema.Struct({
    status: Schema.String,
    admission: Schema.Struct({
      input: Schema.Struct({
        source: Schema.String,
        replyTo: Schema.String,
        agent: Schema.String,
        task: PreparedTask,
      }),
    }),
    outcomeText: optionalString,
    writeback: Schema.optional(WritebackOperation),
    resumptions: Schema.optional(Schema.Array(RunResumption)),
  }),
  projectMessage: publicBusinessMessage,
});
export const runView = {
  matches: (path: string) => /^\/runs\/[^/]+$/.test(path),
  project: (record: Parameters<typeof view.project>[0]) => {
    const projected = view.project(record);
    if (!projected) return undefined;
    const state = projected.state as {
      admission: {
        input: { source: string; replyTo: string; agent: string; task: typeof PreparedTask.Type };
      };
    };
    const { admission, ...content } = state;
    return {
      ...projected,
      state: {
        ...content,
        sourcePath: admission.input.source,
        replyTo: admission.input.replyTo,
        agent: admission.input.agent,
        task: admission.input.task,
      },
    };
  },
};
export const tasksRootView = contextView({
  matches: (path) => path === "/runs",
  state: Schema.Struct({}),
});
