import { Option, Schema } from "effect";
import { WritebackOperation } from "@aster/api-contracts";
import { contextView } from "../context/view.js";
import { TaskState } from "./state.js";
const PublicTask = Schema.Struct({
  status: TaskState.fields.status,
  sourcePath: Schema.String,
  replyTo: Schema.String,
  agent: Schema.String,
  inputs: Schema.Int,
  writeback: Schema.optional(WritebackOperation),
});
export const taskView: import("../context/definition.js").ContextViewPolicy = {
  matches: (path) => /^\/tasks\/[^/]+$/.test(path),
  project: (record) => {
    const canonical = Schema.decodeUnknownOption(TaskState)(record.state);
    let publicState = Schema.decodeUnknownOption(PublicTask)(record.state);
    if (Option.isSome(canonical))
      publicState = Option.some({
        status: canonical.value.status,
        sourcePath: canonical.value.admission.input.source,
        replyTo: canonical.value.admission.input.replyTo,
        agent: canonical.value.admission.input.agent,
        inputs: canonical.value.inputs.length,
        writeback: canonical.value.writeback,
      });
    if (Option.isNone(publicState)) return undefined;
    return {
      path: record.path,
      revision: record.revision ?? 0,
      description: record.description,
      state: publicState.value,
      messages: [],
      projection: { version: 1, visibility: "public" },
    };
  },
};
export const tasksRootView = contextView({
  matches: (path) => path === "/tasks",
  state: Schema.Struct({}),
});
