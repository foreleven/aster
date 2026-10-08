import { askQuery } from "../actors.js";
import { Type } from "@aster/agent";
import type { ContextsCommand } from "../../context/queries/actor.js";
import { queryTool } from "../define.js";
export const listContexts = () =>
  queryTool(
    {
      name: "list_contexts",
      replay: "safe",
      label: "List Contexts",
      description:
        "List active Context capability paths and descriptions, without business data. Optionally scope to descendants of parent. Use describe_context to inspect commands and nextOffset to paginate.",
      parameters: Type.Object({
        parent: Type.Optional(Type.String()),
        offset: Type.Optional(Type.Integer({ minimum: 0 })),
      }),
    },
    ({ parent, offset = 0 }) =>
      askQuery<ContextsCommand>("/user/contexts", (replyTo) => ({
        _tag: "ListContexts",
        parent,
        offset,
        replyTo,
      })),
  );
