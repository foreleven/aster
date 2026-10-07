import { askQuery } from "../actors.js";
import { Type } from "@aster/agent";
import type { ContextsCommand } from "../../context/queries/actor.js";
import { queryTool } from "../define.js";

export const searchContexts = () =>
  queryTool(
    {
      name: "search_contexts",
      replay: "safe",
      label: "Find Contexts",
      description:
        "Find public Context paths by words in their path or description. Empty query browses all. Use nextOffset to paginate.",
      parameters: Type.Object({
        query: Type.String(),
        offset: Type.Optional(Type.Integer({ minimum: 0 })),
      }),
    },
    ({ query, offset = 0 }) =>
      askQuery<ContextsCommand>("/user/contexts", (replyTo) => ({
        _tag: "SearchContexts",
        query,
        offset,
        replyTo,
      })),
  );
