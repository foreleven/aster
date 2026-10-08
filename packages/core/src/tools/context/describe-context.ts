import { askQuery } from "../actors.js";
import { Type } from "@aster/agent";
import type { ContextsCommand } from "../../context/queries/actor.js";
import { queryTool } from "../define.js";
export const describeContext = () =>
  queryTool(
    {
      name: "describe_context",
      replay: "safe",
      label: "Describe Context",
      description:
        "Describe a Context's supported read-only commands and argument schemas. Returns capability metadata, not state or messages. Use query_context to request specific data.",
      parameters: Type.Object({ path: Type.String() }),
    },
    ({ path }) =>
      askQuery<ContextsCommand>("/user/contexts", (replyTo) => ({
        _tag: "DescribeContext",
        path,
        replyTo,
      })),
  );
