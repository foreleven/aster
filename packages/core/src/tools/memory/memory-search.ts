import { askQuery } from "../actors.js";
import { Type } from "@aster/agent";
import type { MemoryCommand } from "../../memory/actor.js";
import { queryTool } from "../define.js";

export const memorySearch = () =>
  queryTool(
    {
      name: "memory_search",
      replay: "safe",
      label: "Search memory",
      description: "Search compact memory candidates; expand before relying on them.",
      parameters: Type.Object({ query: Type.String() }),
    },
    ({ query }) =>
      askQuery<MemoryCommand>("/user/memory", (replyTo, cancelled) => ({
        _tag: "Search",
        query,
        replyTo,
        cancelled,
      })),
  );
