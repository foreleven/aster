import { askQuery } from "../actors.js";
import { Type } from "@aster/agent";
import type { MemoryCommand } from "../../memory/actor.js";
import { queryTool } from "../define.js";

export const memoryExpand = () =>
  queryTool(
    {
      name: "memory_expand",
      replay: "safe",
      label: "Expand memory",
      description: "Read original evidence behind selected memories.",
      parameters: Type.Object({
        items: Type.Array(
          Type.Object({ obsId: Type.String(), sessionId: Type.Optional(Type.String()) }),
        ),
      }),
    },
    ({ items }) =>
      askQuery<MemoryCommand>("/user/memory", (replyTo, cancelled) => ({
        _tag: "Expand",
        items,
        replyTo,
        cancelled,
      })),
  );
