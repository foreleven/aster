import { listContexts, describeContext, queryContext, readQueryResult } from "./contexts.js";
import { memorySearch, memoryExpand } from "./memory.js";
import type { CoreTool } from "../define.js";
import type { ContextQueries } from "../../queries/registry.js";
import type { QueryExecution } from "../../queries/execution.js";
import type { MemoryRecall } from "../../memory/recall.js";
import type { ToolInvocation } from "../invocation.js";

/** Internal Tasks retrieve evidence; Goal coordination tools remain in the Goal catalogue. */
export const taskTools = (): readonly CoreTool<
  ContextQueries | QueryExecution | MemoryRecall | ToolInvocation
>[] =>
  [
    listContexts(),
    describeContext(),
    queryContext(),
    readQueryResult(),
    memorySearch(),
    memoryExpand(),
  ] as const;
