import { listContexts, describeContext, queryContext, readQueryResult } from "./contexts.js";
import { memorySearch, memoryExpand, memorySave } from "./memory.js";
import type { CoreTool } from "../../shared/define-tool.js";
import type { ContextQueries } from "../../../queries/registry.js";
import type { QueryExecution } from "../../../queries/execution.js";
import type { MemoryRecall } from "../../../memory/recall.js";
import type { CurrentActors } from "../../../services/actors.js";
import type { ToolInvocation } from "../../shared/invocation.js";

/** Internal Tasks retrieve evidence; Goal coordination tools remain in the Goal catalogue. */
export const taskTools = (): readonly CoreTool<
  ContextQueries | QueryExecution | MemoryRecall | CurrentActors | ToolInvocation
>[] =>
  [
    listContexts(),
    describeContext(),
    queryContext(),
    readQueryResult(),
    memorySearch(),
    memoryExpand(),
    memorySave(),
  ] as const;
