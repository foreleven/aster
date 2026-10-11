import { createHash } from "node:crypto";
import { Context, Effect } from "effect";
import type { TaskOrigin } from "../tasks/contracts.js";

/** Bound by one model invocation, captured by its SDK callback scope. */
export class ToolInvocation extends Context.Service<
  ToolInvocation,
  {
    readonly ownerPath: string;
    readonly inputId: string;
    readonly acceptedAt: string;
    readonly remainingAgentTurns: number;
  }
>()("tools/Invocation") {}

export const toolRequestId = Effect.fnUntraced(function* (callId: string) {
  const invocation = yield* ToolInvocation;
  return createHash("sha256")
    .update(JSON.stringify([invocation.ownerPath, invocation.inputId, callId]))
    .digest("hex");
});

export const toolOrigin = Effect.fnUntraced(function* (
  callId: string,
): Effect.fn.Return<TaskOrigin, never, ToolInvocation> {
  const invocation = yield* ToolInvocation;
  return {
    source: invocation.ownerPath,
    requestId: yield* toolRequestId(callId),
    createdAt: invocation.acceptedAt,
    remainingAgentTurns: invocation.remainingAgentTurns,
  };
});
