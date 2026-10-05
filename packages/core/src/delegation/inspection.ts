import { ApplicationError, DelegationInspection, DelegationPath } from "@aster/api-contracts";
import { Effect, Schema } from "effect";
import type { PublicContext as ContextRecord } from "@aster/api-contracts";
import { DelegationState } from "./state.js";

export const inspectDelegation = Effect.fn("Delegation.inspect")(function* (
  path: string,
  read: (path: string) => Effect.Effect<ContextRecord, ApplicationError>,
): Effect.fn.Return<DelegationInspection, ApplicationError> {
  yield* Schema.decodeUnknownEffect(DelegationPath)(path).pipe(
    Effect.mapError(
      () => new ApplicationError({ kind: "invalid-input", message: "Invalid Delegation path" }),
    ),
  );
  const record = yield* read(path);
  const state = yield* Schema.decodeUnknownEffect(DelegationState)(record.state).pipe(
    Effect.mapError(
      () =>
        new ApplicationError({
          kind: "unavailable",
          message: "Delegation state cannot be inspected",
        }),
    ),
  );
  return {
    path,
    revision: record.revision ?? 0,
    runPath: state.request.runPath,
    agent: state.request.agent,
    status: state.status,
    instructions: state.request.task.instructions,
    sources: [...new Set(state.request.task.input.flatMap((item) => item.sources))],
    hasExecution: !!state.session,
    ...(state.result === undefined ? {} : { result: state.result }),
    ...(state.error === undefined ? {} : { error: state.error }),
    ...(state.resumptions
      ? {
          resumptions: state.resumptions.map((item) => ({
            requestId: item.input.requestId,
            status: item.status,
          })),
        }
      : {}),
    requests: Object.entries(state.requests).map(([id, request]) => ({
      id,
      kind: request.kind,
      prompt: request.prompt,
      responseStatus: state.responses[id]?.status ?? "pending",
    })),
  };
});
