import { Cause, Layer, Option } from "effect";
import { FetchHttpClient } from "effect/http";
import { AsyncResult, AtomRpc, Reactivity } from "effect/reactivity";
import { RpcClient, RpcSerialization } from "effect/rpc";
import { ApplicationRpcs, QueryKeys } from "@aster/api";
import * as ApiClient from "@aster/api/client";

export class ApplicationClient extends AtomRpc.Service<ApplicationClient>()(
  "web/ApplicationClient",
  {
    group: ApplicationRpcs,
    makeEffect: ApiClient.make,
    protocol: RpcClient.layerProtocolHttp({ url: "/api/rpc" }).pipe(
      Layer.provide([FetchHttpClient.layer, RpcSerialization.layerNdjson]),
    ),
  },
) {}

export const contextsQuery = ApplicationClient.query("ListContexts", undefined, {
  reactivityKeys: [QueryKeys.all, QueryKeys.contexts],
});
export const approvalsQuery = ApplicationClient.query("ListApprovals", undefined, {
  reactivityKeys: [QueryKeys.all, QueryKeys.approvals],
});
export const retryGoalTurn = ApplicationClient.mutation("RetryGoalTurn");
export const sendGoalMessage = ApplicationClient.mutation("SendGoalMessage");
export const respondToApproval = ApplicationClient.mutation("RespondToApproval");
export const invalidateQueries = ApplicationClient.runtime.fn<readonly string[]>()((keys) =>
  Reactivity.invalidate(keys),
);

export const resultValue = <A, E>(result: AsyncResult.AsyncResult<A, E>) =>
  Option.getOrUndefined(AsyncResult.value(result));
export const resultError = <A, E>(result: AsyncResult.AsyncResult<A, E>): string => {
  if (!AsyncResult.isFailure(result)) return "";
  const error = Cause.squash(result.cause);
  return error instanceof Error ? error.message : String(error);
};

export const checkTask = ApplicationClient.mutation("CheckTask");
export const retryTask = ApplicationClient.mutation("RetryTask");
