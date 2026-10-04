import { Cause, Layer, Option } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import { AsyncResult, AtomRpc, Reactivity } from "effect/unstable/reactivity";
import { RpcClient, RpcSerialization } from "effect/unstable/rpc";
import { ApplicationRpcs, QueryKeys } from "@aster/api-contracts";

export class ApplicationClient extends AtomRpc.Service<ApplicationClient>()(
  "web/ApplicationClient",
  {
    group: ApplicationRpcs,
    protocol: RpcClient.layerProtocolHttp({ url: "/api/rpc" }).pipe(
      Layer.provide([FetchHttpClient.layer, RpcSerialization.layerNdjson]),
    ),
  },
) {}

export const contextsQuery = ApplicationClient.query("ListContexts", undefined, {
  reactivityKeys: [QueryKeys.all, QueryKeys.contexts],
});
export const runtimeQuery = ApplicationClient.query("InspectRuntime", undefined, {
  reactivityKeys: [QueryKeys.all, QueryKeys.runtime],
});
export const approvalsQuery = ApplicationClient.query("ListApprovals", undefined, {
  reactivityKeys: [QueryKeys.all, QueryKeys.approvals],
});
export const retryGoalTurn = ApplicationClient.mutation("RetryGoalTurn");
export const retryGoalSignal = ApplicationClient.mutation("RetryGoalSignal");
export const sendGoalMessage = ApplicationClient.mutation("SendGoalMessage");
export const sendPersonalMessage = ApplicationClient.mutation("SendPersonalMessage");
export const retryPersonalInput = ApplicationClient.mutation("RetryPersonalInput");
export const requestPersonalApproval = ApplicationClient.mutation("RequestPersonalApproval");
export const respondPersonalApproval = ApplicationClient.mutation("RespondPersonalApproval");
export const resumePersonalRun = ApplicationClient.mutation("ResumePersonalRun");
export const startPersonalTask = ApplicationClient.mutation("StartPersonalTask");
export const applyPersonalSignal = ApplicationClient.mutation("ApplyPersonalSignal");
export const sendPersonalGoalMessage = ApplicationClient.mutation("SendPersonalGoalMessage");
export const endGoal = ApplicationClient.mutation("EndGoal");
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
