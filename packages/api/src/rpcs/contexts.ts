import {
  ApplicationError,
  PublicContext,
  ContextQueryInput,
  ContextQueryResult,
  ContextQueryError,
} from "@aster/core/contracts";
import { Schema } from "effect";
import { Rpc, RpcGroup } from "effect/rpc";

export const ContextRpcs = RpcGroup.make(
  Rpc.make("ListContexts", { success: Schema.Array(PublicContext), error: ApplicationError }),
  Rpc.make("GetContext", {
    payload: { path: Schema.String },
    success: PublicContext,
    error: ApplicationError,
  }),
  Rpc.make("QueryContext", {
    payload: ContextQueryInput,
    success: ContextQueryResult,
    error: ContextQueryError,
  }),
);
