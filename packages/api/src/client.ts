import { RpcClient, type RpcGroup } from "effect/rpc";
import type { RpcClientError } from "effect/rpc/RpcClientError";
import type { Effect, Scope } from "effect";
import { ApplicationRpcs } from "./rpc.js";

export type Client = RpcClient.RpcClient.Flat<
  RpcGroup.Rpcs<typeof ApplicationRpcs>,
  RpcClientError
>;

/** The caller supplies the Protocol and owns the client's Scope. */
export const make: Effect.Effect<Client, never, RpcClient.Protocol | Scope.Scope> = RpcClient.make(
  ApplicationRpcs,
  { flatten: true },
);
