# Reactive application API

The application boundary is `@aster/api`, implemented with Effect `4.0.0`. It exports shared RPC definitions and separate server/client entries. Applications select the Protocol and serialization. Local and Web use HTTP with NDJSON; the same handlers and client are tested with WebSocket. There are no legacy REST business endpoints or separate SSE channel.

```text
app composition -> Runtime Layer + selected transport
                         |
                  @aster/api/server
                   /             \
          Actor commands      domain reads
                |                  |
        durable admission     public projections
                \                  /
                    typed RPC
                        |
                @aster/api/client
                        |
              Web AtomRpc runtime
```

## Ownership

| Owner                           | Responsibility                                                                                                                         |
| ------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `core/runtime`                  | Assemble services, own root Actors, integration activation, readiness and shutdown; expose Actor addressing and native diagnostics.    |
| Core domain modules             | Actor commands, state transitions, deduplication, durable admission and domain read projections.                                       |
| `api/src/rpcs/`                 | RPC definitions and API schemas grouped by module; `api/src/rpc.ts` merges the groups.                                                 |
| `api/src/rpcs/notifications.ts` | Query keys, notification schema and Context-to-query invalidation mapping.                                                             |
| `api/src/server.ts`             | Inject Runtime/domain services, construct commands, ask Actors, translate replies/errors, normalize wire data and serve subscriptions. |
| `api/src/client.ts`             | Construct the native scoped client without selecting a Protocol.                                                                       |
| `apps/local/src/http-api.ts`    | HTTP Protocol, serialization, assets, host policy, Node server and scoped shutdown.                                                    |
| `apps/web/src/api`              | Browser Protocol, AtomRpc queries/mutations, notification consumption and UI connection state.                                         |

AsterRuntime has no `api` property or application facade. Its value exposes `actors` (Actor selection only), `ready` and `inspect`. Its Layer publishes the same ContextRegistry, ContextQueries and AgentConversations instances used by the running domain. Local does not reconstruct these services. `makeApplicationApi`, `ApplicationApi` and `core/runtime/api.ts` are removed.

RPC-only command adapters stay in server. For example, SendGoalMessage trims ingress text, assigns an identity when omitted, selects the Goal root, sends Route/SubmitInput and translates the reply. There is no core sendGoalMessage wrapper. The Goal Actor remains responsible for admission, deduplication, state changes and persistence before acknowledgement. An accepted request is not a completed conversation; a transport timeout does not prove that the operation was cancelled. RPC handlers never automatically replay uncertain submissions.

Core remains independent of api. Domain schemas are imported directly from `@aster/core/contracts`, without compatibility re-exports. The shared/client entries are browser-safe and never import the server entry. API response schemas do not dictate core's internal model.

## Protocol and injection

Server composition:

```ts
import * as ApiServer from "@aster/api/server";

const rpcRoutes = ApiServer.layer.pipe(
  Layer.provide(
    RpcServer.layerProtocolHttp({
      path: "/api/rpc",
      streamBufferSize: 64,
    }),
  ),
  Layer.provide(RpcSerialization.layerNdjson),
);
// Runtime and its published services come from the host's existing Runtime Layer.
```

Client composition:

```ts
import { ApplicationRpcs } from "@aster/api";
import * as ApiClient from "@aster/api/client";

class ApplicationClient extends AtomRpc.Service<ApplicationClient>()("web/ApplicationClient", {
  group: ApplicationRpcs,
  makeEffect: ApiClient.make,
  protocol: RpcClient.layerProtocolHttp({ url: "/api/rpc" }).pipe(
    Layer.provide([FetchHttpClient.layer, RpcSerialization.layerNdjson]),
  ),
}) {}
```

`ApiClient.make` is an Effect value requiring RpcClient.Protocol and Scope, not a function that starts an independent runtime. Ordinary Effect consumers can acquire it in their own Scope. Calls return the operation's typed Effect or Stream. Web's AtomRpc Service owns one client shared by queries, mutations and its notification listener. A shared client does not imply a single HTTP connection.

Changing transports uses Effect's existing Protocol Layers. A WebSocket host supplies RpcServer.layerProtocolWebsocket; its client supplies RpcClient.layerProtocolSocket and a platform WebSocket Layer. No custom Protocol interface or invoker is introduced.

## RPC inventory

Seven module groups in `api/src/rpcs/` are composed with `RpcGroup.merge` into ApplicationRpcs: ContextRpcs, GoalRpcs, TaskRpcs, ApprovalRpcs, ProcessingRpcs, RuntimeRpcs and NotificationRpcs. Together they contain 17 operations: nine queries, seven commands and one stream. Each group is also exported from `@aster/api`; the default server/client use the combined group.

| Area          | Operations                                                          |
| ------------- | ------------------------------------------------------------------- |
| Context       | ListContexts, GetContext, QueryContext                              |
| Goal          | ListGoals, GetGoalTimeline, SendGoalMessage, RetryGoalTurn, EndGoal |
| Task          | InspectTask, CheckTask, RetryTask                                   |
| Approval      | ListApprovals, RespondToApproval                                    |
| Reaction      | InspectProcessing, RecoverProcessing                                |
| Runtime       | InspectRuntime                                                      |
| Notifications | SubscribeInvalidations                                              |

EndGoal retains its existing semantics in this migration. There is no dedicated Signal RPC or agent-execution stream. Actor commands and private Pi records are never sent verbatim to the browser. Goal timeline reads project user/assistant messages; Task inspection applies its existing domain filtering.

## Live queries

SubscribeInvalidations returns a stream of `{ _tag: "Invalidate", keys: string[] }`. Each subscription acquires its Context reader before emitting `["all-queries"]`. This first frame establishes readiness and refreshes current state without a second Ready wire variant. Notifications are not an audit log and have no replay cursor.

| Committed change             | Invalidated keys              |
| ---------------------------- | ----------------------------- |
| Any Context write            | contexts, context:path        |
| Direct Goal Context write    | Also goals, goal-history:slug |
| Approval Context write       | Also approvals                |
| New subscription / reconnect | all-queries                   |
| Independent telemetry tick   | runtime                       |

Notifications follow successful Context commits, including background Actor writes; failed persistence and unchanged writes emit nothing. Goal history invalidation follows the Context commit after Pi transcript persistence. Mutation success also invalidates the initiating UI's keys; failures retain their error and do not trigger success invalidation. Duplicate invalidations are harmless.

Each subscription owns a capture fiber and a 64-entry outgoing queue. Overflow releases the upstream Context subscription immediately and fails the RPC with a typed unavailable error instead of silently dropping keys. HTTP streaming buffers are bounded separately. Disconnect and shutdown release subscription scopes. Web consumes the raw stream continuously, since AtomRpc's stream-query helper has explicit pull semantics.

Web marks a failed connection disconnected, then retries only recoverable subscription failures with an Effect Schedule capped at 30 seconds. The next subscription's first frame refreshes all queries. Malformed protocol data surfaces as an error and can be restarted by manual refresh; defects and interruption are not converted into retryable business errors. Queries and commands are not wrapped in this retry policy. Telemetry retains a separate three-second refresh of the runtime query.

The Atom registry owns client/subscription lifetimes. The notification listener invalidates Reactivity in the same runtime, without an independent runPromise or EventSource bridge. UI state comes from AsyncResult; React retains presentation state. Goal timeline pagination retains immutable older pages, merges invalidated tail reads by entry ID, and handles invalidation during a concurrent page request.

## HTTP lifetime and policy

Local binds loopback, enforces Host/Origin and request body limits, and serves static/source assets alongside `/api/rpc`. HTTP shuts down before Runtime. Active request fibers are interrupted and their finalizers drain before remaining sockets close, including requests arriving after the platform request handler is removed. Runtime then stops sources, consumers and Actors, drains memory capture and releases infrastructure under the process lock.

## Validation

Backend integration tests use injected services and real Actor/HTTP infrastructure, without external models or agents. They cover typed query errors, durable command admission, duplicate identities, non-replayed rejection, invalid payloads, optional-field encoding, Goal pagination, subscription acquisition order, multiple subscribers, disconnect cleanup, failed/no-op persistence, removed endpoints, origin/body policy and request shutdown. The same RPC layer and generated client are exercised over HTTP and WebSocket.

Playwright uses RPC fixtures plus a real HTTP host with fake domain services. It covers desktop/mobile flows, query isolation, subscription recovery, malformed notifications, mutation rejection, timeline pagination during bursts and private-field projections. No runtime data or credentials are modified for validation.

## Source references

- [RpcServer](../repos/effect/packages/effect/src/rpc/RpcServer.ts), [RpcClient](../repos/effect/packages/effect/src/rpc/RpcClient.ts), [serialization](../repos/effect/packages/effect/src/rpc/RpcSerialization.ts)
- [AtomRpc](../repos/effect/packages/effect/src/reactivity/AtomRpc.ts), [Reactivity](../repos/effect/packages/effect/src/reactivity/Reactivity.ts)
- [Upstream transport tests](../repos/effect/packages/platform/node/test/RpcServer.test.ts)
