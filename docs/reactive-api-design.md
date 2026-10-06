# Reactive application API

Status: implemented with Effect `4.0.0`. AtomRpc supplies typed queries and mutations over HTTP; SSE carries query invalidation keys. REST routes and RPC share application operations. Goal conversation reads use `GetGoalTimeline` or `/api/goals/:slug/timeline`; there is no separate history endpoint. The SSE event is now `invalidate`, replacing the old `context` event.

```text
Actor mailbox -> durable Context commit -> ContextChange
                                             |
                          Application query invalidation keys
                                             |
                           GET /api/events (SSE)
                                             |
                    browser AtomRpc runtime / Reactivity
                                             |
                      refresh affected query atoms
                                             |
                      POST /api/rpc -> ApplicationApi
```

## Ownership and Layers

| Location                           | Responsibility                                                                                                                                                             |
| ---------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `packages/api-contracts`           | Browser-safe Schema DTOs, tagged application errors, RpcGroup definitions, query keys and SSE notification Schema. No Node, Actor, storage or core implementation imports. |
| `packages/core/src/runtime/api.ts` | Transport-independent queries, durable command acknowledgements and normalized query invalidations derived from committed Context changes.                                 |
| `apps/local/src/rpc-api.ts`        | RpcGroup handlers delegate to ApplicationApi.                                                                                                                              |
| `apps/local/src/http-api.ts`       | Scoped NodeHttpServer and route composition. Internal legacy-rest, static-assets and http-policy modules own their respective transport responsibilities.                  |
| `apps/local/src/http-events.ts`    | SSE subscription, readiness, heartbeat and bounded buffering.                                                                                                              |
| `apps/web/src/api/client.ts`       | AtomRpc service, query/mutation atoms and manual invalidation.                                                                                                             |
| `apps/web/src/api/events.ts`       | Scoped EventSource bridge and independent telemetry refresh.                                                                                                               |

AsterRuntime continues to construct its own domain services, own root Actors, activate integrations, and manage readiness and shutdown. Externally supplied server Layers remain configuration, stores, models, external agents and integration adapters. The local host owns the HTTP server and supplies NodeFileSystem; it does not assemble core query helpers or domain Actors.

The browser supplies `FetchHttpClient.layer`, `RpcClient.layerProtocolHttp` and `RpcSerialization.layerNdjson`. One root `RegistryProvider` shares the AtomRpc runtime and Reactivity instance. Query values, loading, errors and mutation state come from AsyncResult; React retains only presentation state. The root connection atom owns one EventSource, closed by its Scope. No callback creates a separate Reactivity layer or calls an independent `Effect.runPromise`.

Reactivity is process-local. The server does not need its own Reactivity service merely to relay committed invalidation keys. SSE bridges independent browser registries, including separate tabs. Core's exact ContextChange snapshots and `stateChanged` remain intact for memory capture and domain evaluation; query invalidations do not replace durable events or become another domain event bus.

The dashboard feature components and projections are checked as TypeScript, including `App.tsx`. Existing JavaScript UI primitives expose React prop contracts through JSDoc. `dashboard/model.ts` decodes the fields the UI understands while preserving the generic Context record's raw state. Invalid known fields surface a projection error rather than an unsafe cast. `dashboard/state.ts` owns Context indexes, row selection, counts and approval diagnostics as derived atoms; telemetry ticks do not re-decode unchanged Context messages.

Runtime phase is a closed Schema union. AsterRuntime owns a private `Ref` containing that phase and the last 200 typed runtime events. Event and lifecycle fibers update it; inspection reads an immutable snapshot. Shutdown's stopping phase cannot be overwritten by late readiness completion. This does not change the Actor mailbox's single-writer ownership.

## Contracts and invalidation

`ApplicationRpcs` exposes `ListContexts`, `GetContext`, `ListGoals`, `GetGoalTimeline`, `ListApprovals`, `InspectRuntime`, `SendGoalMessage`, `EndGoal` and `RespondToApproval`. Schemas validate the public DTOs and ApplicationError values. Clients never import ActorRefs or service implementations. Core retains re-exports of the shared domain records.

| Committed change                      | Keys                                |
| ------------------------------------- | ----------------------------------- |
| Any public Context write              | `contexts`, `context:<path>`        |
| Direct Goal Context write             | Also `goals`, `goal-history:<slug>` |
| Approval Context write                | Also `approvals`                    |
| Telemetry refresh                     | `runtime` only                      |
| SSE ready/reconnect or manual refresh | `all-queries`                       |

`contextQueryKeys` defines the mapping once. Core subscribes to successful Context commits, including background Actor writes; handlers are not the sole source of notifications. Failed persistence and unchanged writes emit nothing. Goal history refresh follows the Goal's Context commit after transcript persistence. An independently writable history service would need its own commit notification before exposing such writes.

Query atoms declare `reactivityKeys`. Successful mutation setters receive `{ payload, reactivityKeys }`; AtomRpc invalidates those keys after acceptance. SSE also refreshes other tabs and background changes. Duplicate invalidations are harmless. Failed mutations retain their typed error, do not invalidate success state and are never automatically resubmitted. A timeout with unknown acceptance still requires the user to inspect current state.

The EventSource callback only enqueues notifications into a scoped Effect Stream. `ready` invalidates all queries, `invalidate` decodes `QueryInvalidation` then calls Reactivity.invalidate in the same runtime, and transport errors update connection status while native EventSource reconnects. Invalid protocol data terminates the stream, releases the connection and presents a refresh action. Telemetry has a separate scoped three-second tick; it does not reload Contexts or history.

`api/timeline.ts` retains loaded immutable history pages. Its reactive AtomRpc tail query is independent of the backward pagination cursor: loading an older page does not refetch the tail or previously loaded pages. On invalidation it fetches the latest page, reads backward only until the cached newest entry ID, and merges the new entries by entry ID. A tail update during an older-page request interrupts and restarts that merge with both dependencies intact. Cache updates become visible only after the complete read succeeds; a smaller server count discards a truncated store's stale cache. The component retains its Atom.family bundle with useMemo because the family cache uses weak references; its hooks alone retain only individual atoms.

## Transport lifecycle

The server acquires its Context subscription before emitting `ready`; the subsequent query observes the latest committed state. Reconnect invalidates all queries because this SSE channel has no durable cursor or replay. It must not be used as an audit log.

Each SSE response owns a scoped producer, a 64-frame queue and a heartbeat stream. A full queue fails the response so the browser reconnects and refreshes. Disconnect and HTTP shutdown interrupt subscriptions and await cleanup. Native HTTP socket and EventSource boundaries are isolated; request handlers and background streams stay in Effect.

RPC uses `RpcServer.layerHttp` with explicit `protocol: "http"` and NDJSON serialization. Effect 4.0.0 has no SSE RPC serializer: native GET EventSource frames and the POST RPC protocol are distinct. Loopback binding, same-origin checks and durable mutation acknowledgement remain in place. The host rejects advertised oversized bodies before parsing, and the platform enforces its 32 KiB body limit while reading.

## Future server-side reactive snapshots

Add runtime-owned `ApplicationQueries` only when a consumer needs pushed snapshots. It should own one Reactivity instance, invalidate it from committed changes, and expose scoped `Reactivity.stream(queryEffect, keys)` queries. AsterRuntime constructs it internally; apps/local only provides transports. A new Reactivity layer per request would isolate subscribers from the runtime's invalidations.

AtomRpc stream queries in 4.0.0 use `runtime.pull`: subsequent chunks require pulls and accumulate by default. For endless pushed snapshots, consume the raw RPC Stream through `ApplicationClient.runtime.atom(...)` to retain the latest value and scope its lifetime to the mounted view. Use supported NDJSON HTTP or WebSocket transport explicitly if replacing SSE.

## Verification

Backend tests exercise typed RPC errors, failed mutation submission counts, multiple SSE subscribers, failed/no-op persistence, disconnect cleanup and active request interruption during shutdown. Playwright covers the production HTTP/RPC/SSE path, query-key isolation, telemetry isolation, invalidation during an in-flight read, reconnect refresh, malformed event cleanup/recovery, mutation rejection, history pagination across bursts with request-count assertions, invalidation during an older-page request, and malformed display fields with raw-state inspection. All transports and domain fixtures are local; no live model or external agent is invoked.

## Source references

- [AtomRpc implementation](../repos/effect/packages/effect/src/unstable/reactivity/AtomRpc.ts) and [tests](../repos/effect/packages/effect/test/reactivity/AtomRpc.test.ts)
- [Atom runtime](../repos/effect/packages/effect/src/unstable/reactivity/Atom.ts) and [Reactivity](../repos/effect/packages/effect/src/unstable/reactivity/Reactivity.ts)
- [RPC HTTP server](../repos/effect/packages/effect/src/unstable/rpc/RpcServer.ts), [client](../repos/effect/packages/effect/src/unstable/rpc/RpcClient.ts) and [serialization](../repos/effect/packages/effect/src/unstable/rpc/RpcSerialization.ts)
- [React registry and hooks](../repos/effect/packages/atom/react/src/Hooks.ts)
