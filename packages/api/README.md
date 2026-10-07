# Application RPC

Effect 4 RPC definitions and protocol-independent server/client implementations.

- `@aster/api`: browser-safe RPC definitions, API response schemas and query invalidation keys.
- `@aster/api/server`: `layer`, requiring AsterRuntime, ContextRegistry, ContextQueries, AgentConversations and RpcServer.Protocol. Handlers construct typed Actor commands, await admission replies and adapt public reads. It never constructs a Runtime.
- `@aster/api/client`: `make`, a scoped Effect requiring RpcClient.Protocol, and its inferred `Client` type. It returns the native flattened RPC client; there is no global client or mandatory UI framework.

`src/rpcs/` defines ContextRpcs, GoalRpcs, TaskRpcs, ApprovalRpcs, ProcessingRpcs, RuntimeRpcs and NotificationRpcs in separate modules alongside their request/response schemas. Query invalidation keys and mapping belong to the notifications module. `src/rpc.ts` merges them into ApplicationRpcs for the shared server/client. All groups are exported from `@aster/api`; operation names and payloads are defined only in their owning module.

Applications supply Protocol, serialization and platform resources. Local/Web use HTTP with NDJSON; integration tests also exercise WebSocket. Web passes `make` to AtomRpc.Service and consumes the notification stream in that same runtime.

Domain schemas remain in `@aster/core/contracts`; core has no dependency on this package. Only the server entry imports core runtime implementations. Shared/client modules do not import or re-export server, Actor internals or Node APIs.

`SubscribeInvalidations` subscribes before sending the initial `all-queries` invalidation. Later frames contain affected query keys. Reconnection starts a new subscription and refreshes state; there is no replay cursor. Slow consumers fail instead of silently losing notifications. Commands are never automatically replayed after transport failure.

See [Reactive application API](../../docs/reactive-api-design.md).
