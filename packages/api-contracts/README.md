# Application API contracts

Browser-safe Effect 4 Schemas and RPC contracts shared by core, the local HTTP host and the web client. This package owns public Context, history, approval and runtime inspection DTOs, ApplicationError, ApplicationRpcs, query keys and SSE invalidation payloads.

It depends only on Effect. Do not import Node APIs, ActorRefs, infrastructure adapters or core implementations here. Core re-exports shared domain records for existing consumers. Query keys describe public reads; they do not introduce another domain event bus.

See [Reactive application API](../../docs/reactive-api-design.md) for ownership, transport and lifecycle semantics.
