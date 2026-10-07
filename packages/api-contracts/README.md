# Application API contracts

Browser-safe Effect 4 RPC contracts, application response schemas, query keys and SSE invalidation payloads.

Domain schemas are owned by `@aster/core/contracts`. This package depends on that pure entry point; core never depends on this package. Consumers import domain schemas directly from core/contracts rather than through compatibility re-exports. The core root entry, ActorRefs, Node APIs, provider handles and storage implementations are not browser contracts.

Task and processing inspection, Goal timeline pagination and runtime telemetry response schemas belong here. Core implements the queries and derives their return types without importing these API schemas. The local host validates runtime inspection at the RPC boundary and maps scoped Context changes to query invalidations. Pi reads and domain filtering remain in core.

See [Reactive application API](../../docs/reactive-api-design.md).
