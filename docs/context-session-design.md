# Context owner handles

Status: superseded by the implemented [Context design](context-design.md), 2026-10-10.

Owners use `DurableContext.open(options)` and its scoped typed `ContextHandle`. The separate session facade, session-storage service, registry and generic view policies have been removed. Infrastructure still owns native Pi Sessions; the domain API exposes committed state, immutable append-ordered message operations, atomic commits and watches.

See [Durable Context unification](context-durable-unification-design.md) for persistence and lifecycle, and [append-only messages](context-append-only-messages-design.md) for duplicate conflicts, deletion and summary batching. Historical Context storage formats have no compatibility path.
