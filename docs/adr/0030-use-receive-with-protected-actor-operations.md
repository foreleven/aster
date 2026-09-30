---
status: superseded by ADR-0032
---

# Use receive with protected Actor operations

An Actor subclass implements `receive(command): Effect<void, ActorError>` and expresses runtime actions through protected operations such as `spawn`, `tell`, `pipeToSelf`, and `stop`, rather than returning a separate handler tree. `PersistentActor` additionally exposes `persist` and `persistAll`; after persistence succeeds, the runtime calls the subclass's pure `applyEvent(state, event)` to evolve state. Recovery invokes only `applyEvent`, never `receive`. A restart constructs a fresh Actor instance, avoiding reuse of potentially corrupted fields.
