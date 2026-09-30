---
status: partially superseded by ADR-0032
---

# Scope Actor lifecycle and put child creation on ActorContext

Each actor instance is created inside its own Effect Scope. A persistent actor recovers before the runtime invokes its optional `started()` hook; only then does mailbox processing begin. Stopping or restarting closes the Scope and runs registered Effect finalizers, so the API does not add a separate imperative `stopped()` cleanup callback.

Child creation belongs to `ActorContext.spawn`, not the Actor base class. The actor accesses `context.self`, its path, child creation and stopping, watching, and `pipeToSelf` through `ActorContext`; message delivery belongs to `ActorRef.tell`. This keeps Actor behavior separate from runtime and hierarchy operations.
