---
status: superseded by ADR-0017
---

# Map each Context path to one Effect Entity

Implement the actor model with Effect 4 RC's `effect/unstable/cluster`: one unified `ContextEntity` type uses the normalized, complete Context path as its entity ID. Channels and message histories are capabilities or data attached to that entity rather than separate entity types. The local application can run these entities with Effect's single-runner topology while retaining typed RPC, mailbox, persistence, and deduplication boundaries; adopting an unstable RC API is an accepted implementation risk.
