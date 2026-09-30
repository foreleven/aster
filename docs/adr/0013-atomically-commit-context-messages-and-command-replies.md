---
status: superseded by ADR-0026
---

# Atomically commit Context changes and Command completion

Context storage and the local durable Command inbox share a transactional boundary. A write Command's exact state changes, appended Context Messages, and completion record either commit together or roll back together. This removes the crash window in which a recovered Command could repeat an already-committed state transition, at the cost of constraining both stores to a compatible transactional backend.
