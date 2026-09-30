---
status: superseded by ADR-0015
---

# Event-source Context state from immutable Messages

The ordered, immutable Messages of a Context are the source of truth for its durable state, including non-secret Channel configuration, Signal definition changes, confirmations, and execution outcomes. A `ContextEntity` rebuilds current state by replaying these Messages and may use discardable snapshots to bound recovery time. Effect's mailbox persistence remains a separate concern that makes Context Commands recoverable; it is not the domain journal. This provides auditability and deterministic recovery at the cost of event schema evolution, projection, and compaction work.
