---
status: superseded by ADR-0026
---

# Use SQLite for the local durable mailbox and Context store

SQLite is the default local database for the durable Command inbox and Context storage. Submitting a write records its Context path, per-Context order, and encoded Command in the inbox; an Effect Queue only wakes an active actor, which reads authoritative work from SQLite. Exact state, Message changes, Summary Checkpoints, and Command completion share SQLite transactions. On restart, unfinished inbox rows are scanned and their Context actors are awakened. This provides local durability and per-Context ordering without distributed RPC or treating an in-memory queue as authoritative.
