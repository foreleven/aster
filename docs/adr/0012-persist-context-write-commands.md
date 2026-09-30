---
status: superseded by ADR-0026
---

# Persist Context write Commands, not reads

Every Context Command that can change durable Context state enters a local durable inbox before processing; read-only requests are not persisted. The actor runtime recovers accepted writes after process termination, while Context storage records the exact state and Messages produced by those Commands. This avoids losing accepted mutations without imposing persistence overhead on queries.
