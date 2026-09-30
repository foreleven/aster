---
status: amended by ADR-0036
---

# Align Context paths with actor supervision

The runtime follows an Akka-style actor hierarchy: the parent Context actor creates, holds, and supervises child actors whose Context paths are beneath its own. A child may stop through ReceiveTimeout and be recreated by its parent when new data arrives. Stopping a parent cascades to its currently running children, but never deletes their SQLite-backed Contexts. Path hierarchy therefore represents both durable Context organization and runtime supervision, accepting tighter lifecycle coupling in exchange for a predictable actor tree.
