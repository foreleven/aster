---
status: superseded by ADR-0032
---

# Separate volatile and persistent Actor base classes

The actor module exposes a domain-neutral `Actor<Command>` base class and a `PersistentActor<Command, Event, State>` subclass. Ordinary actors use only local mailbox and lifecycle behavior; persistent actors add a stable persistence identity, Schema-defined events and state, recovery, snapshots, and compaction hooks. This avoids forcing journal semantics onto guardians, temporary reply actors, and runtime coordinators while giving Context actors a dedicated persistent foundation.
