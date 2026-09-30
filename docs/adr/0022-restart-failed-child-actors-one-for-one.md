---
status: amended by ADR-0034
---

# Restart failed child actors one-for-one

The default supervision directive for an actor failure is one-for-one restart: stop only the failed child scope and reconstruct it from its latest checkpoint plus subsequent persisted Messages. The Command that caused the failure is not automatically replayed; later Commands already queued in the same runtime mailbox may continue after recovery. Restarts use backoff and a consecutive-attempt limit; exhausting the limit stops the child and reports the failure to its parent. The runtime does not resume the existing in-memory actor after a defect because that state may be inconsistent. A registered Context Type may override this strategy.
