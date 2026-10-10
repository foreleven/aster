# Unify Context owner access in DurableContext

Accepted during the design interview on 2026-10-10; implemented. DurableContext acquires scoped, typed owner handles through `open(options)`, absorbing the existing ContextSession API and lifecycle while retaining revision checks, atomic persistence, recovery and committed notifications. This removes the separate ContextSession construction path and its dependency on ContextRegistry; concrete Pi storage remains in infra, and Actor owners retain responsibility for domain transitions.

The requested refactor does not preserve historical storage formats. Remove ContextRegistry and generic structured views; Context definitions serialize typed State into reader-visible text. Persist that derived text in the same transaction so dormant reads and frozen source events do not depend on live owner code. Structured business responses remain owned by domain APIs.

Internal coordination decodes committed state with an explicit schema without waiting for another Actor mailbox. Actor-mediated business APIs remain the public structured boundary; generic reads and evidence are text-only. This avoids cyclic startup dependencies while preserving single-owner mutations.
