# Prune source events after durable Reaction admission

Status: accepted; implementation pending the core refactor.

Context source events currently retain a second unbounded copy of evidence already accepted by the durable Reaction inbox. Reaction will acknowledge source revisions only after saving the evidence or coalescing still-unfrozen queued changes, then the source removes those confirmed event copies; frozen, failed and retryable processing keeps its own evidence. Lost acknowledgments remain replayable and unfinished handoffs retain source evidence. This is an explicit single-consumer handoff rather than a generic event-consumer registry.
