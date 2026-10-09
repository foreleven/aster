# IM summary design

Status: implemented with [ContextSession](context-session-design.md), 2026-10-09. IM uses the new format only; no old-data compatibility or import is provided.

## Storage and ownership

Each Chat owns one continuous ContextSession. State contains chat metadata, the rolling summary, deduplication receipts (`fingerprint`, source time), the retrieval replay boundary and an optional `flushThrough` date. The separate Messages Document stores pending evidence by message ID. Ordinary writes update individual keys. Actor Behaviors acquire scoped handles; infra owns Pi Sessions, persistence and recovery.

Acceptance normalizes messages to the public evidence schema before saving or fingerprinting them. Transport-only sender fields cannot resurrect already summarized evidence or change an assessment key. Accepted messages, receipts and metadata commit together before the Chat acknowledges ingress.

The public view exposes chat identity, summary and permitted pending evidence. Chat name and purpose contribute to its description. Internal receipts and execution details stay private. Goal relevance receives the public state and description; pending-message changes alone do not initiate screening.

The Channel cursor/readiness and shared Agent admission timing use separate continuous Sessions. Retrieval coverage uses date-qualified daily Sessions in the current business timezone. Daily directories record retrieval coverage; they do not require daily summaries or Markdown archives.

## Retrieval and receipt retention

Startup uses today's business midnight as the retrieval lower bound and resumes a valid saved cursor with overlap. Catch-up windows default to one hour; polling waits fifteen minutes after completion and overlaps the last successful interval by up to one minute. Continuous polling can finish the previous day's tail. Missing historical intervals are not backfilled automatically.

The Channel resolves Chat Actors in its mailbox and awaits durable Update acknowledgements in a scoped worker through `pipeToSelf`. It advances coverage and its cursor only after every affected Chat acknowledges. A crash during handoff replays the interval and deduplicates against receipts, including those for messages already summarized.

After its cursor is durable, the Channel sends receipt-retention boundaries to Chats. Keep receipts for all pending messages and every interval eligible for automatic replay. The cutoff follows the active cursor with a conservative one-minute overlap, rather than a wall-clock today/yesterday rule. A lagging cursor therefore retains older receipts. Summary commits also prune retired receipts using the last saved boundary. Historical import is outside this replay contract.

Startup resumes accepted pending messages, including prior-day evidence, without fetching missing history. Prior-day pending evidence establishes a flush obligation. Successful retrieval of a day's final interval also flushes that day's pending evidence; failed retrieval cannot trigger a flush.

## Summary lifecycle

A Chat owns one in-flight workflow and remembers whether evidence or a flush arrived during it. It has no local summary queue or additional batching timer. `ChatState` exposes evidence, receipt retention, flush and atomic result operations; it does not return scheduling variants. One Behavior-scoped `ChatSummaryWork` owns assessment, shared admission, bounded batching and retries.

1. Read the accumulated evidence through the owning mailbox. Within `summary.maxMessages`, System One judges whether it warrants an update. Cache an explicit defer only in this Behavior; unchanged evidence is not judged again until new evidence arrives. Exceeding the configured message limit, a flush, or a recognized gate input-capacity failure requests summarization directly.
2. Request shared Agent admission once. After admission, an acknowledged `ReadInput` returns the latest accumulated snapshot, including queued arrivals. This read does not write another summary decision or checkpoint.
3. Select all messages when within the limit, otherwise take the oldest chronological batch. AgentRunner updates the rolling summary using Lark's prompt, validated source references and required `save_summary` result tool. Model execution runs outside storage transactions.
4. Send `ApplySummary` to the mailbox and await its acknowledgement. The mailbox checks the Behavior generation, then atomically installs the summary and removes only unchanged selected versions. Edits and arrivals remain pending. An unchanged summary retires covered messages without another source event.
5. Each remaining batch obtains a separate shared admission. Completed batches remain durable if a later batch fails or the Behavior stops. Once the admitted backlog's obligation is covered, later arrivals and edits of covered messages return to ordinary assessment. A failed cycle waits for new evidence or a subsequent flush; duplicate ingress does not restart it.

`Flush(date)` forces only pending evidence at or before that date. Store the latest outstanding date, and clear it once those versions are covered. An admitted batch can also include newer messages. A capacity-reduced flush stops its forced continuation when its date is covered; newer pending messages return to ordinary assessment. Uncovered edits within the obligated date retain the obligation.

## Message limits and failures

Configure `summary.maxMessages` under `contexts./lark.children./im.config`, defaulting to 200. It bounds message count, not tokens: a long individual message or previous summary can still exceed model capacity. Do not silently truncate or drop message content.

- Recognized input-capacity errors halve the selected message count, with a minimum of one. Re-enter ordinary shared admission after every reduction. Keep the reduced count for the remaining batches of this workflow. A single-message failure stops the cycle and preserves evidence with a log error.
- Transient network, rate-limit and service failures keep their selected input unchanged and use Effect retry schedules: at most three retries after the initial attempt, thirty seconds apart. Gate assessment and each selected summary input have their own finite retry budget. SDK retries inside a logical gate call remain separately configured.
- Invalid output, authentication, configuration and unknown failures stop the cycle. Provider diagnostics lack a uniform typed category, so classification recognizes only known capacity/transient diagnostics and treats unknown failures as non-retryable.
- Defects enter Actor supervision. Interruption retains cancellation semantics; neither is converted into an ordinary retry or successful reply.

Shared capacity is released before waiting or reducing a batch. Retry deadlines, consumed retry counts, gate decisions and per-chat errors are not persisted. Errors remain in logs. Restart reconstructs accepted evidence and the saved summary, performs fresh assessment and starts with a fresh retry budget.

## Shared admission and cancellation

`ImAgentQueue.layer` is acquired once by the Lark integration. Its Effect FIFO Queue and fixed worker pool default to two concurrent summary runs and ten seconds between starts across all chats. A `SynchronizedRef` serializes dequeue and start-time persistence; workers remain occupied until their admitted calls complete. Gate assessment runs before admission and has no added IM-wide concurrency limit.

Each chat has at most one admission request. Deferred signals grant admission and release its worker. Model execution runs in a service-owned fiber with the caller's Effect Context; interruption of either owner cancels it. Cancelled queued requests are skipped; cancellation during start-spacing wait releases the wait. Typed failures and defects reach the requesting Actor without retiring a shared worker. The last start is persisted before execution. Restart preserves spacing and rebuilds queue positions.

Behavior closure cancels admission, model work and retry waits. Generation checks reject retired input reads, summary commits and completion messages. Chat Actors remain alive while IM runs. A model result lost before commit may require fresh execution: ContextSession provides data transactions, not Harness execution recovery.

There is no daily-then-rolling pipeline, daily-output checkpoint, prepared multi-store commit or Markdown write. A future daily summary would be a separate business result. Pi checkpoints reclaim current-only Document sidecars; main commit logs and source-event journals retain their independent policies.

## Verification

Tests use fake transports/models, real Actors, Deferred and TestClock. They cover durable handoff, catch-up pacing, midnight and date-bounded flushes, in-memory deferral and restart assessment, queued arrivals, edited versions, canonical fingerprints, cursor-based receipt cleanup, bounded batches, capacity reduction, finite retries, permit release, partial success recovery, cancellation and supervision. Core/infra suites cover atomic state/message commits, conditional removal, scoped ownership, native recovery and incremental storage. No live Lark or model call is required.
