# Context design

Context is the versioned shared state boundary between an Actor owner and its readers. It validates owner data, commits it durably, projects public views and announces successful commits. It does not decide which Goals or Signals to invoke, what to memorize, or how to generate descriptions.

## Models

| Model             | Fields and purpose                                                                       |
| ----------------- | ---------------------------------------------------------------------------------------- |
| `ContextInput`    | `path`, `description`, `state`, `messages`; complete owner content proposed for commit   |
| `ContextSnapshot` | Input plus required `revision`; detached canonical owner read                            |
| `ContextChange`   | `{ record: ContextSnapshot }`; live commit notification                                  |
| `PublicContext`   | Allowlisted state/messages plus projection metadata; application and model read contract |
| `ContextEvent`    | `id`, versioned public `record`, `createdAt`; durable source evidence                    |
| `StoredContext`   | `snapshot`, `events`; kernel state separated from owner reads                            |

`ContextDefinition` contains schema-generated validation, an optional view and `changes: "none" | "durable-state"`. There is no duplicated identity, capture callback or Signal-specific source flag. Omitting `changes` does not produce durable events. Description identity and Memory selection are consumer policies.

Callers supply `expectedRevision` separately from content. Optional `mode: "bootstrap"` suppresses durable reaction creation for initialization; it still persists and publishes changed content. Owners cannot supply a journal event through the registry commit API.

## File and package boundaries

| Location                                                     | Responsibility                                                                                                |
| ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------- |
| `core/context/model.ts`                                      | Snapshot, input, change and event schemas; stable event identity                                              |
| `core/context/definition.ts`                                 | Schema-backed definition and view policy contracts                                                            |
| `core/context/registry.ts`                                   | Definition registration, validation, public reader, projection registration and description compare-and-swap  |
| `core/context/kernel.ts`                                     | Revision checks, no-op suppression, persistence ordering, detached reads, journal and uncertain-write fencing |
| `core/context/persistence.ts`                                | Durable capability, driver port, stored model and conversion helpers                                          |
| `core/context/storage-format.ts`                             | Legacy disk/Pi envelope validation and compatibility schemas                                                  |
| `core/context/actor.ts`                                      | Actor lifecycle registration and public path mapping                                                          |
| `core/context/view.ts`, `queries.ts`, `json.ts`, `errors.ts` | Generic projection, Context query capability, serialization support and typed errors                          |
| `core/reactions/`                                            | System One screening, phase-specific work, durable delivery, receipts and inspection                          |
| `core/memory/contracts.ts`, `capture.ts`, `actor.ts`         | Memory ports, capture policies and durable queue/deduplication                                                |
| `core/reasoning/context-description.ts`                      | Description identity policies and generation                                                                  |
| `core/tools/context/`                                        | Agent tool adapters                                                                                           |
| `core/runtime/context-*.ts`, `processing.ts`                 | Consumer lifecycle, core policy assembly and diagnostic composition                                           |
| `core/{goals,signals,tasks,delegation,approvals}/view.ts`    | Owner-specific public schemas                                                                                 |
| `core/commands/recovery.ts`                                  | Shared recovery-command receipt validation                                                                    |
| `infra/storage/`                                             | Local/Pi persistence, native storage adapters, routing and migration                                          |
| `core/testing/context.ts`                                    | In-memory test setup, exported through `@aster/core/testing`                                                  |

Integrations own their view, description and capture policies. Context imports no integration or business-owner schemas. Runtime installs core policies, then integrations add their policies before activating sources. The Memory Actor owns accepted capture identities; runtime does not keep an ephemeral duplicate-suppression set.

## Reads and commits

Owners use `registry.get/snapshot` for complete canonical snapshots. Application queries and model tools use `registry.reader.get/snapshot/subscribe`. `registry.views` is the explicit policy-registration and projection capability used during composition and by consumers holding frozen evidence. Missing or invalid views produce restricted public records. Projection never changes stored owner data.

A commit validates owner schemas, compares the observed revision, then persists the complete next snapshot and any source event in one transaction. Only successful persistence changes canonical memory and publishes a notification. An unchanged record preserves its revision. State changes produce an event only when the definition requests durable changes; message-only and description-only changes still notify readers without producing source work.

The kernel creates the event revision and stable ID. The source path and revision live in its public record, and the System One target is implied by the consumer. Owner reads do not expose journals. `DurableContext.journal()` supports durable consumption; `exportRecords()` supports storage migration and authority validation.

Admitted commits drain persistence and publication even when their caller is interrupted. Waiting writers remain interruptible. Failed or uncertain writes fence the path until registration recovers authoritative storage. Recovery rejects missing or regressed snapshots, preserves original journal identities and does not invent a new source event. Domain owners serialize business writes: GoalState owns Goal transitions from mailbox handlers and local tools, while other domain state remains mailbox-owned. ContextRegistry persistence and publication do not introduce another business writer.

## Reaction work

System One scans the journal on startup and live wakeups, so recovery does not require the source Actor to be alive. It persists work before planning or delivery. Work uses phase-specific schemas:

- `pending`: source event and attempt count.
- `planning`: event, attempts and frozen screening input.
- `failed`: the same input plus a failure description for explicit retry.
- `ready`: committed screenings and one or more deliveries.
- `completed`: screenings and terminal deliveries.

Frozen input contains other Context evidence, the Goal catalogue and screening time. The source is retained once in the event and reinserted for screening. Completed work does not retain a redundant full catalogue. Deliveries require a receipt only after delivery, and an error only for unknown or rejected outcomes. Receipt identities, target versions and bounded retry behavior remain unchanged. Unknown external outcomes never authorize a new submission.

## Compatibility

The existing HTTP/RPC `PublicContext` shape is retained, including optional wire revision and the versioned projection marker. Actual canonical snapshots require a revision; unversioned stored snapshots normalize to zero. No browser contract migration is required.

Existing disk/Pi records keep the v1 flat `reactionEvents` envelope. Compatibility conversion reconstructs its source, target, revision and causation fields from the compact model; the `context-reaction-v1` identity namespace stays unchanged. Full exported records, including journals, participate in routed-storage divergence checks.

Legacy System One work is validated before decoding into phase-specific work. Migration preserves source IDs, frozen input and delivery commands/receipts. New writes encode the compact work schema. This is forward read compatibility, not a promise that an older binary can read newly written reaction work. No offline data rewrite or production-data access is part of this refactor. Journal compaction remains unimplemented.
