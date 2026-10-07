# Context design

Context is versioned shared state between an Actor owner and its readers. It validates owner data, commits it durably, projects public views and announces successful commits. It does not select Goals or Signals, execute Tasks, capture Memory or generate descriptions.

## Models

| Model             | Purpose                                                                           |
| ----------------- | --------------------------------------------------------------------------------- |
| `ContextInput`    | `path`, `description`, `state`, `messages`; complete content proposed by an owner |
| `ContextSnapshot` | Input plus required `revision`; detached canonical owner read                     |
| `ContextEntry`    | `path`, `description`; lightweight discovery without copying state or messages    |
| `ContextChange`   | `{ record: ContextSnapshot }`; live notification after a successful commit        |
| `PublicContext`   | Allowlisted state/messages and projection metadata for application/model reads    |
| `ContextEvent`    | `id`, versioned public `record`, `createdAt`; durable source evidence             |
| `StoredContext`   | `{ snapshot, events }`; the same validated model in memory, Local and Pi          |

Context messages can contain source evidence, such as a Lark chat's message window. Goal and Task conversations remain in Pi; Context is not a second Agent transcript store.

`ContextDefinition` provides schema validation, an optional public view and `changes: "none" | "durable-state"`. Omitting `changes` produces no durable source events. Owners supply `expectedRevision` separately from content. Optional `mode: "bootstrap"` suppresses durable source events while still persisting and publishing changed content.

## Organization

| File                         | Responsibility                                                                         |
| ---------------------------- | -------------------------------------------------------------------------------------- |
| `context/model.ts`           | Inputs, snapshots, directory entries, source events, storage schema and event identity |
| `context/definition.ts`      | Owner definitions and allowlisted public projection rules                              |
| `context/registry.ts`        | Registration, owner validation, public reads and view registration                     |
| `context/store.ts`           | DurableContext port, persistence driver contract, ordered commits and recovery         |
| `context/actor.ts`           | ContextActor lifecycle registration and public/Actor path mapping                      |
| `context/errors.ts`          | Typed commit, validation, conflict and recovery errors                                 |
| `context/queries/actor.ts`   | Query Actor and its commands, bounded work and asynchronous result replies             |
| `context/queries/routes.ts`  | Scoped registration and dispatch of integration queries                                |
| `context/queries/results.ts` | Pi query evidence, operation identity checks and result pagination                     |
| `services/actors.ts`         | Shared query replies and cancellation used by Contexts, Memory, Signals and Tools      |
| `json.ts`                    | Shared JSON boundary normalization                                                     |

`ContextActor` wraps an owner Actor definition; it is not a separate Actor or a business-state service. `ContextsActor` is the Runtime-owned `/user/contexts` query endpoint and owns no Context snapshots. Tools ask this endpoint; integrations register query routes that dispatch to their own Actors. Runtime uses native `awaitStarted` for query startup, without a Ready command.

Storage adapters and backend selection remain in infra. Reactions and Memory own independent supervised consumers and policies. Memory evidence reads run outside its mailbox. Runtime installs core views before source startup; integrations install their policies before source activation. Dormant owners remain discoverable through persisted snapshots and registered view policies.

## Reads and commits

Owners use `registry.get/snapshot`. Application queries and model tools use `registry.reader.get/snapshot`; directory search uses `reader.directory`, which returns only paths and descriptions. Public projections never change owner data. Missing or invalid view policies restrict state and messages. `registry.views.project` also projects frozen consumer evidence.

A commit validates the owner schema and expected revision, then persists the next snapshot and source event together. Only successful persistence updates canonical memory and publishes a notification. An unchanged record retains its revision. State changes produce source events only when requested by the definition; message-only and description-only changes still notify readers.

Owners supply `description` directly when creating or updating a Context. It identifies the Context and its purpose; changing activity belongs in state or summary. There is no model-generated description, metadata consumer or separate description-write API.

The store serializes commit and recovery per path. Unrelated Contexts may progress independently. Waiting writers are interruptible; admitted persistence, canonical-state updates and publication drain together. Pi adapters additionally serialize access to their shared Session, including poisoning and reopen, so recovery cannot close a Session while another Context is writing.

Failed or uncertain writes fence their path until registration reconciles authoritative storage. Recovery rejects missing or regressed snapshots and retains original source event identities. Domain owners still serialize business transitions; the store's revision checks and persistence ordering do not replace mailbox/state ownership.

## Durable evidence and storage

Source event IDs derive from the `context-event` namespace, path and committed revision. The storage schema validates event ownership, increasing revisions, snapshot bounds, stable identity and creation time. Event records retain public evidence only. `journal()` supplies durable consumers; `exportRecords()` supplies full records for routed-storage divergence checks and offline backend migration.

Local keeps its fsync-backed pending transaction across state and message files. Pi atomically commits the storage record, entry and index mapping. Both use `{ snapshot, events }`; old flat records, missing revisions and reaction-envelope conversions are unsupported. No runtime data is rewritten by this change.

Live notifications carry newly committed durable events, including events discovered during uncertain-write recovery. System One admits these events directly and scans the journal only at startup. Its persisted per-source revision watermarks prevent replay; queued changes from the same Context coalesce to the latest revision, while in-flight matching retains its frozen evidence. Reactions own target matching, deliveries and receipts. Memory owns capture selection and deduplication. Neither relies on a live source Actor to preserve accepted evidence.

Source journals still grow with retained history and are saved with snapshots. Append-only storage and journal compaction require a separate retention design that accounts for every durable consumer.
