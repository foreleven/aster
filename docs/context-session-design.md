# ContextSession design

Status: implemented, 2026-10-09. ContextSession uses the existing ContextRegistry/DurableContext commit chain with a Pi Document persistence adapter. IM Chat, Channel, daily retrieval coverage and Agent admission use Sessions. Other owners retain their current storage. IM uses the new format only; no legacy-data compatibility or import is provided.

## Purpose and boundaries

`ContextSession` is an owner-scoped capability for committed Context data. An Actor acquires it while constructing its Behavior, reads its restored data, and uses it for short durable transitions. It hides document initialization, schema validation, serialization, persistence ordering and recovery. Domain code does not open files, manage Pi document tokens, or maintain a second authoritative Ref.

State and messages are independent data collections. They share one transaction boundary. A successful summary can update state and remove precisely the messages it covered in the same commit.

Use a plain pi-durable Session with session-scoped Documents. Do not create a Harness, conversation, task or conversation entry merely to persist Context data. AgentRunner remains an independent capability for business summarization; model compaction does not decide which source messages have been processed.

| Owner              | Responsibility                                                                                  |
| ------------------ | ----------------------------------------------------------------------------------------------- |
| Core               | Typed ContextSession contract, validation, revision/event rules, registry and public projection |
| Infra              | Pi Session/storage adapter, directories, checkpoints, recovery, discovery and process ownership |
| Integration/domain | State/message schemas, deduplication, message retention, summary policy and date selection      |
| Actor Behavior     | Capability lifetime, mutation scheduling, worker results and generation checks                  |

The Actor package remains domain-neutral. Integrations consume the core capability and never import infra. AsterRuntime assembles it; the host selects infrastructure. This does not replace Goal/Task Agent conversations or their execution durability.

## Data model

Each physical Session holds these session-scoped Documents:

| Document | Content                                                                                           |
| -------- | ------------------------------------------------------------------------------------------------- |
| State    | `{ value: State }`, including private owner bookkeeping                                           |
| Messages | `{ messages: Record<string, Message> }`, the currently retained messages keyed by stable identity |
| Metadata | Format version, identity, description and committed Context revision                              |
| Events   | Retained durable Context source events, including their frozen public evidence                    |

Pi document roots must be JSON objects. The framework Documents have independently versioned schemas. Owner state is validated against its current schema; unsupported formats fail validation. Encode and decode through Effect Schema; Pi's TypeScript generics do not validate application data. Reject non-JSON values, encode optional fields deliberately, and do not serialize live Actor refs, Effects or timers.

The definition supplies `messageKey(message)` and a deterministic `compareMessages(left, right)` with identity as a final tie-breaker. IM uses the source message ID and orders by source timestamp then ID. Public Context messages remain an ordered array. Metadata retains a lightweight ID order index so dormant Contexts are readable before their owner definition is loaded; message bodies are stored only in the keyed Document. The index is rebuilt from the definition's comparator on commit. Domains requiring another order must encode that order in their message schema. Storage maps external IDs to safe, injective object keys, including IDs such as `__proto__`, and verifies key/payload agreement during recovery. A change of message identity is a removal plus insertion.

The storage adapter mutates individual native draft keys, such as `doc.messages[id] = message` and `delete doc.messages[id]`. It never assigns a reconstructed `messages` object for a routine write. Upsert stores only a changed message; removal emits a key deletion. Identical upserts and removal of missing messages are no-ops. Pi may coalesce large batches, and checkpoints intentionally store the complete current document, so this is not a promise that every commit contains only one small operation.

This distinction was checked with the installed Chord 1.1.0 tracker used by pi-durable: with 1,000 messages containing 200-character bodies, native array append emitted 251 bytes of operations and native keyed insertion emitted 249 bytes. Replacing the whole array emitted approximately 224 KB, and replacing the whole record emitted approximately 231 KB. These measure serialized operations, not complete storage files. Arrays also support efficient splice operations; keyed storage is selected for stable identity, edits and selective removal, rather than an assumption that arrays always rewrite their contents.

The State Document contains canonical owner state. `contextView` still defines its public subset and description. Private deduplication or retry fields are not automatically exposed. Messages are source/domain data, not an immutable Agent transcript and not a required event-replay log.

Framework metadata and source events are hidden from the ordinary state/messages API. Owners supply data and commit options; they do not manufacture revisions or event IDs. Updating the Events Document for future retention maintenance must not itself generate another source event.

## Owner interface

The following summarizes the exported API in `packages/core/src/context/session.ts`. `SessionError` stands for typed validation, conflict, persistence, recovery and closed-owner errors.

```ts
interface SessionSnapshot<S, M> {
  readonly state: S;
  readonly messages: Readonly<Record<string, M>>;
  readonly revision: number;
  readonly description: string;
}

interface MessageChanges<M> {
  readonly upsert?: readonly M[];
  // Remove only if the currently stored message equals this expected value.
  readonly removeUnchanged?: readonly M[];
}

interface SessionChange<S, M> {
  readonly state?: S;
  readonly messages?: MessageChanges<M>;
}

interface ContextSession<S, M> {
  readonly state: {
    readonly get: Effect.Effect<S, SessionError>;
    readonly update: (
      change: (state: S) => S,
      options?: CommitOptions,
    ) => Effect.Effect<SessionSnapshot<S, M>, SessionError>;
  };
  readonly messages: {
    readonly get: (id: string) => Effect.Effect<Option.Option<M>, SessionError>;
    readonly list: Effect.Effect<readonly M[], SessionError>;
    readonly upsert: (
      messages: readonly M[],
      options?: CommitOptions,
    ) => Effect.Effect<SessionSnapshot<S, M>, SessionError>;
    readonly removeUnchanged: (
      messages: readonly M[],
      options?: CommitOptions,
    ) => Effect.Effect<SessionSnapshot<S, M>, SessionError>;
  };
  readonly snapshot: Effect.Effect<SessionSnapshot<S, M>, SessionError>;
  readonly commit: (
    change: (current: SessionSnapshot<S, M>) => SessionChange<S, M>,
    options?: CommitOptions,
  ) => Effect.Effect<SessionSnapshot<S, M>, SessionError>;
}
```

`CommitOptions` preserves `mode: "update" | "bootstrap"` and permits an `expectedRevision`. The owner binding supplies the initial description; an optional description in commit options permits an atomic metadata change. Revision checks happen inside serialization. Without an explicit revision, the callback sees the latest committed content. Every write variant uses the same commit engine; `state.update` preserves messages and message operations preserve state. Omitted fields in `SessionChange` are unchanged. No public operation returns a replacement message array for the adapter to assign.

`removeUnchanged` derives each key and compares the expected value against the current canonical schema-encoded message inside the transaction. Missing or changed messages are retained without failing the other removals. Expected values are preconditions, not additional persisted message copies. Reject duplicate operation keys or a key present in both upsert and removal, rather than inventing an implicit operation order. Exact retries are idempotent. The domain still owns generation checks and source-version ordering; upsert must not let an old retrieved version overwrite a newer source version.

For IM, summary completion is one operation (bookkeeping fields omitted here):

```ts
yield *
  session.commit((current) => {
    if (current.state.generation !== completed.generation) return {};
    return {
      state: { ...current.state, summary: completed.summary },
      messages: { removeUnchanged: completed.batch },
    };
  }, options);
```

The batch is the exact frozen input supplied to AgentRunner. A changed message received during that run remains pending. Generation and removal checks use current committed data, not the snapshot from before model execution. The production Chat transition checks generation in the mailbox, clears its active work checkpoint, and selects bootstrap mode if the public summary did not change. Generation in this generic example illustrates the check; it is not a persisted IM field.

The state API deliberately accepts a complete next state for a small structured snapshot. Its adapter may replace the State Document's `value`, so a state change can write that whole state. Do not place growing message bodies in state or claim generic fine-grained state diffs. Large independently changing collections should have their own operation-based Document contract when a concrete use requires one.

Callbacks are short, pure transformations of detached, readonly values. All asynchronous domain work happens outside them. Expected business rejection is represented before committing or through a separately typed domain transition; throwing is not a business error protocol. The adapter validates the returned content and handles schema failures in the Effect error channel. No-op updates retain the revision and do not publish.

`snapshot` reads state, messages and metadata coherently; its record uses logical message IDs and is detached from native storage keys. Separate `state.get`, `messages.get` and `messages.list` calls each read committed data, but may observe different revisions. The facade serializes coherent reads with writes using an Effect Semaphore and reads the committed registry snapshot. It does not rely on Pi's internal `readOnLine` method. Native recovery and storage operations are serialized by the adapter to prevent reopening a Session while another write is admitted.

Acquisition is conceptually:

```ts
const session =
  yield *
  ContextSession.open({
    path,
    definition: ChatContext,
    initial: { description, state: initialChatState, messages: [] },
    persistence: { layout: "single" },
  });
```

Types are inferred from the definition's state and message schemas; the definition also declares message identity and ordering once. Initial messages are accepted as a list, validated for unique IDs and encoded into keyed storage. Initial content is used only for a missing Session; opening existing storage validates and restores it. Empty existing storage, corrupt storage and a new identity must be distinguishable. Acquiring the capability requires the configured persistence service and a Scope.

`ContextActor` binds the session to its existing definition and registry lifecycle, avoiding duplicate schema/view declarations. Registry reads become detached projections of committed Session data. Do not implement Session writes followed by a second `registry.commit`: that would recreate the current dual-write recovery problem. Each owner uses exactly one authoritative persistence path.

## Commit and lifetime guarantees

1. Admit one owner operation; validate ownership and any expected revision.
2. Read the committed documents, apply the pure transition and validate the next content.
3. In one Pi `session.commit`, write changed documents, increment the Context revision, and retain any source event.
4. Only after confirmed persistence, expose the new canonical snapshot and publish the live notification.
5. Return success; only then may a handler acknowledge durable acceptance.

Preserve the current event contract: a definition with `changes: "durable-state"` emits source evidence for canonical state changes outside bootstrap mode. Message-only and description-only changes notify live readers without producing source events. IM uses bootstrap mode for ingestion, receipt retention and flush intent. Gate decisions and retry budgets remain transient. A changed rolling summary uses update mode. An unchanged summary may still retire covered messages, but uses bootstrap mode when private bookkeeping also changes; it must not trigger System One merely because a generation or receipt changed.

The native Session owns document transactions and its committed cache. The Effect adapter serializes access and publishes coherent snapshots; an additional cache, if required for registry reads, is a derived read model rather than another durable authority. No native Session or mutable document handle escapes to the owner. Infra caches native Sessions for its host Scope; Behavior-scoped facades are revoked independently, and the shared cache closes at infrastructure shutdown.

Only one live writer may acquire an identity. Behavior closure revokes its handle, seals admission and drains already admitted persistence before releasing its owner lease. Infrastructure drains and closes native storage at host Scope shutdown. Queued operations remain interruptible; cancellation after storage admission cannot be interpreted as rollback. Reopening after an uncertain write must reconcile persisted metadata and original event IDs before admitting another write. Do not retry the transition blindly. Defects and interruption remain distinct from expected storage failures.

Native Promise/Chord context adaptation belongs in infra. Preserve the calling Effect Context and cancellation boundary there. Native publication listeners only enqueue observations; they must not synchronously call back into the Session. Keep the host storage lock because Pi storage does not provide cross-process ownership.

## Directory policy and daily identity

Directory layout is selected per owner, independently of the physical backend and root directory. Default to `single`. Use the host's resolved durable root, rather than a separate hardcoded `~/.aster/im` root.

```ts
// One continuously updated Session.
{ layout: "single" }

// One explicitly addressed calendar-day Session.
{ layout: "daily", date: "2026-10-09", timeZone: "Asia/Shanghai" }
```

Conceptual layout, with owner paths safely encoded by infra:

```text
<durable-root>/context-sessions/<owner>/
  single/                         # layout: single
    <Pi storage files>
  days/2026-10-09/                 # layout: daily
    <Pi storage files>
  days/2026-10-10/
    <Pi storage files>
```

These are alternative layouts for an owner, not parallel copies of the same state. State, messages, metadata and events for a partition live in the same storage instance. Owner directory names are SHA-256 hashes of the logical owner path. The diagram does not promise literal `state.json` and `messages.json` files: Pi JSONL assigns document sidecars, while SQLite uses database tables.

The date is a validated calendar date selected by the domain from its source interval or message timestamp. It is fixed for the handle's lifetime. Timezone is part of the owner's stable partition policy; reject a conflicting timezone or layout on reopen. A conflicting configuration fails acquisition instead of falling back to empty storage.

Daily layout means independent dated Sessions, not automatic midnight rotation of a continuous state machine. Opening tomorrow does not copy yesterday, clear pending messages, generate a summary, or delete files. Existing work finishing after midnight commits through its original dated handle. A domain may open several explicit dates while finishing accepted work; the storage service does not scan and execute historical business work.

Revision and identity must remain unambiguous. A daily Session is identified by owner plus date. If exposed as a Context, use a date-qualified Context path, for example `/reports/days/2026-10-09`; its revision and source event IDs belong to that path. Reject binding different day partitions to the same undated public path. A continuously addressable `/lark/im/chats/{id}` therefore uses `single`. This preserves existing per-path source-event IDs and consumer watermarks without inventing cross-store atomic revision allocation.

Carrying rolling state and pending work between daily stores would need a durable, idempotent handoff across two Sessions. That is a separate domain workflow and is deliberately unnecessary for the IM implementation. Do not offer a misleading `rotateToday()` operation that silently drops work or resets revisions.

## Discovery, events and cleanup

Storage persists identity metadata with initialization. Infrastructure can enumerate Session locations and read validated metadata/snapshots even when an Actor is dormant. Any discovery index is rebuildable from committed Session metadata; failure to update an index cannot make accepted data permanently invisible. An owner may be restored without executing historical retrieval or summarization. Public policies remain registered independently of a live owner.

Pi watches provide live observations only. Retain source events in the Events Document and expose them through the existing durable journal interface. A crash after commit but before publication is repaired by replay; System One and Memory must not depend on the source Actor being alive. Preserve subscribe/startup replay ordering and consumer deduplication.

Initially retain events, as the existing journal does. Safe pruning requires a durable acknowledgement from every relevant consumer, including a defined policy for disabled consumers and frozen downstream evidence. A watch notification or a successful enqueue is not an acknowledgement. Do not hide this requirement behind a generic `clear()` method.

Message cleanup is an ordinary atomic data update, governed by the owner. Configure `checkpointWhen` explicitly for current-only Documents, for example an initial adapter policy of a full checkpoint after 64 deltas, with size-based tuning considered separately. Deleting message keys alone does not guarantee immediate physical removal of their old revisions.

Verified against installed pi-durable 1.1.0:

- Session-scoped Document checkpoints allow JSONL document sidecars to be replaced by their current base; reclamation is best effort after commit and retried during recovery.
- JSONL `main.jsonl` commit metadata still grows. This design does not claim whole-store bounded disk usage.
- SQLite can delete old current-only document revision rows on checkpoint. Freed pages are reusable; the database file need not shrink.
- `retireDoc` retires a whole document, not selected messages. There is no public API for pruning conversation entries, which this design does not create.

Daily directories make whole-partition retention possible, but no automatic deletion ships with the initial implementation. Deletion requires a closed, completed partition, no pending work, and discharged event/evidence obligations. Routine tests use temporary directories.

The JSONL adapter uses `fsync: true` and additionally syncs newly created directory chains and the Session directory after commit. Admitted native work drains before cancellation or release. Tests verify reopen and uncertain-acknowledgement recovery; they do not simulate physical power loss. The bundled SQLite adapter's WAL `synchronous=NORMAL` is not an equivalent power-loss guarantee. Backend availability does not imply interchangeable durability.

## IM Chat adaptation

Use one continuous Session per chat. Its private state contains chat metadata, rolling summary, source deduplication receipts, retrieval replay boundary and a date-bounded flush. Behavior generation, gate deferrals and retry budgets remain transient. The worker holds its exact selected input; restart reassesses accepted pending evidence with a fresh retry budget. Messages contains pending source messages. The public view exposes chat information, summary and the permitted message evidence.

1. **Accept:** Normalize public evidence, then merge observations by stable message identity and content fingerprint. Atomically persist pending messages, receipts and metadata in bootstrap mode before acknowledging the Channel. Transport-only sender changes do not create a new evidence version.
2. **Assess:** System One judges accumulated evidence within `summary.maxMessages`. Cache explicit deferrals only in the Behavior. Larger input and date-bounded flushes bypass assessment. There is no extra batching timer or persisted assessment.
3. **Run:** Acquire shared IM Agent admission, then read the latest accumulated evidence through the mailbox without another persistence write. Process bounded chronological batches. Capacity errors shrink input; transient failures use finite Effect retries, releasing the permit before waiting. Model execution runs outside any storage transaction.
4. **Apply:** Verify the generation and atomically write the rolling summary, remove only unchanged selected versions, prune eligible receipts and retain the public Context event when appropriate. An edit received during execution remains pending. Await each batch's mailbox commit before acquiring its successor's permit.
5. **Recover:** Resume accepted unfinished work, including messages accepted before midnight. Do not fetch missing historical intervals or resummarize completed historical data. If a model result was lost before commit, a new model run may be required; plain Session does not provide Harness execution recovery.

Retain admission spacing/concurrency and cross-midnight durable handoff ordering. `Flush(date)` covers only pending evidence at or before its date; once covered, newer evidence uses ordinary assessment. Each rolling summary attempt uses one permit. No daily-then-rolling pipeline, daily output checkpoint, Markdown write or prepared multi-store summary commit remains. An optional future Daily Chat Summary is a distinct day-only business result. See [IM summary design](im-summary-design.md) for failure classification and retry limits.

Channel retrieval progress and global Agent admission timing are separate owner Sessions. For a fetched interval, the Channel waits for durable Chat acknowledgements from every affected chat before advancing its cursor. `tell` only confirms enqueueing and is insufficient. A crash after some acknowledgements causes replay and deduplication, not lost ingress. After the cursor is durable, receipt cleanup retains pending evidence and a conservative one-minute replay overlap behind that cursor; a lagging cursor preserves older receipts.

## Verification

Tests cover state/message atomicity, no-op revisions, conflicts, reserved IDs, conditional removal, concurrent mutations, scoped ownership, stale handles, daily partitions, native reopen, uncertain-write recovery and ordinary-write payload growth. Actor tests use fake agents/transports, Deferred and TestClock for admission, deferral, failures, cancellation, supervision, edited messages, accepted prior-day backlog, cursor acknowledgement ordering, catch-up pacing and midnight flushes. Document checkpoint reclamation remains separate from main-log or source-event retention; neither is advertised as bounded total disk usage.
