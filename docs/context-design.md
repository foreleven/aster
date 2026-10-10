# Context design

Status: implemented, 2026-10-10.

DurableContext is the single authority for Context ownership, canonical state, messages, revisions, persistence, recovery and committed publication. A domain owner acquires a scoped `ContextHandle<S, M>` with `DurableContext.open(options)`. Generic readers receive `ContextText`: path, revision, description and serialized state text. Domain Actors own business transitions and scheduling.

## Owner API

```ts
const durable = yield * DurableContext;
const context =
  yield *
  durable.open({
    path,
    state: StateSchema,
    message: MessageSchema,
    keyOf: (message) => message.id,
    serialize: (state) => state.summary,
    initial: { description, state: initialState },
    events: "state",
    persistence: { layout: "single" },
  });
```

`open` validates the path and partition, acquires the sole owner lease, restores schema-validated committed data and bootstraps initial state when missing. An owner admitted by a later command can omit `initial`; `current` is then absent until `state.set` creates its first state. No placeholder business state is persisted.

The handle exposes:

- `current` and `snapshot`: coherent, detached typed owner reads.
- `state.get/set/update/watch`: typed state operations; set also initializes a missing record.
- `messages.get/list/append/remove/watch`: immutable observations in append order.
- `commit(change, options)`: atomic state and message mutations.
- `serializeState`: serialized text of the current committed state.

The callback passed to `commit` is pure and short. It returns optional next state and optional `{ append, remove }` message operations. Omitted fields remain unchanged. Options support `expectedRevision`, description and update/bootstrap mode. All operations share one per-path writer. Unrelated Contexts can progress independently.

## Messages and summary

Infra stores each message in a `defineDocFamily` member; metadata stores the ordered retained keys. `keyOf` selects member identity and defaults to the JSON value for small value messages. Identical content under a currently retained key is a no-op; different content fails with `ContextMessageConflict`. Duplicate batch keys and simultaneous append/removal of the same key are invalid. Messages have no update, upsert or sorting operation.

Removing a key retires its member and deletes its order entry. No application tombstone, fingerprint or historical identity record is retained. A removed key may be appended again at the end. An absent-key removal is a no-op. Native storage may retain transaction metadata; logical deletion does not promise instantaneous physical erasure of every historical byte.

A summary owner atomically saves the result and removes the exact covered observation keys:

```ts
yield *
  context.commit((current) => ({
    state: { ...current.state, summary: completed.summary },
    messages: { remove: completed.batch.map(keyOf) },
  }));
```

The domain owns generation checks and in-flight batch identity. IM uses source message IDs, ignores IDs already retained, and appends replayed IDs again after deletion. It retains no historical message deduplication records. Chronological batching is a domain transformation of a detached list.

## Reader boundary

`get`, `snapshot`, `directory`, `watch`, `changes`, `subscribe` and `serializeState(path)` read the committed index. Text is the persisted result of the owner's pure `serialize(state): string`; it receives no messages or framework metadata. Dormant reads require no live owner or registered serializer. Reopening with changed formatting bootstraps a new revision without a source event.

There is no generic view policy, projection metadata or second registry service. Generic RPCs, model context evidence and Memory read text. Goal, Task, Signal and approval business APIs expose explicit schemas and query their owning Actors. Private provider handles and native conversation frames remain outside their public business responses.

Internal domain coordination uses `readState(path, schema)` to decode committed state without waiting for another Actor's startup or mailbox. This prevents cyclic startup and cross-mailbox waits during admission and receipt reconciliation. It is read-only, requires an explicit schema, is not exposed by generic transport APIs, and does not grant another domain a writer. `readOwnerSnapshot(path, { state, message })` additionally reads one coherent detached committed State/Messages revision for internal recovery. Missing records return undefined; invalid retained data fails validation. Neither read acquires a writer lease, publishes changes or activates an Actor. Business query handlers select their explicit response fields. Owners still perform mutations through their handle or Actor-local state service.

## Durability and lifecycle

A commit validates the revision and schemas, computes text, then persists State, immutable message members, ordered metadata and source events in one native transaction. Only confirmed persistence updates canonical memory, publishes a notification and returns success. A no-op retains the revision. Goal and Task stores update their committed Ref only after this handoff.

`events: "state"` retains a frozen text source event only when state and its serialized text change outside bootstrap. Message-only, description-only and private bookkeeping changes notify live readers without creating model evidence. Event identity derives from path and committed revision. Durable consumers replay retained events after restart; live notifications carry only newly committed events.

Scope closure revokes the owner handle and releases its lease after admitted writes drain. Waiting operations remain interruptible. An uncertain persistence failure fences writes until reopen reconciles authoritative data and original event identities; it does not blindly rerun a transition. Infra owns native Sessions and closes them at host shutdown.

Watches subscribe and hydrate under the same path writer as commits. Generic watches return coherent text snapshots. State watches emit distinct committed states. A message watch emits `Some(value)`, including nullable values, then `None` and completes when removed. An absent member emits `None` and completes. Owner watches terminate when their owner scope ends; retired handle operations fail with `ContextClosed`.

## Storage and organization

`context/store.ts` owns the service, leases and canonical commit engine; `handle.ts` owns the typed owner contract; `model.ts` owns storage and event schemas; `contracts.ts` owns browser-safe text and query contracts. `ContextActor` owns scoped command catalogues and path addressing, not snapshots. Context query routing and retained query evidence remain independent of state persistence.

Configured infra uses native Pi Documents under the resolved durable root's `context-sessions/`: State, Messages family, Metadata and Events. Historical formats are unsupported; there is no dual reader or file fallback. The standalone file adapter remains an explicit alternate adapter for tests/hosts, not a runtime compatibility route.

Owners choose continuous `single` or explicit `daily` partitions with validated date/timezone and date-qualified Context paths. Daily handles never rotate or copy state automatically. The host retains its process ownership lock. JSONL uses fsync and directory syncing; tests prove reopen and uncertain acknowledgement recovery, not physical power-loss behavior. Source event pruning requires a separate consumer acknowledgement policy.

See [unification](context-durable-unification-design.md), [messages](context-append-only-messages-design.md) and [IM summary](im-summary-design.md).
