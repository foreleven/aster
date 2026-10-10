# Durable Context unification design

Status: implemented, 2026-10-10. This document records the agreed contracts and implementation boundaries.

## Intent

The current Context implementation has three overlapping boundaries:

- DurableContext owns canonical records, revisions, storage, recovery and publication.
- ContextSession adds typed owner validation, owner lifetime, state/message operations and watches.
- ContextRegistry provides another read boundary and applies ContextViewPolicy projections.

The replacement has one core service, DurableContext, and one scoped result of opening a typed owner: ContextHandle<S, M>. Infrastructure remains responsible for Pi documents and native storage. Domain Actors remain responsible for business transitions and scheduling.

Historical storage compatibility is intentionally out of scope. The new document family format is the only supported format; old files are not dual-read or migrated.

## Target boundary

The owner acquires the service and opens one typed handle:

    const durable = yield* DurableContext
    const context = yield* durable.open({
      path,
      state: StateSchema,
      message: MessageSchema,
      keyOf: (message) => message.id,
      serialize: (state) => state.summary?.text ?? "",
      initial: { description, state: initialState },
      events: "state",
      persistence: { layout: "single" },
    })

open validates the partition, restores the committed record, performs bootstrap when needed, and acquires the sole owner lease for the path. ContextSession.make is removed. ContextSessionStorage becomes an internal persistence option owned by DurableContext.open.

DurableContext keeps the service-level read and publication surface:

    open(options) -> Effect<ContextHandle<S, M>, ContextError, Scope>
    get(path) -> ContextText | undefined
    snapshot() -> Readonly<Record<string, ContextText>>
    directory() -> readonly ContextEntry[]
    journal() -> readonly ContextEvent[]
    changes -> Stream<ContextChange>
    subscribe -> Effect<Stream<ContextChange>, never, Scope>
    watch(path) -> Stream<ContextText | undefined, ContextError, Scope>
    serializeState(path) -> Effect<string, ContextError>

The handle keeps the typed owner API: snapshot, current, commit, state.get/set/update/watch, messages.get/list/append/remove/watch, and serializeState. All handle operations use the existing per-path writer and revision checks. Owner callbacks remain pure and short; asynchronous work stays outside the commit callback.

The accepted [append-only Messages contract](context-append-only-messages-design.md) defines duplicate conflicts and atomic summary completion. Duplicate checks cover current members only. Removal retires a member and removes its order entry without retaining identity evidence; reusing a removed key appends a new member.

## Remove views and the second registry boundary

ContextViewPolicy, contextView, restrictedContext, ContextRegistry.views, coreContextViews, and every view option on an owner are removed. ContextRegistry is removed as a service; DurableContext is the read index and publication boundary.

The generic reader contract is ContextText: path, revision, description, and text. The text is the committed result of serialize(state). Generic reads, notifications, System One, and Memory do not receive raw State or pending Messages. There is no projection marker or policy registration. Calls to registry.reader.get, registry.reader.snapshot, registry.views.project, and registry.subscribe move to the text-reading DurableContext methods.

The owner handle retains typed State and append-ordered Messages. Business APIs that need structured information ask the owning Actor through existing domain commands and expose explicit response schemas. Goal/Task/Signal lists and inspections use these domain contracts. Internal admission, eligibility and receipt coordination use explicit schema-decoded committed `readState` reads; they do not wait for another Actor during startup or mailbox processing, preventing cyclic waits. This internal capability is not exposed through generic RPCs or model tools. This preserves structured UI functionality while removing the generic view service. Credentials, live handles, Effects, and transport objects remain outside persisted Context data.

Source events freeze ContextText from the committing revision. The owner option events: none | state controls event capture. For a non-bootstrap state-changing commit, retain an event only when the serialized text changed; private bookkeeping and unchanged summaries therefore do not create new model evidence. Message-only and description-only commits still notify readers but do not create source events. The serializer is evaluated before persistence so an error cannot leave a partially accepted record.

## State serialization

serializeState is a State-only operation. Each Context definition supplies a pure, synchronous serializer:

    serialize: (state: Readonly<S>) => string

The commit engine invokes the serializer on a detached decoded next State and persists the resulting text atomically with that State. The serializer receives State only, not Messages, description, revision, events, or framework metadata. Its output can be plain text, Markdown, or JSON as the domain requires. It does not create a second structured projection. Thrown programming errors remain defects; schema decoding failures remain typed validation errors.

Both context.serializeState and durable.serializeState(path) return the text of the latest committed revision. Keep that derived string in MetadataDoc, alongside description and revision, so dormant reads require no live owner or registered formatter. There is no serializer service, format registry, asynchronous formatting job, or raw JSON fallback. On open, validate the owner data and recompute text with the supplied serializer; when it differs, commit the new text as a bootstrap revision without a source event before returning the handle.

## Persistence and family documents

Infra keeps four session-scoped Pi documents for one physical Context partition:

- StateDoc: { value: JsonValue }
- MessagesDoc: defineDocFamily<{ value: JsonValue }, JsonValue>, keyed by safe message identity
- MetadataDoc: identity, partition, revision, description, serialized State text, and ordered message keys
- EventsDoc: retained source events

Metadata order is authoritative for reconstructing append-ordered Messages. Message family members are immutable after creation: a commit appends new members and retires removed members in the same native transaction as State, Metadata and Events. There is no update operation. No legacy singleton message document is read or retired. Orphaned family members are outside the logical record because metadata is the committed identity set.

ContextPersistence receives one owner configuration from DurableContext.open. save and configureSession collapse into an internal owner binding. Core acquires watches from canonical committed publication after persistence; native early callbacks do not acknowledge durability. The adapter owns native Session handles and closes them with its host Scope.

## Lifecycle and publication

1. open validates the path and partition and acquires the owner lease.
2. Recovery loads the canonical record and validates State and Messages with the supplied schemas.
3. Bootstrap persists initial State and metadata before returning the handle.
4. Handle commits serialize per path, validate data and compute text, persist State, Messages, text and events atomically, then update in-memory records and publish.
5. Scope release closes the handle and releases the owner lease; admitted writes drain and waiting writes remain interruptible.

Handle message watches emit the acquired immutable value and Option.none when the member is retired. A watch of an absent member emits Option.none and completes. The generic watch(path) emits coherent canonical records and is not an acknowledgement of durable source events.

## Migration surface

This is a one-pass API migration:

- Replace ContextSession.make with durable.open.
- Replace ContextRegistry injection and reads with DurableContext.
- Remove every view argument and every view registration.
- Replace public projection types with ContextText contracts.
- Delete view-specific tests and add direct canonical-reader tests.
- Keep domain schemas, keyOf, append order, event mode, daily partition validation, atomic removal, and owner Scope semantics.

The implementation is complete only when no core or integration package imports context/view.ts, ContextRegistry, PublicContext, or ContextSession. Rename internal session change types to ContextMutation so removed facade terminology does not remain in the handle contract. Raw commit/recover/acquire methods become implementation details; owners write through scoped handles. Test setup seeds records through those handles and injected persistence rather than keeping a second public write API.

## Validation

Tests cover typed open/bootstrap, text-only generic reads, structured domain queries, state serialization, dormant serialized reads, text-change source events, atomic State plus Message family commits, append order, conflicts for current duplicate keys, removed-key reuse without historical deduplication, message watch retirement/absent behavior, owner conflicts, daily partitions, cancellation, and uncertain-write recovery. Run pnpm build, pnpm test for the cross-package/runtime migration, pnpm check, and Effect language-service diagnostics for affected packages. Run browser tests when changing structured application contracts consumed by the web app.
