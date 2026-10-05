import { isDeepStrictEqual } from "node:util";
import { isJsonValue, type Context as ChordContext } from "@earendil-works/chord";
import { BACKGROUND_CONTEXT, withAbortSignal } from "@earendil-works/chord/context";
import {
  createSession,
  ROOT_CONVERSATION_ID,
  type ConversationId,
  type Cursor,
  type Session,
  type Storage,
} from "@earendil-works/pi-durable";
import { PiStorageLease, PiExecutionOwner, type PiDurableRuntime } from "@aster/agent";
import { openNodeJsonlStorage } from "@earendil-works/pi-durable/storage/jsonl/node";
import {
  ContextCommitError,
  ContextRecoveryError,
  DurableContext,
  makeDurableContext,
  type ContextRecord,
} from "@aster/core";
import { Effect, Layer, Schema } from "effect";
import {
  PiContextCommit,
  PiContextCommitSchema,
  PiContextDocument,
  PiContextDocumentSchema,
  PiContextIndex,
  PiContextIndexSchema,
} from "./pi-context-documents.js";

/** SDK transactions require Promise callbacks. Schema failures reject that
 * callback before storage admission; the Effect boundary maps them to typed errors. */
export const readPiContextSnapshots = async (
  session: Session,
  shardId: string,
  context: ChordContext,
  sharedExecutionOwner = false,
) => {
  const executionOwner = await session.snapshot(PiExecutionOwner, context);
  if (executionOwner && executionOwner.ownerId !== shardId)
    throw new Error("Pi Context and execution owner identities differ");
  const raw = await session.snapshot(PiContextIndex, context);
  const conversations = await session.commit(async (tx) => {
    const result = new Map<number, ConversationId>();
    let hasUnmappedOwner = false;
    let cursor: Cursor | undefined;
    do {
      const page = await tx.scanConversations({}, 100, cursor);
      for (const conversation of page.items) {
        result.set(conversation.id, conversation.id);
        if (conversation.id !== ROOT_CONVERSATION_ID && !conversation.owner)
          hasUnmappedOwner = true;
      }
      cursor = page.next;
    } while (cursor);
    return { ids: result, hasUnmappedOwner };
  }, context);
  if (!raw) {
    // A dedicated Context shard must not silently adopt an unrelated Pi session.
    if (sharedExecutionOwner ? conversations.hasUnmappedOwner : conversations.ids.size > 0)
      throw new Error("Pi Context index is missing");
    return { records: [], conversations: new Map<string, ConversationId>() };
  }
  const index = Schema.decodeUnknownSync(PiContextIndexSchema)(raw);
  if (index.shardId !== shardId) throw new Error("Pi Context shard identity mismatch");
  const paths = new Set<string>();
  const ids = new Set<number>();
  const records: ContextRecord[] = [];
  const mapped = new Map<string, ConversationId>();
  for (const mapping of index.contexts) {
    if (paths.has(mapping.path) || ids.has(mapping.conversationId))
      throw new Error("Duplicate Pi Context mapping");
    paths.add(mapping.path);
    ids.add(mapping.conversationId);
    // Obtain branded IDs from actual SDK records, never cast persisted integers.
    const conversationId = conversations.ids.get(mapping.conversationId);
    if (conversationId === undefined) throw new Error("Pi Context conversation is missing");
    const snapshot = Schema.decodeUnknownSync(PiContextDocumentSchema)(
      await session.snapshot(PiContextDocument, conversationId, context),
    );
    if (
      snapshot.record.path !== mapping.path ||
      snapshot.record.revision !== mapping.revision ||
      snapshot.entryId !== mapping.entryId
    )
      throw new Error("Pi Context index/document mismatch");
    const latest = await session.commit(async (tx) => {
      let cursor: Cursor | undefined;
      do {
        const page = await tx.scanEntries({ conversationId }, 100, cursor);
        const entry = page.items.find((item) => item.kind === PiContextCommit.kind);
        if (entry) return entry;
        cursor = page.next;
      } while (cursor);
      return undefined;
    }, context);
    const commit = Schema.decodeUnknownSync(PiContextCommitSchema)(latest?.data);
    if (latest?.id !== mapping.entryId || !isDeepStrictEqual(commit.record, snapshot.record))
      throw new Error("Pi Context entry/document mismatch");
    records.push(snapshot.record);
    mapped.set(mapping.path, conversationId);
  }
  return { records, conversations: mapped };
};

export const writePiContextSnapshot = async (
  session: Session,
  shardId: string,
  conversations: ReadonlyMap<string, ConversationId>,
  record: ContextRecord,
  context: ChordContext,
  imported?: { readonly expectedStoredRevision: number | undefined },
) => {
  const data: unknown = structuredClone(record);
  if (!isJsonValue(data)) throw new Error("Pi Context requires JSON state and messages");
  const validated = Schema.decodeUnknownSync(PiContextCommitSchema)({ mappingVersion: 1, record });
  return session.commit(async (tx) => {
    const index = await tx.doc(PiContextIndex);
    if (index.shardId !== "" && index.shardId !== shardId)
      throw new Error("Pi Context shard identity mismatch");
    const mapping = index.contexts.find((item) => item.path === record.path);
    const expected = imported ? imported.expectedStoredRevision : validated.record.revision - 1;
    const actual = imported ? mapping?.revision : (mapping?.revision ?? 0);
    if (actual !== expected)
      throw new Error("Pi Context persisted revision differs from canonical revision");
    let conversationId = conversations.get(record.path);
    if (mapping && conversationId !== mapping.conversationId)
      throw new Error("Pi Context mapping changed outside its owner");
    if (conversationId === undefined)
      conversationId = (await tx.createConversation({ ownership: { kind: "ownerless" } })).id;
    const snapshot = await tx.doc(PiContextDocument, conversationId);
    const entry = await tx.appendEntry(PiContextCommit, conversationId, {
      data: { mappingVersion: 1, record: data },
    });
    snapshot.record = data;
    snapshot.entryId = entry.id;
    index.shardId = shardId;
    const next = {
      path: record.path,
      conversationId,
      revision: validated.record.revision,
      entryId: entry.id,
    };
    if (mapping) Object.assign(mapping, next);
    else index.contexts.push(next);
    return conversationId;
  }, context);
};

export interface PiContextStorageOptions {
  readonly shardId: string;
  /** Open a fresh handle on each call. Caller holds the exclusive storage lease
   * for the enclosing Scope, including recovery and finalization. */
  readonly openStorage: Effect.Effect<Storage, ContextRecoveryError>;
  readonly onCloseFailure?: Effect.Effect<void>;
}

const make = Effect.fn("PiDurableContext.make")(function* (options: PiContextStorageOptions) {
  yield* Schema.decodeUnknownEffect(Schema.NonEmptyString)(options.shardId).pipe(
    Effect.mapError((cause) => new ContextRecoveryError({ path: "/", cause })),
  );
  const open = options.openStorage.pipe(Effect.map(createSession));
  const close = (session: Session) =>
    Effect.tryPromise({
      try: (signal) => session.close(withAbortSignal(signal, BACKGROUND_CONTEXT)),
      catch: (cause) => new ContextRecoveryError({ path: "/", cause }),
    }).pipe(Effect.tapError(() => options.onCloseFailure ?? Effect.void));
  const owner = yield* Effect.acquireRelease(
    Effect.map(open, (session): { session: Session | undefined } => ({ session })),
    (holder) =>
      Effect.suspend(() =>
        holder.session ? close(holder.session).pipe(Effect.orDie) : Effect.void,
      ),
  );
  let poisoned = false;
  let conversations = new Map<string, ConversationId>();
  const load = Effect.gen(function* () {
    if (poisoned) {
      if (owner.session) {
        yield* close(owner.session);
        owner.session = undefined;
      }
      owner.session = yield* open;
    }
    const session = owner.session;
    if (!session)
      return yield* new ContextRecoveryError({ path: "/", cause: "Pi session is closed" });
    const restored = yield* Effect.tryPromise({
      try: (signal) =>
        readPiContextSnapshots(
          session,
          options.shardId,
          withAbortSignal(signal, BACKGROUND_CONTEXT),
        ),
      catch: (cause) => new ContextRecoveryError({ path: "/", cause }),
    });
    conversations = restored.conversations;
    poisoned = false;
    return restored.records;
  }).pipe(Effect.uninterruptible);
  return yield* makeDurableContext({
    load,
    save: (record) =>
      Effect.gen(function* () {
        const session = owner.session;
        if (poisoned || !session)
          return yield* new ContextCommitError({
            path: record.path,
            cause: "Pi shard requires recovery",
          });
        const conversationId = yield* Effect.tryPromise({
          try: (signal) =>
            writePiContextSnapshot(
              session,
              options.shardId,
              conversations,
              record,
              withAbortSignal(signal, BACKGROUND_CONTEXT),
            ),
          catch: (cause) => new ContextCommitError({ path: record.path, cause }),
        }).pipe(
          Effect.tapCause(() =>
            Effect.sync(() => {
              poisoned = true;
            }),
          ),
        );
        conversations.set(record.path, conversationId);
      }),
  });
});

/** Attach to the execution owner's existing Harness. This adapter never opens
 * storage or closes a Session independently; uncertainty retires both consumers
 * through the one owner before reloading authoritative Context snapshots. */
const fromRuntime = Effect.fn("PiDurableContext.fromRuntime")(function* (
  runtime: PiDurableRuntime,
) {
  let uncertain = false;
  let conversations = new Map<string, ConversationId>();
  const load = Effect.gen(function* () {
    if (uncertain) yield* runtime.recover;
    const restored = yield* runtime.withSession((session, context) =>
      readPiContextSnapshots(session, runtime.ownerId, context, true),
    );
    conversations = restored.conversations;
    uncertain = false;
    return restored.records;
  }).pipe(Effect.mapError((cause) => new ContextRecoveryError({ path: "/", cause })));
  return yield* makeDurableContext({
    load,
    save: (record) =>
      Effect.gen(function* () {
        if (uncertain)
          return yield* new ContextCommitError({
            path: record.path,
            cause: "Pi owner requires recovery",
          });
        const id = yield* runtime
          .withSession((session, context) =>
            writePiContextSnapshot(session, runtime.ownerId, conversations, record, context),
          )
          .pipe(
            Effect.mapError((cause) => new ContextCommitError({ path: record.path, cause })),
            Effect.tapCause(() =>
              Effect.sync(() => {
                uncertain = true;
              }),
            ),
          );
        conversations.set(record.path, id);
      }),
  });
});

/** The lock outlives the Session finalizer and is held across poisoned reopen.
 * Native lock I/O is isolated at this resource boundary, like the local host lock. */
const directory = Effect.fn("PiDurableContext.directory")(function* (options: {
  readonly directory: string;
  readonly shardId: string;
}) {
  const lease = yield* PiStorageLease.acquire(options.directory, options.shardId).pipe(
    Effect.mapError((cause) => new ContextRecoveryError({ path: "/", cause })),
  );
  return yield* make({
    shardId: options.shardId,
    onCloseFailure: lease.quarantine,
    openStorage: lease.assertHeld.pipe(
      Effect.mapError((cause) => new ContextRecoveryError({ path: "/", cause })),
      Effect.andThen(
        Effect.tryPromise({
          try: (signal) =>
            openNodeJsonlStorage(
              lease.identity.directory,
              withAbortSignal(signal, BACKGROUND_CONTEXT),
              {
                fsync: true,
              },
            ),
          catch: (cause) => new ContextRecoveryError({ path: "/", cause }),
        }),
      ),
    ),
  });
});

export const PiDurableContext = {
  make,
  fromRuntime,
  directory,
  layer: (options: Parameters<typeof directory>[0]) =>
    Layer.effect(DurableContext, directory(options)),
};
