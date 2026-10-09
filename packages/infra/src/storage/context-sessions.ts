import { createHash } from "node:crypto";
import { mkdir, open, readdir } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { isDeepStrictEqual } from "node:util";
import { BACKGROUND_CONTEXT, withAbortSignal } from "@earendil-works/chord/context";
import { copyJson, type Context as NativeContext, type JsonValue } from "@earendil-works/chord";
import {
  createSession,
  defineDoc,
  type Session,
  type JsonObject,
} from "@earendil-works/pi-durable";
import { openNodeJsonlStorage } from "@earendil-works/pi-durable/storage/jsonl/node";
import {
  ContextCommitError,
  ContextRecoveryError,
  ContextSessionLayout,
  StoredContext,
  publicJson,
  type ContextPersistence,
} from "@aster/core";
import { Effect, Schema, Semaphore } from "effect";
import type { ContextStore } from "./file-context-store.js";

const MetadataSchema = Schema.Struct({
  path: Schema.String,
  partition: ContextSessionLayout,
  revision: Schema.Int,
  description: Schema.String,
  order: Schema.Array(Schema.String),
});
type Metadata = typeof MetadataSchema.Type;
const checkpointWhen = (
  _value: Readonly<JsonObject>,
  _ops: unknown,
  info: { deltasSinceBase: number },
) => info.deltasSinceBase >= 63;
const StateDoc = defineDoc<{ value: JsonValue }>({
  kind: "aster.context.state",
  version: 1,
  scope: "session",
  initial: () => ({ value: {} }),
  checkpointWhen,
});
const MessagesDoc = defineDoc<{ messages: Record<string, JsonValue> }>({
  kind: "aster.context.messages",
  version: 1,
  scope: "session",
  initial: () => ({ messages: {} }),
  checkpointWhen,
});
const MetadataDoc = defineDoc<{ value: JsonValue }>({
  kind: "aster.context.metadata",
  version: 1,
  scope: "session",
  initial: () => ({ value: {} }),
  checkpointWhen,
});
const EventsDoc = defineDoc<{ events: Record<string, JsonValue> }>({
  kind: "aster.context.events",
  version: 1,
  scope: "session",
  initial: () => ({ events: {} }),
  checkpointWhen,
});

// Prefixing is injective and avoids Chord's reserved object-path segments.
const messageKey = (key: string) => `message:${key}`;
const contextSessionDirectory = (root: string, path: string, partition: ContextSessionLayout) => {
  const owner =
    partition.layout === "daily" ? path.slice(0, -`/days/${partition.date}`.length) : path;
  const directory = join(root, createHash("sha256").update(owner).digest("hex"));
  return partition.layout === "single"
    ? join(directory, "single")
    : join(directory, "days", partition.date);
};

/** Native fsync is necessary to retain directory-entry durability around Pi's file commits. */
const syncDirectory = async (path: string) => {
  const handle = await open(path, "r");
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
};
const ensureDirectory = async (path: string) => {
  const created = await mkdir(path, { recursive: true, mode: 0o700 });
  if (created === undefined) return;
  const parent = dirname(created);
  for (let current = path; ; current = dirname(current)) {
    await syncDirectory(current);
    if (current === parent) break;
  }
};

/** Pi owns Document deltas; core retains revision, projection and publication semantics. */
export const makeContextSessionPersistence = Effect.fn("ContextSessionPersistence.make")(function* (
  fileStore: ContextStore,
  directory: string,
) {
  const root = resolve(directory);
  const gate = yield* Semaphore.make(1);
  const handles = new Map<string, Session>();
  const locations = new Map<
    string,
    { directory: string; metadata: Metadata; record: StoredContext }
  >();
  const close = async () => {
    for (const [path, session] of handles) {
      await session.close(BACKGROUND_CONTEXT);
      handles.delete(path);
    }
  };
  yield* Effect.addFinalizer(() =>
    gate.withPermit(
      Effect.tryPromise({
        try: close,
        catch: (cause) => new ContextRecoveryError({ path: "/", cause }),
      }).pipe(Effect.orDie),
    ),
  );
  const openSession = async (path: string, context: NativeContext) => {
    const existing = handles.get(path);
    if (existing) return existing;
    await ensureDirectory(path);
    const session = createSession(await openNodeJsonlStorage(path, context, { fsync: true }));
    handles.set(path, session);
    return session;
  };
  const load = gate.withPermit(
    Effect.tryPromise({
      try: async (signal) => {
        // Recovery must discard any poisoned native cache before reading authoritative storage.
        await close();
        locations.clear();
        const context = withAbortSignal(signal, BACKGROUND_CONTEXT);
        await ensureDirectory(root);
        const records = new Map(
          fileStore.loadAll().map((record) => [record.snapshot.path, record]),
        );
        const files = await readdir(root, { recursive: true, withFileTypes: true });
        for (const file of files) {
          if (!file.isFile() || file.name !== "main.jsonl") continue;
          const directory = file.parentPath;
          const session = await openSession(directory, context);
          const meta = await session.snapshot(MetadataDoc, context);
          const state = await session.snapshot(StateDoc, context);
          const messages = await session.snapshot(MessagesDoc, context);
          const events = await session.snapshot(EventsDoc, context);
          if (!meta && !state && !messages && !events) continue;
          if (!meta || !state || !messages || !events)
            throw new Error(`Incomplete Context Session: ${directory}`);
          const metadata = Schema.decodeUnknownSync(MetadataSchema)(meta.value);
          if (contextSessionDirectory(root, metadata.path, metadata.partition) !== directory)
            throw new Error(`Context Session identity does not match directory: ${directory}`);
          if (records.has(metadata.path))
            throw new Error(`Multiple storage authorities for ${metadata.path}`);
          const keys = Object.keys(messages.messages);
          if (
            new Set(metadata.order).size !== metadata.order.length ||
            keys.length !== metadata.order.length ||
            metadata.order.some((id) => !Object.hasOwn(messages.messages, messageKey(id)))
          )
            throw new Error(`Invalid Context message index: ${metadata.path}`);
          const record = Schema.decodeUnknownSync(StoredContext)({
            snapshot: {
              path: metadata.path,
              revision: metadata.revision,
              description: metadata.description,
              state: state.value,
              messages: metadata.order.map((id) => messages.messages[messageKey(id)]),
            },
            events: Object.values(events.events)
              .map((event) => Schema.decodeUnknownSync(StoredContext.fields.events.value)(event))
              .sort((a, b) => a.record.revision - b.record.revision),
          });
          records.set(metadata.path, record);
          locations.set(metadata.path, { directory, metadata, record });
        }
        return [...records.values()];
      },
      catch: (cause) => new ContextRecoveryError({ path: "/", cause }),
    }).pipe(Effect.uninterruptible),
  );
  const configureSession: NonNullable<ContextPersistence["configureSession"]> = (path, config) =>
    gate.withPermit(
      Effect.try({
        try: () => {
          const existing = locations.get(path);
          if (existing && !isDeepStrictEqual(existing.metadata.partition, config.partition))
            throw new Error(`Context Session layout changed for ${path}`);
          if (
            existing &&
            !isDeepStrictEqual(
              existing.record.snapshot.messages.map(config.messageKey),
              existing.metadata.order,
            )
          )
            throw new Error(`Context Session message identities changed for ${path}`);
          if (fileStore.loadAll().some((record) => record.snapshot.path === path))
            throw new Error(`Context path ${path} is already owned by file storage`);
        },
        catch: (cause) => new ContextRecoveryError({ path, cause }),
      }),
    );
  const save: ContextPersistence["save"] = (record, config) =>
    gate.withPermit(
      Effect.tryPromise({
        try: async (signal) => {
          const path = record.snapshot.path;
          if (!config) {
            if (locations.has(path))
              throw new Error(
                `ContextSession owner must acquire its Session before writing ${path}`,
              );
            fileStore.save(record);
            return;
          }
          const context = withAbortSignal(signal, BACKGROUND_CONTEXT);
          const directory = contextSessionDirectory(root, path, config.partition);
          const session = await openSession(directory, context);
          const order = record.snapshot.messages.map(config.messageKey);
          if (order.some((key) => !key) || new Set(order).size !== order.length)
            throw new Error("Messages require unique nonempty IDs");
          const metadata: Metadata = {
            path,
            partition: config.partition,
            revision: record.snapshot.revision,
            description: record.snapshot.description,
            order,
          };
          // Normalize optional object properties once; all document placements are strict JSON.
          const state = copyJson(publicJson(record.snapshot.state));
          const values = new Map(
            record.snapshot.messages.map((message, index) => [
              messageKey(order[index]!),
              copyJson(publicJson(message)),
            ]),
          );
          await session.commit(async (tx) => {
            const stateDoc = await tx.doc(StateDoc);
            const messages = await tx.doc(MessagesDoc);
            const meta = await tx.doc(MetadataDoc);
            const events = await tx.doc(EventsDoc);
            if (!isDeepStrictEqual(stateDoc.value, state)) stateDoc.value = state;
            for (const key of Object.keys(messages.messages))
              if (!values.has(key)) delete messages.messages[key];
            for (const [key, value] of values)
              if (!isDeepStrictEqual(messages.messages[key], value)) messages.messages[key] = value;
            meta.value = copyJson(publicJson(metadata));
            // Retain source evidence without rewriting earlier event bodies on every commit.
            for (const event of record.events)
              if (!Object.hasOwn(events.events, event.id))
                events.events[event.id] = copyJson(publicJson(event));
          }, context);
          await syncDirectory(directory);
          locations.set(path, { directory, metadata, record });
        },
        catch: (cause) => new ContextCommitError({ path: record.snapshot.path, cause }),
      }).pipe(Effect.uninterruptible),
    );
  return { load, save, configureSession } satisfies ContextPersistence;
});
