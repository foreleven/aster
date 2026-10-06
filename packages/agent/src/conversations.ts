import { createHash } from "node:crypto";
import { homedir } from "node:os";
import { join } from "node:path";
import { isDeepStrictEqual } from "node:util";
import { BACKGROUND_CONTEXT, withAbortSignal } from "@earendil-works/chord/context";
import { isJsonValue, type Context as NativeContext } from "@earendil-works/chord";
import { createModels } from "@earendil-works/pi-ai";
import {
  createRegistry,
  defineDoc,
  Harness,
  MemoryStorage,
  type Cursor,
  type EntryId,
  type EntryRecord,
  type Storage,
  type Submission,
  type HarnessSettings,
} from "@earendil-works/pi-durable";
import { openNodeJsonlStorage } from "@earendil-works/pi-durable/storage/jsonl/node";
import {
  Clock,
  Config,
  Context,
  Data,
  Effect,
  Exit,
  Layer,
  Schema,
  Scope,
  Semaphore,
  Ref,
} from "effect";
import { PiStorageLease } from "./pi-storage-lease.js";

export class ConversationError extends Data.TaggedError("ConversationError")<{
  readonly kind: "conflict" | "unavailable" | "invalid-input" | "not-found";
  readonly message: string;
  readonly cause?: unknown;
}> {}

export const ConversationEntry = Schema.Struct({
  id: Schema.Int,
  requestId: Schema.String,
  kind: Schema.String,
  data: Schema.Unknown,
  at: Schema.String,
});
export type ConversationEntry = typeof ConversationEntry.Type;
const StoredEntry = Schema.Struct({
  requestId: Schema.String,
  kind: Schema.String,
  data: Schema.Unknown,
  at: Schema.String,
});
const decodeEntry = (entry: EntryRecord): ConversationEntry => ({
  ...Schema.decodeUnknownSync(StoredEntry)(entry.data),
  id: entry.id,
});
const Index = defineDoc<{ requests: Record<string, number> }>({
  kind: "app.aster.messages",
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  initial: () => ({ requests: {} }),
});
export interface ConversationDriver {
  readonly harness: Harness;
  readonly registry: ReturnType<typeof createRegistry>;
  readonly models: ReturnType<typeof createModels>;
  readonly assertAvailable: Effect.Effect<void, ConversationError>;
  readonly exclusive: <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | ConversationError, R>;
  readonly quarantine: Effect.Effect<void>;
  readonly settings: { compaction?: HarnessSettings["compaction"] };
  /** The runner owns registration and drains every accepted submission before releasing callbacks. */
  steering?: (requestId: string, text: string, context: NativeContext) => Promise<Submission>;
}

/** One scoped writer per conversation. Message admission never waits for model execution. */
export class AgentConversations extends Context.Service<
  AgentConversations,
  {
    readonly append: (
      owner: string,
      requestId: string,
      kind: string,
      data: unknown,
    ) => Effect.Effect<ConversationEntry, ConversationError>;
    readonly steer: (
      owner: string,
      requestId: string,
      text: string,
    ) => Effect.Effect<boolean, ConversationError>;
    readonly read: (
      owner: string,
    ) => Effect.Effect<readonly ConversationEntry[], ConversationError>;
    readonly tools: (
      owner: string,
    ) => Effect.Effect<
      readonly { id: number; kind: string; text: string; at: string }[],
      ConversationError
    >;
    readonly get: (
      owner: string,
      id: number,
    ) => Effect.Effect<ConversationEntry, ConversationError>;
    /** SDK boundary used by AgentRunner, never by business workflows. */
    readonly driver: (owner: string) => Effect.Effect<ConversationDriver, ConversationError>;
  }
>()("agent/Conversations") {
  static readonly make = Effect.fn("AgentConversations.make")(function* (
    options: {
      readonly root?: string;
      readonly openStorage?: (owner: string, context: NativeContext) => Promise<Storage>;
    } = {},
  ) {
    const scope = yield* Scope.Scope;
    const lock = yield* Semaphore.make(1);
    const owners = new Map<string, ConversationDriver>();
    const root = options.root ?? join(homedir(), ".aster", "conversations");
    const driver = Effect.fn("AgentConversations.owner")(function* (owner: string) {
      if (!owner)
        return yield* new ConversationError({
          kind: "unavailable",
          message: "Conversation owner is required",
        });
      const previous = owners.get(owner);
      if (previous) {
        yield* previous.assertAvailable;
        return previous;
      }
      const resourceScope = yield* Scope.fork(scope);
      const resource = yield* Effect.gen(function* () {
        const directory = join(root, createHash("sha256").update(owner).digest("hex"));
        const lease = options.openStorage
          ? undefined
          : yield* PiStorageLease.acquire(directory, owner).pipe(
              Effect.mapError(
                (cause) =>
                  new ConversationError({
                    kind: "unavailable",
                    message: "Cannot acquire conversation writer",
                    cause,
                  }),
              ),
            );
        const turnLock = yield* Semaphore.make(1);
        const quarantined = yield* Ref.make(false);
        const assertAvailable = Effect.gen(function* () {
          if (yield* Ref.get(quarantined))
            return yield* new ConversationError({
              kind: "unavailable",
              message: "Conversation writer requires reconciliation",
            });
          if (lease)
            yield* lease.assertHeld.pipe(
              Effect.mapError(
                (cause) =>
                  new ConversationError({
                    kind: "unavailable",
                    message: "Conversation writer is closed",
                    cause,
                  }),
              ),
            );
        });
        const registry = createRegistry();
        const models = createModels();
        const settings: ConversationDriver["settings"] = {};
        const harness = yield* Effect.acquireRelease(
          Effect.tryPromise({
            try: async (signal) => {
              const context = withAbortSignal(signal, BACKGROUND_CONTEXT);
              const storage = options.openStorage
                ? await options.openStorage(owner, context)
                : await openNodeJsonlStorage(directory, context, { fsync: true });
              try {
                return await Harness.open(storage, { registry, models, settings }, context);
              } catch (cause) {
                await storage.close(BACKGROUND_CONTEXT);
                throw cause;
              }
            },
            catch: (cause) =>
              new ConversationError({
                kind: "unavailable",
                message: "Cannot open conversation",
                cause,
              }),
          }),
          (harness) =>
            Effect.tryPromise({
              try: () => harness.close(BACKGROUND_CONTEXT),
              catch: (cause) =>
                new ConversationError({
                  kind: "unavailable",
                  message: "Cannot drain conversation writer",
                  cause,
                }),
            }).pipe(
              Effect.tapError(() => lease?.quarantine ?? Effect.void),
              Effect.orDie,
            ),
        );
        return {
          harness,
          registry,
          models,
          settings,
          assertAvailable,
          exclusive: <A, E, R>(effect: Effect.Effect<A, E, R>) =>
            turnLock.withPermit(Effect.andThen(assertAvailable, effect)),
          quarantine: Ref.set(quarantined, true).pipe(
            Effect.andThen(lease?.quarantine ?? Effect.void),
          ),
        };
      }).pipe(
        Effect.provideService(Scope.Scope, resourceScope),
        Effect.onExit((exit) =>
          Exit.isFailure(exit) ? Scope.close(resourceScope, exit) : Effect.void,
        ),
      );
      owners.set(owner, resource);
      return resource;
    }, lock.withPermit);
    const access = <A>(
      owner: string,
      use: (driver: ConversationDriver, context: NativeContext) => Promise<A>,
    ) =>
      driver(owner).pipe(
        Effect.flatMap((resource) =>
          Effect.tryPromise({
            try: (signal) => use(resource, withAbortSignal(signal, BACKGROUND_CONTEXT)),
            catch: (cause) =>
              cause instanceof ConversationError
                ? cause
                : new ConversationError({
                    kind: "unavailable",
                    message: "Conversation operation failed",
                    cause,
                  }),
          }),
        ),
      );
    const read = (owner: string) =>
      access(owner, async ({ harness }, context) => {
        const conversation = await harness.root(context);
        const entries: ConversationEntry[] = [];
        let cursor: Cursor | undefined;
        do {
          const page = await conversation.entries({}, 100, cursor, context);
          for (const entry of page.items) {
            if (entry.kind === "app.aster.message") entries.push(decodeEntry(entry));
          }
          cursor = page.next;
        } while (cursor);
        return entries.reverse();
      });
    return AgentConversations.of({
      driver,
      read,
      tools: (owner) =>
        access(owner, async ({ harness }, context) => {
          const conversation = await harness.root(context);
          const records: { id: number; kind: string; text: string; at: string }[] = [];
          let cursor: Cursor | undefined;
          do {
            const page = await conversation.entries({}, 100, cursor, context);
            for (const entry of page.items) {
              for (const message of entry.model ?? []) {
                const at = new Date(message.timestamp).toISOString();
                if (message.role === "assistant") {
                  const calls = message.content.filter((part) => part.type === "toolCall");
                  if (calls.length)
                    records.push({
                      id: entry.id,
                      kind: "tool-call",
                      text: JSON.stringify(
                        calls.map((call) => ({ name: call.name, arguments: call.arguments })),
                      ),
                      at,
                    });
                } else if (message.role === "toolResult")
                  records.push({
                    id: entry.id,
                    kind: "tool-result",
                    text: JSON.stringify({
                      name: message.toolName,
                      content: message.content,
                      isError: message.isError,
                    }),
                    at,
                  });
              }
            }
            cursor = page.next;
          } while (cursor);
          return records.reverse();
        }),
      steer: (owner, requestId, text) =>
        access(owner, async (driver, context) => {
          // The callback synchronously registers the submission before its first await.
          // An idle owner returns false; only the Task mailbox starts a new turn.
          if (!driver.steering) return false;
          await driver.steering(requestId, text, context);
          return true;
        }),
      get: (owner, id) =>
        access(owner, async ({ harness }, context) => {
          const conversation = await harness.root(context);
          return harness.commit(async (tx) => {
            // Entry IDs are numeric in Aster; restore the SDK brand only at this boundary.
            const entry = await tx.entry(id as EntryId);
            if (entry?.conversationId !== conversation.id || entry.kind !== "app.aster.message")
              throw new ConversationError({
                kind: "not-found",
                message: "Conversation entry not found",
              });
            return decodeEntry(entry);
          }, context);
        }),
      append: (owner, requestId, kind, data) =>
        Effect.gen(function* () {
          const at = new Date(yield* Clock.currentTimeMillis).toISOString();
          return yield* access(owner, async ({ harness }, context) => {
            if (!isJsonValue(data))
              throw new ConversationError({
                kind: "invalid-input",
                message: "Conversation data must be JSON",
              });
            const conversation = await harness.root(context);
            return harness.commit(async (tx) => {
              const index = await tx.doc(Index, conversation.id);
              const key = createHash("sha256").update(requestId).digest("hex");
              const id = index.requests[key];
              if (id !== undefined) {
                const saved = await tx.entry(id as EntryId);
                if (saved?.conversationId !== conversation.id || saved.kind !== "app.aster.message")
                  throw new ConversationError({
                    kind: "unavailable",
                    message: "Message reference is missing",
                  });
                const previous = decodeEntry(saved);
                if (
                  previous.requestId !== requestId ||
                  previous.kind !== kind ||
                  !isDeepStrictEqual(previous.data, data)
                )
                  throw new ConversationError({
                    kind: "conflict",
                    message: "Message identity belongs to another payload",
                  });
                return previous;
              }
              const value = { requestId, kind, data, at };
              const entry = await tx.appendEntry(conversation.id, {
                kind: "app.aster.message",
                data: value,
              });
              index.requests[key] = entry.id;
              return { ...value, id: entry.id };
            }, context);
          });
        }).pipe(Effect.uninterruptible),
    });
  });
  static readonly makeMemory = () =>
    AgentConversations.make({ openStorage: async () => new MemoryStorage() });
  static readonly memory = Layer.effect(AgentConversations, AgentConversations.makeMemory());
  static readonly layer = Layer.effect(
    AgentConversations,
    Effect.gen(function* () {
      const root = yield* Config.String("config.durable.root").pipe(
        Config.withDefault(join(homedir(), ".aster")),
      );
      return yield* AgentConversations.make({ root: join(root, "conversations") });
    }),
  );
}
