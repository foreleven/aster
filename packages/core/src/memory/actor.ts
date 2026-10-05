import { ContextCaptures } from "./capture.js";
import { ReplyTo, type ActorContext } from "@aster/actor";
import { Effect, HashSet, Layer, Match, Schema } from "effect";
import { ContextActor } from "../context/actor.js";
import { ContextRegistry } from "../context/registry.js";
import { PublicContext as ContextRecord } from "@aster/api-contracts";
import { defineContext } from "../context/definition.js";
import { type ContextCapture } from "./contracts.js";
import { contextView } from "../context/view.js";
import { MemoryBackend, MemoryCaptureError } from "./contracts.js";

const Capture = Schema.Struct({ sessionId: Schema.String, records: Schema.Array(ContextRecord) });
const MemoryState = Schema.Struct({
  pending: Schema.optional(Schema.Array(Capture)),
  captured: Schema.optional(Schema.Array(Schema.String)),
  status: Schema.Literal("ready"),
  retrieval: Schema.Literals(["bm25", "hybrid"]),
  llm: Schema.optional(Schema.Struct({ provider: Schema.String, model: Schema.String })),
});

export const MemoryCommand = Schema.Union([
  Schema.TaggedStruct("Ready", { replyTo: ReplyTo<void>() }),
  Schema.TaggedStruct("Retry", {}),
  Schema.TaggedStruct("Capture", { input: Capture, replyTo: ReplyTo<void>() }),
  Schema.TaggedStruct("Captured", {
    sessionId: Schema.String,
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: Schema.Void }),
      Schema.TaggedStruct("Failure", { error: Schema.instanceOf(MemoryCaptureError) }),
    ]),
  }),
]);
export type MemoryCommand = typeof MemoryCommand.Type;

export const memoryView = contextView({
  matches: (path) => path === "/memory",
  state: Schema.Struct({
    status: Schema.Literal("ready"),
    retrieval: Schema.Literals(["bm25", "hybrid"]),
    llm: Schema.optional(Schema.Struct({ provider: Schema.String, model: Schema.String })),
  }),
});

export class MemoryActor extends ContextActor.Service<
  MemoryActor,
  MemoryBackend | ContextCaptures
>()("memory/Actor", {
  command: MemoryCommand,
  context: defineContext({
    view: memoryView,
    state: MemoryState,
    message: Schema.Never,
  }),
}) {
  static readonly layer = Layer.effect(
    MemoryActor,
    Effect.gen(function* () {
      const backend = yield* MemoryBackend;
      const captures = yield* ContextCaptures;
      const registry = yield* ContextRegistry;
      // Only the mailbox changes this set. Each Behavior gets a fresh set on recovery.
      let inFlight = HashSet.empty<string>();
      const state = Effect.suspend(() =>
        Schema.decodeUnknownEffect(MemoryState)(registry.get("/memory")!.state),
      ).pipe(Effect.orDie);
      const save = Effect.fn("Memory.save")(function* (patch: Partial<typeof MemoryState.Type>) {
        const current = registry.get("/memory")!;
        yield* registry
          .commit(
            { ...current, state: { ...current.state, ...patch } },
            { expectedRevision: current.revision ?? 0 },
          )
          .pipe(Effect.orDie);
      });
      const admit = Effect.fn("Memory.admit")(function* (input: ContextCapture) {
        const current = yield* state;
        if (
          current.captured?.includes(input.sessionId) ||
          current.pending?.some((pending) => pending.sessionId === input.sessionId)
        )
          return;
        yield* save({
          pending: [
            ...(current.pending ?? []),
            { ...input, records: input.records.map(registry.views.project) },
          ],
        });
      });
      const dispatch = Effect.fn("Memory.dispatch")(function* (
        context: ActorContext<MemoryCommand, MemoryBackend | ContextRegistry>,
      ) {
        for (const input of (yield* state).pending ?? []) {
          if (HashSet.size(inFlight) >= 2) break;
          if (HashSet.has(inFlight, input.sessionId)) continue;
          inFlight = HashSet.add(inFlight, input.sessionId);
          // Re-project recovered payloads as well as new captures. Backend results return
          // through the Behavior-owned mailbox; defects retain Actor supervision semantics.
          yield* context.pipeToSelf(
            backend.capture({ ...input, records: input.records.map(registry.views.project) }),
            (result) => ({ _tag: "Captured", sessionId: input.sessionId, result }),
          );
        }
      });
      return MemoryActor.of({
        started: (context) =>
          Effect.gen(function* () {
            const previous = registry.get("/memory");
            yield* registry
              .commit(
                {
                  path: "/memory",
                  description: backend.description,
                  state: {
                    ...previous?.state,
                    status: "ready",
                    retrieval: backend.retrieval,
                    ...(backend.llm ? { llm: backend.llm } : {}),
                  },
                  messages: [],
                },
                { expectedRevision: previous?.revision ?? 0 },
              )
              .pipe(Effect.orDie);
            yield* context.self.tell({ _tag: "Retry" });
          }),
        receive: (command, context) =>
          Match.value(command).pipe(
            Match.tag("Ready", ({ replyTo }) => replyTo.tell(undefined)),
            Match.tag("Capture", ({ input, replyTo }) =>
              Effect.gen(function* () {
                yield* admit(input);
                // A caller may acknowledge the source handoff only after the queue commit.
                yield* replyTo.tell(undefined);
                yield* dispatch(context);
              }),
            ),
            Match.tag("Captured", ({ sessionId, result }) =>
              Effect.gen(function* () {
                inFlight = HashSet.remove(inFlight, sessionId);
                yield* Match.value(result).pipe(
                  Match.tag("Success", () =>
                    Effect.gen(function* () {
                      const current = yield* state;
                      yield* save({
                        pending:
                          current.pending?.filter((pending) => pending.sessionId !== sessionId) ??
                          [],
                        captured: [...(current.captured ?? []), sessionId],
                      });
                      yield* dispatch(context);
                    }),
                  ),
                  Match.tag("Failure", ({ error }) =>
                    Effect.logError(`Memory capture will retry for ${sessionId}: ${error.message}`),
                  ),
                  Match.exhaustive,
                );
              }),
            ),
            Match.tag("Retry", () =>
              Effect.gen(function* () {
                // Recover the gap between source commit and durable capture admission.
                for (const record of Object.values(registry.snapshot())) {
                  const input = yield* captures.select(record);
                  if (input) yield* admit(input);
                }
                yield* context.pipeToSelf(Effect.sleep("30 seconds"), () => ({ _tag: "Retry" }));
                yield* dispatch(context);
              }),
            ),
            Match.exhaustive,
          ),
      });
    }),
  );
}
