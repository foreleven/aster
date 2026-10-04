import { taskExecutionLayer } from "./workflow-fixtures.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import { Effect, Layer, Schema, Stream } from "effect";
import {
  ContextRegistry,
  ExternalAgents,
  SignalDefinitions,
  SignalRootActor,
  makeContextRegistry,
  type ContextRecord,
  type SignalCommandReply,
} from "../src/index.js";
import { SignalReactionInput, SignalReactionReceipt } from "../src/signals/reaction.js";
import { BusinessOutbox } from "../src/notifications/inbox.js";

const definition = {
  slug: "review",
  when: "Release changed",
  task: "Review evidence",
  mode: "confirm" as const,
  agent: "test",
};
const receipts = Schema.Struct({
  reactionReceipts: Schema.optional(Schema.Array(SignalReactionReceipt)),
  occurrences: Schema.optional(Schema.Array(Schema.Unknown)),
});

test("Signal commits a reaction receipt and occurrence together; restart and late retries cannot create another Run", async () => {
  const records = new Map<string, ContextRecord>();
  let loseAck = true;
  let input: SignalReactionInput | undefined;
  let accepted: SignalCommandReply | undefined;
  for (const restart of [false, true]) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* makeContextRegistry({
            loadAll: () => [...records.values()],
            save: (record) => {
              records.set(record.path, structuredClone(record));
              if (
                record.path === "/signals/review" &&
                Schema.decodeUnknownSync(receipts)(record.state).reactionReceipts?.length &&
                loseAck
              ) {
                loseAck = false;
                throw new Error("Occurrence committed before acknowledgement loss");
              }
            },
          });
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              Layer.succeed(ContextRegistry, registry),
              Layer.succeed(SignalDefinitions, [definition]),
              Layer.succeed(ExternalAgents, {}),
              taskExecutionLayer({
                prepare: () => Effect.never,
                ready: () => Effect.succeed(true),
              }),
            ),
          );
          const root = yield* system.spawn("signals", SignalRootActor);
          yield* root.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
          input ??= {
            requestId: "reaction-one",
            causationId: "source-one",
            source: "/system-one",
            target: "/signals/review",
            expectedRevision: registry.get("/signals/review")!.revision!,
            sourceContext: {
              path: "/source",
              revision: 1,
              description: "Source",
              state: { summary: "Release delayed" },
              messages: [],
            },
          };
          const send = (value: SignalReactionInput) =>
            root.ask<SignalCommandReply>((replyTo) => ({ _tag: "React", input: value, replyTo }));
          if (!restart) {
            const changes = yield* registry.subscribe;
            const sending = yield* send(input).pipe(Effect.forkScoped);
            yield* changes.pipe(
              Stream.filter((change) => change.path.startsWith("/signals/review/runs/")),
              Stream.take(1),
              Stream.runDrain,
            );
            assert.equal(sending.pollUnsafe(), undefined);
          }
          const replay = yield* send(input);
          assert.equal(replay._tag, "Accepted");
          if (accepted) assert.deepEqual(replay, accepted);
          accepted = replay;
          const state = Schema.decodeUnknownSync(receipts)(registry.get("/signals/review")!.state);
          assert.equal(state.reactionReceipts?.length, 1);
          assert.equal(state.occurrences?.length, 1);
          const outbox = Schema.decodeUnknownSync(BusinessOutbox)(
            registry.get("/signals/review")!.state,
          ).businessOutbox;
          assert.equal(outbox.length, 1);
          assert.equal(outbox[0]!.kind, "SignalMatched");
          assert.equal(outbox[0]!.causal.rootRequestId, input.causationId);
          assert.equal(outbox[0]!.revision, state.reactionReceipts![0]!.receipt.revision);
          assert.equal(
            Object.keys(registry.snapshot()).filter((path) =>
              path.startsWith("/signals/review/runs/"),
            ).length,
            1,
          );
          const conflict = yield* send({
            ...input,
            sourceContext: { ...input.sourceContext, revision: 2 },
          });
          assert.ok(conflict._tag === "Rejected" && conflict.error.kind === "conflict");
          const stale = yield* send({ ...input, requestId: "stale" });
          assert.ok(stale._tag === "Rejected" && stale.error.kind === "conflict");
          const missing = yield* send({ ...input, target: "/signals/missing" });
          assert.ok(missing._tag === "Rejected" && missing.error.kind === "not-found");
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  }
});

test("a scheduled Signal rejects source reactions without creating an occurrence", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(SignalDefinitions, [
              { ...definition, schedule: { type: "once" as const, at: "2099-01-01T00:00:00Z" } },
            ]),
            Layer.succeed(ExternalAgents, {}),
            taskExecutionLayer({
              prepare: () => Effect.never,
              ready: () => Effect.succeed(true),
            }),
          ),
        );
        const root = yield* system.spawn("signals", SignalRootActor);
        yield* root.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
        const result = yield* root.ask<SignalCommandReply>((replyTo) => ({
          _tag: "React",
          replyTo,
          input: {
            requestId: "reaction",
            causationId: "source",
            source: "/system-one",
            target: "/signals/review",
            expectedRevision: registry.get("/signals/review")!.revision!,
            sourceContext: { path: "/source", description: "Source", state: {}, messages: [] },
          },
        }));
        assert.ok(result._tag === "Rejected" && result.error.kind === "conflict");
        const state = Schema.decodeUnknownSync(receipts)(registry.get("/signals/review")!.state);
        assert.equal(state.occurrences?.length, 0);
        assert.equal(state.reactionReceipts, undefined);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});
