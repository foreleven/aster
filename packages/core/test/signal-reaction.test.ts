import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import { Effect, Layer, Schema } from "effect";
import {
  ContextRegistry,
  SignalDefinitions,
  SignalRootActor,
  type ContextRecord,
  type SignalCommandReply,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";
import { SignalReactionInput } from "../src/signals/reaction.js";
import { SignalState } from "../src/signals/state.js";
const definition = {
  slug: "review",
  trigger: { _tag: "Context" as const, when: "Release changed" },
  task: { _tag: "Goal" as const, target: "/goals/personal", text: "Review evidence" },
};

test("Signal freezes one Task and evidence with its receipt; restart reuses the original envelope", async () => {
  const records = new Map<string, ContextRecord>();
  let input: SignalReactionInput | undefined;
  let first: unknown;
  for (const restart of [false, true])
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* makeContextRegistry({
            loadAll: () => [...records.values()],
            save: (record) => {
              records.set(record.path, structuredClone(record));
            },
          });
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              Layer.succeed(ContextRegistry, registry),
              Layer.succeed(SignalDefinitions, [definition]),
            ),
          );
          const root = yield* system.spawn("signals", SignalRootActor);
          yield* root.ask((replyTo) => ({ _tag: "Ready", replyTo }));
          input ??= {
            requestId: "reaction",
            causationId: "source",
            source: "/system-one",
            target: "/signals/review",
            expectedRevision: registry.get("/signals/review")!.revision!,
            sourceContext: {
              path: "/source",
              revision: 1,
              description: "Evidence",
              state: { summary: "Release delayed" },
              messages: [],
            },
          };
          const send = (value: SignalReactionInput) =>
            root.ask<SignalCommandReply>((replyTo) => ({ _tag: "React", input: value, replyTo }));
          const reply = yield* send(input);
          assert.equal(reply._tag, "Accepted");
          const state = Schema.decodeUnknownSync(SignalState)(registry.get(input.target)!.state);
          assert.equal(state.occurrences.length, 1);
          assert.equal(state.reactionReceipts?.length, 1);
          assert.deepEqual(state.occurrences[0]!.message.evidence, input.sourceContext);
          assert.deepEqual(state.occurrences[0]!.message.task, definition.task);
          if (restart) assert.deepEqual(state.occurrences[0], first);
          first = state.occurrences[0];
          assert.equal(
            (yield* send({ ...input, sourceContext: { ...input.sourceContext, revision: 2 } }))
              ._tag,
            "Rejected",
          );
          assert.equal((yield* send({ ...input, requestId: "stale" }))._tag, "Rejected");
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
});
test("scheduled Signals reject Context reactions", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(SignalDefinitions, [
              {
                ...definition,
                trigger: {
                  _tag: "Schedule",
                  schedule: { type: "once", at: "2099-01-01T00:00:00Z" },
                },
              },
            ]),
          ),
        );
        const root = yield* system.spawn("signals", SignalRootActor);
        yield* root.ask((replyTo) => ({ _tag: "Ready", replyTo }));
        const result = yield* root.ask<SignalCommandReply>((replyTo) => ({
          _tag: "React",
          replyTo,
          input: {
            requestId: "wrong",
            causationId: "source",
            source: "/system-one",
            target: "/signals/review",
            expectedRevision: registry.get("/signals/review")!.revision!,
            sourceContext: { path: "/source", description: "Source", state: {}, messages: [] },
          },
        }));
        assert.equal(result._tag, "Rejected");
        assert.equal(
          Schema.decodeUnknownSync(SignalState)(registry.get("/signals/review")!.state).occurrences
            .length,
          0,
        );
      }),
    ),
  );
});
