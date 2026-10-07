import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import { Effect, Layer } from "effect";
import {
  ContextRegistry,
  SignalDefinitions,
  SignalRootActor,
  type ContextRecord,
  type SignalCommandReply,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";
import { SignalReactionInput } from "../src/signals/protocol.js";
import { AgentConversations } from "@aster/agent";
import { testConversations } from "./conversation-fixtures.js";
import { readSignalHistory } from "../src/signals/state/store.js";
const definition = {
  slug: "review",
  trigger: { _tag: "Context" as const, when: "Release changed" },
  task: { _tag: "Goal" as const, target: "/goals/personal", text: "Review evidence" },
};

test("Signal freezes one Task and evidence with its receipt; restart reuses the original envelope", async () => {
  const records = new Map<string, ContextRecord>();
  const messages = testConversations();
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
              Layer.succeed(AgentConversations, messages),
              Layer.succeed(SignalDefinitions, [definition]),
            ),
          );
          const root = yield* system.spawn("signals", SignalRootActor);
          yield* root.awaitStarted;
          yield* (yield* system.select("/user/signals/review").resolve()).awaitStarted;
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
          const state = yield* readSignalHistory(messages, input.target);
          assert.equal(state.deliveries.length, 1);
          assert.equal(state.receipts?.length, 1);
          assert.deepEqual(state.deliveries[0]!.message.evidence, input.sourceContext);
          assert.deepEqual(state.deliveries[0]!.message.task, definition.task);
          if (restart) assert.deepEqual(state.deliveries[0]!.message, first);
          first = state.deliveries[0]!.message;
          assert.equal(
            (yield* send({ ...input, sourceContext: { ...input.sourceContext, revision: 2 } }))
              ._tag,
            "Rejected",
          );
          assert.equal(
            (yield* send({
              ...input,
              requestId: "stale",
              expectedRevision: input.expectedRevision - 1,
            }))._tag,
            "Rejected",
          );
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
});
test("scheduled Signals reject Context reactions", async () => {
  const messages = testConversations();
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(AgentConversations, messages),
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
        yield* root.awaitStarted;
        yield* (yield* system.select("/user/signals/review").resolve()).awaitStarted;
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
        assert.equal((yield* readSignalHistory(messages, "/signals/review")).deliveries.length, 0);
      }),
    ),
  );
});
