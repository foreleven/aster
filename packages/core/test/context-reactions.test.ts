import assert from "node:assert/strict";
import { test } from "node:test";
import type { ActorRef } from "@aster/actor";
import { Effect, Layer, Schema } from "effect";
import {
  ContextRegistry,
  ContextCaptureSink,
  InternalAgent,
  GoalSettings,
  SystemOneClient,
  makeContextRegistry,
  defineContext,
  startContextReactions,
  sourceSignals,
  type SignalRootCommand,
  type GoalsRootCommand,
  type ContextRecord,
} from "../src/index.js";

const ref = <A>(values: A[]): ActorRef<A> => ({
  path: "/test",
  incarnation: "test",
  tell: (value) =>
    Effect.sync(() => {
      values.push(value);
    }),
  ask: () => Effect.die("Unexpected ask"),
});
const record = (path: string, state: object): ContextRecord => ({
  path,
  description: path,
  state,
  messages: [],
});
const definition = { when: "changed", task: "read", agent: "test", mode: "confirm" as const };

test("source Signal eligibility handles deadlines, deletion, schedules and completed owners consistently", () => {
  const snapshot = Object.fromEntries(
    [
      record("/signals/live", { ...definition, slug: "live" }),
      record("/signals/deleted", { ...definition, slug: "deleted", deleted: true }),
      record("/signals/inactive", { ...definition, slug: "inactive", active: false }),
      record("/signals/timer", {
        ...definition,
        slug: "timer",
        schedule: { type: "once", at: new Date(1000).toISOString() },
      }),
      record("/signals/later", {
        ...definition,
        slug: "later",
        notBefore: new Date(2000).toISOString(),
      }),
      record("/signals/finished", { ...definition, slug: "finished", goal: "done" }),
      record("/goals/done", { status: "completed" }),
    ].map((item) => [item.path, item]),
  );
  assert.deepEqual(
    sourceSignals(snapshot, 1999).map((s) => s.slug),
    ["live"],
  );
  assert.deepEqual(
    sourceSignals(snapshot, 2000).map((s) => s.slug),
    ["live", "later"],
  );
});

test("Context reactions coordinate multiple Signals and Goals without integration knowledge or duplicate Goal screening", async () => {
  const signals: SignalRootCommand[] = [],
    goals: GoalsRootCommand[] = [];
  const screened: string[] = [];
  const registry = await Effect.runPromise(makeContextRegistry());
  const generic = defineContext({
    identity: "test",
    state: Schema.Record(Schema.String, Schema.Unknown),
    message: Schema.Unknown,
  });
  for (const item of [
    record("/signals/one", { ...definition, slug: "one", goal: "owned" }),
    record("/signals/two", { ...definition, slug: "two", goal: "owned" }),
    record("/goals/owned", { status: "active" }),
    record("/goals/other", { status: "active" }),
    record("/goals/done", { status: "completed" }),
  ]) {
    await Effect.runPromise(registry.register(item.path, generic));
    await Effect.runPromise(registry.set(item));
  }
  await Effect.runPromise(registry.register("/source", { ...generic, signalSource: true }));
  const layers = Layer.mergeAll(
    Layer.succeed(ContextRegistry, registry),
    Layer.succeed(ContextCaptureSink, { capture: () => Effect.void, drain: Effect.void }),
    Layer.succeed(GoalSettings, {
      definitions: ["owned", "other", "done"].map((slug) => ({ slug, description: slug })),
    }),
    Layer.succeed(InternalAgent, {
      extract: (_path, candidates) => Effect.sync(() => candidates.map((s) => s.slug)),
      describe: () => Effect.sync(() => "test"),
      prepare: () => Effect.sync(() => ({ instructions: "test", input: [] })),
    }),
    Layer.succeed(SystemOneClient, {
      systemOne: (request) =>
        Effect.sync(() => {
          for (const [key, question] of Object.entries(request.questions))
            if (key.startsWith("goal_")) screened.push(question.instructions);
          return {
            answers: Object.fromEntries(
              Object.keys(request.questions).map((key) => [key, { type: "choice", choice: "yes" }]),
            ),
          };
        }),
    }),
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        yield* startContextReactions({ signals: ref(signals), goals: ref(goals) });
        // The subscription is acquired before this returns, even without yieldNow.
        yield* registry.set(record("/source", { value: 1 }));
        yield* Effect.gen(function* () {
          while (!goals.length) yield* Effect.sleep(1);
        }).pipe(Effect.timeout("2 seconds"));
        assert.equal(signals.length, 2);
        assert.deepEqual(
          goals.map((command) => (command._tag === "Route" ? command.slug : "initialize")),
          ["other"],
        );
        assert.equal(screened.length, 1);
        assert.match(screened[0]!, /other/);
        yield* registry.set({ ...record("/source", { value: 1 }), messages: ["message only"] });
        yield* Effect.sleep(5);
        assert.equal(signals.length, 2);
      }).pipe(Effect.provide(layers)),
    ),
  );
});
