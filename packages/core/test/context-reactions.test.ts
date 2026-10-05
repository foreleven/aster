import { DurableContext } from "../src/context/persistence.js";
import { ContextCaptures } from "../src/memory/capture.js";
import { ContextDescriptions } from "../src/reasoning/context-description.js";
import { reasoningConfig, emptyRecall, modelReplyLayer, agentResult } from "./workflow-fixtures.js";
import { ReactionPolicy, makeReactionPolicy } from "../src/reactions/policy.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem, ActorTestKit } from "@aster/actor";
import { Effect, Fiber, Layer, Schema } from "effect";
import {
  ContextRegistry,
  ContextCaptureSink,
  GoalSettings,
  SystemOneClient,
  defineContext,
  contextView,
  startContextReactions,
  sourceSignals,
  type SignalRootCommand,
  type GoalsRootCommand,
  type ContextRecord,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";

const record = (path: string, state: object): ContextRecord => ({
  path,
  description: path,
  state: path.startsWith("/goals/")
    ? {
        definition: { slug: path.split("/").at(-1), description: path },
        summary: "",
        inputs: [],
        receipts: [],
        ...state,
      }
    : state,
  messages: [],
});
const definition = {
  trigger: { _tag: "Context", when: "changed" },
  task: { _tag: "Goal", target: "/goals/personal", text: "read" },
  active: true,
  revision: 1,
  occurrences: [],
};

test("source Signal eligibility handles deletion, schedules and completed owners consistently", () => {
  const snapshot = Object.fromEntries(
    [
      record("/signals/live", { ...definition, slug: "live" }),
      record("/signals/deleted", { ...definition, slug: "deleted", deleted: true }),
      record("/signals/inactive", { ...definition, slug: "inactive", active: false }),
      record("/signals/timer", {
        ...definition,
        slug: "timer",
        trigger: { _tag: "Schedule", schedule: { type: "once", at: new Date(1000).toISOString() } },
      }),
      record("/signals/finished", { ...definition, slug: "finished", goal: "done" }),
      record("/goals/done", { status: "completed" }),
    ].map((item) => [item.path, item]),
  );
  assert.deepEqual(
    sourceSignals(snapshot).map((s) => s.slug),
    ["live"],
  );
  assert.deepEqual(
    sourceSignals(snapshot).map((s) => s.slug),
    ["live"],
  );
});

test("Context reactions coordinate multiple Signals and Goals without integration knowledge or duplicate Goal screening", async () => {
  const signals: SignalRootCommand[] = [],
    goals: GoalsRootCommand[] = [];
  const screened: string[] = [];
  const registry = await Effect.runPromise(makeContextRegistry());
  const generic = defineContext({
    state: Schema.Record(Schema.String, Schema.Unknown),
    message: Schema.Unknown,
  });
  for (const item of [
    record("/signals/one", { ...definition, slug: "one", goal: "owned" }),
    record("/signals/two", { ...definition, slug: "two", goal: "owned" }),
    record("/goals/owned", { status: "active", summary: "Owned Goal" }),
    record("/goals/other", { status: "active", summary: "Other Goal" }),
    record("/goals/done", { status: "completed" }),
  ]) {
    await Effect.runPromise(registry.register(item.path, generic));
    await Effect.runPromise(
      registry.commit(item, { expectedRevision: registry.get(item.path)?.revision ?? 0 }),
    );
  }
  await Effect.runPromise(
    registry.register("/source", {
      ...generic,
      changes: "durable-state",
      view: contextView({ state: Schema.Struct({ summary: Schema.String }) }),
    }),
  );
  const layers = Layer.mergeAll(
    Layer.succeed(ContextRegistry, registry),
    Layer.succeed(DurableContext, registry.backend),
    Layer.succeed(ContextCaptureSink, { capture: () => Effect.void, drain: Effect.void }),
    Layer.succeed(GoalSettings, {
      definitions: ["owned", "other", "done"].map((slug) => ({ slug, description: slug })),
    }),
    reasoningConfig,
    emptyRecall,
    modelReplyLayer("submit_result", () =>
      Effect.succeed(agentResult("submit_result", { description: "test" })),
    ),
    Layer.succeed(SystemOneClient, {
      systemOne: (request) =>
        Effect.sync(() => {
          for (const question of Object.values(request.questions))
            if (question.type === "score") screened.push(question.instructions);
          return {
            answers: Object.fromEntries(
              Object.entries(request.questions).map(([key, question]) =>
                question.type === "score"
                  ? [key, { type: "score", score: 8, legend: { "8": "Relevant evidence" } }]
                  : [key, { type: "choice", choice: "yes" }],
              ),
            ),
          };
        }),
    }),
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const goalProbe = yield* ActorTestKit.probe<GoalsRootCommand>();
        const signalProbe = yield* ActorTestKit.probe<SignalRootCommand>();
        const policy = yield* makeReactionPolicy({
          client: yield* SystemOneClient,
        });
        yield* policy.bind(signalProbe.ref, goalProbe.ref);
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(DurableContext, registry.backend),
            Layer.succeed(ReactionPolicy, policy),
            Layer.succeed(GoalSettings, yield* GoalSettings),
          ),
        );
        const starting = yield* startContextReactions({
          system,
          changes: yield* registry.subscribe,
          signals: signalProbe.ref,
          goals: goalProbe.ref,
        }).pipe(
          Effect.provide(Layer.mergeAll(ContextCaptures.layer, ContextDescriptions.layer)),
          Effect.forkScoped,
        );
        const signalReady = yield* signalProbe.take();
        assert.equal(signalReady._tag, "Ready");
        if (signalReady._tag === "Ready") yield* signalReady.replyTo.tell(undefined);
        const goalReady = yield* goalProbe.take();
        assert.equal(goalReady._tag, "AwaitReady");
        if (goalReady._tag === "AwaitReady") yield* goalReady.replyTo.tell({ _tag: "Ready" });
        yield* Fiber.join(starting);
        // The subscription is acquired before this returns, even without yieldNow.
        yield* registry.commit(record("/source", { summary: "A relevant source summary" }), {
          expectedRevision: registry.get("/source")?.revision ?? 0,
        });
        for (let index = 0; index < 2; index++) {
          const signal = yield* signalProbe.take();
          signals.push(signal);
          assert.equal(signal._tag, "React");
          if (signal._tag === "React")
            yield* signal.replyTo.tell({
              _tag: "Accepted",
              receipt: { requestId: signal.input.requestId, revision: 2 },
            });
        }
        for (let index = 0; index < 2; index++) {
          const delivered = yield* goalProbe.take();
          goals.push(delivered);
          if (delivered._tag === "Route" && delivered.command._tag === "SubmitInput")
            yield* delivered.command.replyTo.tell({
              _tag: "Accepted",
              receipt: { requestId: delivered.command.requestId, revision: 2 },
            });
        }
        assert.equal(signals.length, 2);
        assert.deepEqual(
          goals.map((command) => (command._tag === "Route" ? command.slug : "initialize")),
          ["owned", "other"],
        );
        const intent = goals[0];
        assert.equal(intent?._tag, "Route");
        if (
          intent?._tag === "Route" &&
          intent.command._tag === "SubmitInput" &&
          intent.command.input._tag === "GoalIntent"
        ) {
          assert.equal(intent.command.input.delivery.intent.relevance.score, 8 / 9);
          assert.equal(
            intent.command.input.delivery.intent.relevance.rationale,
            "Relevant evidence",
          );
        }
        assert.equal(screened.length, 2);
        assert.match(screened[1]!, /other/);
        yield* registry.commit(
          {
            ...record("/source", { summary: "A relevant source summary" }),
            messages: ["message only"],
          },
          { expectedRevision: registry.get("/source")?.revision ?? 0 },
        );
        yield* Effect.sleep(5);
        assert.equal(signals.length, 2);
      }).pipe(Effect.provide(layers)),
    ),
  );
});
