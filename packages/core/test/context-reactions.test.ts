import { CurrentActors } from "../src/tools/actors.js";
import { SystemOneActor } from "../src/reactions/actor.js";
import { DurableContext } from "../src/context/store.js";
import { reasoningConfig, emptyRecall, modelReplyLayer, agentResult } from "./workflow-fixtures.js";
import {
  makeSystemOneGate,
  sourceSignals,
  ReactionPolicy,
  makeReactionPolicy,
} from "../src/reactions/policy.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem, ActorTestKit, type ActorRef } from "@aster/actor";
import { Effect, Layer, Schema } from "effect";
import {
  ContextRegistry,
  GoalSettings,
  SystemOneClient,
  defineContext,
  contextView,
  type SignalRootCommand,
  type GoalsRootCommand,
  type ContextInput,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";

const record = (path: string, state: object): ContextInput => ({
  path,
  description: path,
  state: path.startsWith("/goals/")
    ? {
        definition: { slug: path.split("/").at(-1), description: path },
        summary: "",
        inputs: [],
        receipts: [],
        tasks: [],
        ...state,
      }
    : state,
  messages: [],
});
const definition = {
  trigger: { _tag: "Context", when: "changed" },
  task: { _tag: "Goal", target: "/goals/personal", text: "read" },
  status: "active",
  version: 1,
};

test("source Signal eligibility handles deletion, schedules and completed owners consistently", () => {
  const snapshot = Object.fromEntries(
    [
      record("/signals/live", { ...definition, slug: "live" }),
      record("/signals/deleted", { ...definition, slug: "deleted", status: "deleted" }),
      record("/signals/inactive", { ...definition, slug: "inactive", status: "paused" }),
      record("/signals/timer", {
        ...definition,
        slug: "timer",
        nextDue: new Date(1000).toISOString(),
        trigger: { _tag: "Schedule", schedule: { type: "once", at: new Date(1000).toISOString() } },
      }),
      record("/signals/finished", { ...definition, slug: "finished", owner: "/goals/done" }),
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
    record("/signals/one", { ...definition, slug: "one", owner: "/goals/owned" }),
    record("/signals/two", { ...definition, slug: "two", owner: "/goals/owned" }),
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
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(DurableContext, registry.backend),
            Layer.succeed(ReactionPolicy, {
              ...policy,
              deliver: (command) =>
                policy.deliver(command).pipe(
                  Effect.provideService(CurrentActors, {
                    select: (path) => ({
                      path,
                      resolve: () =>
                        Effect.succeed(
                          (path === "/user/goals"
                            ? goalProbe.ref
                            : signalProbe.ref) as ActorRef<unknown>,
                        ),
                    }),
                  }),
                ),
            }),
            Layer.succeed(GoalSettings, yield* GoalSettings),
          ),
        );
        yield* (yield* system.spawn("system-one", SystemOneActor)).awaitStarted;
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

test("System One receives email fields and every Signal condition, then selects typed yes answers", async () => {
  const email = { subject: "Please review the draft", bodyPlainText: "Review today?" };
  let request: unknown;
  const client = {
    systemOne: (value: unknown) =>
      Effect.sync(() => {
        request = value;
        return {
          answers: {
            signal_0: { type: "choice", choice: "yes" },
            signal_1: { type: "choice", choice: "no" },
          },
        };
      }),
  } as unknown as SystemOneClient;
  const signals = ["review", "invoice"].map((slug) => ({
    slug,
    trigger: { _tag: "Context" as const, when: slug === "review" ? "Review request" : "Invoice" },
    task: { _tag: "Goal" as const, target: "/goals/personal", text: "Review" },
  }));
  const selected = await Effect.runPromise(
    makeSystemOneGate(client)(
      {
        path: "/lark/mail/me/new-id",
        description: "email",
        state: email,
        messages: [],
      },
      signals,
    ),
  );
  assert.deepEqual(
    selected.map((item) => item.slug),
    ["review"],
  );
  const payload = request as {
    state: string;
    questions: Record<string, { instructions: string }>;
  };
  assert.equal(JSON.parse(payload.state).context.state.subject, email.subject);
  assert.equal(JSON.parse(payload.state).context.state.bodyPlainText, email.bodyPlainText);
  assert.match(payload.questions.signal_1!.instructions, /Invoice/);
});
