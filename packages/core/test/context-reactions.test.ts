import type { FrozenReaction } from "../src/reactions/state.js";
import { CurrentActors } from "../src/services/actors.js";
import { SystemOneActor } from "../src/reactions/actor.js";
import { DurableContext } from "../src/context/store.js";
import { reasoningConfig, emptyRecall, modelReplyLayer, agentResult } from "./workflow-fixtures.js";
import {
  matchSignal,
  sourceSignals,
  ReactionPolicy,
  makeReactionPolicy,
} from "../src/reactions/policy.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem, ActorTestKit, type ActorRef } from "@aster/actor";
import { Deferred, Effect, Fiber, Layer, Schema } from "effect";
import {
  ContextRegistry,
  GoalSettings,
  SystemOneClient,
  contextView,
  type SignalRootCommand,
  type GoalsRootCommand,
  type ContextSnapshot,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";

const record = (path: string, state: object): ContextSnapshot => ({
  path,
  revision: 1,
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
  const generic = {
    state: Schema.Record(Schema.String, Schema.Unknown),
    message: Schema.Unknown,
  };
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
      changes: "durable-state" as const,
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

test("Signal matching sends one condition and preserves the source fields", async () => {
  const client: SystemOneClient = {
    systemOne: (request) =>
      Effect.sync(() => {
        assert.deepEqual(Object.keys(request.questions), ["matches"]);
        assert.match(request.questions.matches!.instructions, /Review request/);
        assert.equal(JSON.parse(String(request.state)).context.state.subject, "Draft");
        return { answers: { matches: { type: "choice", choice: "yes" } } };
      }),
  };
  assert.deepEqual(
    await Effect.runPromise(
      matchSignal(
        client,
        {
          path: "/email",
          revision: 1,
          description: "Email",
          state: { subject: "Draft" },
          messages: [],
        },
        { _tag: "Signal", slug: "review", when: "Review request", version: 1 },
      ),
    ),
    { _tag: "Matched", reason: "Signal condition satisfied: Review request" },
  );
});

test("Signals and Goals share a bounded pool of single-target requests", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const release = yield* Deferred.make<void>();
        const full = yield* Deferred.make<void>();
        let active = 0,
          peak = 0,
          calls = 0;
        const policy = yield* makeReactionPolicy({
          client: {
            systemOne: (request) =>
              Effect.gen(function* () {
                assert.equal(Object.keys(request.questions).length, 1);
                active++;
                calls++;
                peak = Math.max(peak, active);
                if (calls === 2) yield* Deferred.succeed(full, undefined);
                yield* Deferred.await(release);
                const answers: Effect.Success<ReturnType<SystemOneClient["systemOne"]>>["answers"] =
                  request.questions.matches
                    ? { matches: { type: "choice", choice: "yes" } }
                    : { relevance: { type: "score", score: 9 } };
                return { answers };
              }).pipe(
                Effect.ensuring(
                  Effect.sync(() => {
                    active--;
                  }),
                ),
              ),
          },
        });
        const work: FrozenReaction = {
          event: {
            id: "source",
            createdAt: "2026-10-07T00:00:00Z",
            record: {
              path: "/source",
              revision: 1,
              description: "Source",
              state: { summary: "Release evidence" },
              messages: [],
            },
          },
          status: "frozen",
          targets: [
            {
              input: { _tag: "Signal", slug: "review", when: "Release changed", version: 2 },
              result: { _tag: "Pending" },
            },
            ...["one", "two", "three", "four"].map((slug) => ({
              input: {
                _tag: "Goal" as const,
                slug,
                description: "Release",
                title: slug,
                summary: "",
              },
              result: { _tag: "Pending" as const },
            })),
          ],
        };
        const fiber = yield* policy.plan(work, 2).pipe(Effect.forkScoped);
        yield* Deferred.await(full);
        assert.equal(calls, 2, "A Signal and Goal can be in flight together");
        yield* Deferred.succeed(release, undefined);
        const plan = yield* Fiber.join(fiber);
        assert.equal(calls, 5);
        assert.equal(peak, 2);
        assert.equal(plan.length, 5);
        assert.ok(plan.every((outcome) => outcome.result._tag === "Matched"));
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("malformed answers retain failed targets while valid no answers complete without delivery", async () => {
  const work: FrozenReaction = {
    event: {
      id: "source",
      createdAt: "2026-10-07T00:00:00Z",
      record: {
        path: "/source",
        revision: 1,
        description: "Source",
        state: { summary: "Release evidence" },
        messages: [],
      },
    },
    status: "frozen",
    targets: [
      ...["bad", "no"].map((slug) => ({
        input: { _tag: "Signal" as const, slug, when: slug, version: 1 },
        result: { _tag: "Pending" as const },
      })),
      {
        input: { _tag: "Goal", slug: "bad", description: "Release", title: "Bad", summary: "" },
        result: { _tag: "Pending" },
      },
    ],
  };
  const policy = await Effect.runPromise(
    makeReactionPolicy({
      client: {
        systemOne: (request) => {
          const answers: Effect.Success<ReturnType<SystemOneClient["systemOne"]>>["answers"] =
            request.questions.matches?.instructions.includes("? no")
              ? { matches: { type: "choice", choice: "no" } }
              : {};
          return Effect.succeed({ answers });
        },
      },
    }),
  );
  const plan = await Effect.runPromise(policy.plan(work, 2));
  assert.ok(plan.every((outcome) => outcome.result._tag !== "Matched"));
  assert.deepEqual(
    plan
      .filter((item) => item.result._tag === "Failed")
      .map((item) => item.target)
      .sort(),
    ["/goals/bad", "/signals/bad"],
  );
  assert.deepEqual(
    plan.find((item) => item.target === "/signals/no"),
    {
      target: "/signals/no",
      result: { _tag: "NotMatched", reason: "Signal condition not satisfied: no" },
    },
  );
});
