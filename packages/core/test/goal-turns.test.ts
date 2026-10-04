import { goalWorkflowLayer, type GoalScenario } from "./workflow-fixtures.js";
import { GoalToolError } from "../src/goals/tasks.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem, type ActorRef } from "@aster/actor";
import { Deferred, Effect, Fiber, Layer, Schema, Stream } from "effect";
import {
  ContextRegistry,
  makeContextRegistry,
  type ContextRecord,
  type ContextStore,
} from "../src/index.js";
import { GoalState } from "../src/goals/state.js";
import { GoalReasoningError } from "../src/goals/errors.js";
import { GoalsRootActor, type GoalCommandReply, type GoalReadyReply } from "../src/goals/actors.js";
import type { GoalReasoner } from "../src/goals/reasoner.js";
import type { GoalPlan } from "../src/goals/plan.js";
import { makeMemoryGoalHistory } from "../src/goals/history.js";
import { ExternalAgents } from "../src/tasks/model.js";
import { preparationLayer } from "./fixtures.js";

const answer = (
  input: Parameters<GoalReasoner["plan"]>[0],
  patch: Partial<GoalPlan> = {},
): GoalPlan => ({
  version: 2,
  turnId: input.durable.requestId,
  resultId: input.durable.requestId,
  disposition: "advance",
  progress: "Compared destinations using available evidence",
  evidence: [],
  nextStep: { _tag: "WaitForInput", questions: ["Which destination do you prefer?"] },
  ...patch,
});
const setup = Effect.fnUntraced(function* (
  plan: GoalReasoner["plan"],
  options: {
    store?: ContextStore;
    waitForRestore?: boolean;
    contextTokens?: number;
    history?: ReturnType<typeof makeMemoryGoalHistory>;
    reconcile?: GoalScenario["reconcile"];
    deactivate?: GoalScenario["deactivate"];
  } = {},
) {
  const registry = yield* makeContextRegistry(options.store);
  const system = yield* ActorSystem.make().pipe(
    ActorSystem.provide(
      Layer.succeed(ContextRegistry, registry),
      preparationLayer,
      Layer.succeed(ExternalAgents, {}),
      goalWorkflowLayer({
        contextTokens: options.contextTokens,
        reserveTokens: options.contextTokens ? 0 : undefined,
        definitions: [{ slug: "travel", description: "Research travel options" }],
        reasoner: { plan },
        signals: () => [],
        history: options.history ?? makeMemoryGoalHistory(),
        reconcile: options.reconcile ?? (() => Effect.succeed([])),
        deactivate: options.deactivate ?? (() => Effect.void),
      }),
    ),
  );
  const root = yield* system.spawn("goals", GoalsRootActor);
  if (options.waitForRestore !== false)
    yield* root.ask<import("../src/goals/protocol.js").GoalReadyReply>((replyTo) => ({
      _tag: "AwaitReady",
      stage: "restored",
      replyTo,
    }));
  const state = () => Schema.decodeUnknownSync(GoalState)(registry.get("/goals/travel")!.state);
  const wait = (predicate: (value: GoalState) => boolean) =>
    Effect.scoped(
      Effect.gen(function* () {
        const changes = yield* registry.subscribe;
        if (!predicate(state()))
          yield* changes.pipe(
            Stream.filter(() => predicate(state())),
            Stream.take(1),
            Stream.runDrain,
          );
      }),
    );
  const submit = (requestId: string, text: string) =>
    root.ask<GoalCommandReply>((replyTo) => ({
      _tag: "Route",
      slug: "travel",
      command: { _tag: "SubmitInput", requestId, input: { _tag: "UserInput", text }, replyTo },
    }));
  const activate = root
    .tell({ _tag: "Initialize" })
    .pipe(Effect.andThen(root.ask<GoalReadyReply>((replyTo) => ({ _tag: "AwaitReady", replyTo }))));
  return { registry, system, root, state, wait, submit, activate };
});
const run = <A, E>(effect: Effect.Effect<A, E, import("effect").Scope.Scope>) =>
  Effect.runPromise(Effect.scoped(effect).pipe(Effect.timeout("5 seconds")));

test("first activation starts useful work once; empty restart and repeated activation do not rerun", async () => {
  const records = new Map<string, ContextRecord>();
  const history = makeMemoryGoalHistory();
  const store: ContextStore = {
    loadAll: () => [...records.values()],
    save: (record) => {
      records.set(record.path, structuredClone(record));
    },
  };
  let calls = 0;
  for (const restart of [false, true])
    await run(
      Effect.gen(function* () {
        const env = yield* setup(
          (input) =>
            Effect.sync(() => {
              calls++;
              assert.match(
                JSON.stringify(input.messages),
                /Begin pursuing the configured Goal now/,
              );
              assert.doesNotMatch(JSON.stringify(input.messages), /Start Goal evaluation/);
              return answer(input);
            }),
          { store, history },
        );
        assert.equal((yield* env.activate)._tag, "Ready");
        if (!restart) yield* env.wait((state) => state.evaluations?.[0]?.status === "completed");
        yield* env.activate;
        assert.equal(
          env.state().inputs?.filter((input) => input.payload._tag === "GoalStarted").length,
          1,
        );
        assert.equal(env.state().evaluations?.length, 1);
        assert.equal(calls, 1);
      }),
    );
});

test("admission freezes complete input before the session starts and queues later input", async () =>
  run(
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const calls: Parameters<GoalReasoner["plan"]>[0][] = [];
      const env = yield* setup((input): Effect.Effect<GoalPlan> =>
        Effect.gen(function* () {
          calls.push(input);
          const frozen = env.state().pendingHandoff?.input;
          assert.ok(frozen?.inputs?.length);
          assert.equal(env.state().pendingRequestId, input.durable.requestId);
          assert.deepEqual(input.contexts, frozen.contexts);
          if (calls.length === 1) {
            yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(release);
          }
          return answer(input);
        }),
      );
      yield* env.activate;
      yield* Deferred.await(entered);
      const original = structuredClone(env.state().pendingHandoff);
      const receipt = yield* env.submit("city", "Depart from Shanghai");
      assert.equal(receipt._tag, "Accepted");
      assert.deepEqual(env.state().pendingHandoff, original);
      assert.deepEqual(yield* env.submit("city", "Depart from Shanghai"), receipt);
      assert.equal((yield* env.submit("city", "Different payload"))._tag, "Rejected");
      yield* Deferred.succeed(release, undefined);
      yield* env.wait((state) => state.evaluations?.[1]?.status === "completed");
      assert.equal(calls.length, 2);
      assert.doesNotMatch(JSON.stringify(calls[0].messages), /Shanghai/);
      assert.match(JSON.stringify(calls[1].messages), /Shanghai/);
    }),
  ));

test("continuations preserve findings, link outcomes and exhaust one causal budget", async () =>
  run(
    Effect.gen(function* () {
      let calls = 0;
      const env = yield* setup((input) =>
        Effect.sync(() => {
          calls++;
          return answer(input, {
            progress: `Research finding ${calls}`,
            nextStep: {
              _tag: "Continue",
              objective: `Compare the next destination ${calls}`,
              previousResultId: input.durable.requestId,
            },
          });
        }),
      );
      yield* env.activate;
      yield* env.wait((state) => !!state.lastError?.includes("reached its limit"));
      assert.equal(calls, 4);
      assert.equal(env.state().summary, "Research finding 4");
      assert.equal(env.state().agentAdmissions?.length, 1);
      assert.equal(
        env.state().inputs?.filter((input) => input.payload._tag === "Continuation").length,
        4,
      );
      yield* env.submit("new-instruction", "Now compare hotels");
      yield* env.wait((state) => state.summary === "Research finding 8" && !!state.lastError);
      assert.equal(calls, 8);
    }),
  ));

for (const outcome of ["failed", "unknown"] as const)
  test(`RetryTurn handles ${outcome} without replacing uncertain work`, async () =>
    run(
      Effect.gen(function* () {
        let calls = 0;
        const env = yield* setup((input) =>
          Effect.suspend(() =>
            ++calls === 1
              ? Effect.fail(
                  new GoalReasoningError({
                    operation: "plan",
                    outcome,
                    message: "Provider outcome",
                  }),
                )
              : Effect.succeed(answer(input)),
          ),
        );
        yield* env.activate;
        yield* env.wait((state) => !!state.lastError);
        const first = env.state().evaluations![0];
        const retry = () =>
          env.root.ask<GoalCommandReply>((replyTo) => ({
            _tag: "Route",
            slug: "travel",
            command: {
              _tag: "RetryTurn",
              requestId: "retry-one",
              turnId: first.evaluationId,
              replyTo,
            },
          }));
        const receipt = yield* retry();
        if (outcome === "unknown") {
          assert.equal(receipt._tag, "Rejected");
          assert.equal(calls, 1);
          assert.equal(env.state().pendingRequestId, first.evaluationId);
        } else {
          assert.equal(receipt._tag, "Accepted");
          yield* env.wait((state) => state.evaluations?.[1]?.status === "completed");
          assert.deepEqual(yield* retry(), receipt);
          const second = env.state().evaluations![1];
          assert.equal(second.retryOf, first.evaluationId);
          assert.deepEqual(second.inputIds, first.inputIds);
          assert.notEqual(second.evaluationId, first.evaluationId);
        }
      }),
    ));

test("recovery failure fails readiness and never runs durably accepted input", async () =>
  run(
    Effect.gen(function* () {
      const env = yield* setup(() => Effect.die("Must not run"), {
        reconcile: () => Effect.fail(new GoalToolError({ message: "Restore failed" })),
      });
      assert.equal((yield* env.submit("during-recovery", "Research this"))._tag, "Accepted");
      assert.equal((yield* env.activate)._tag, "Failed");
      assert.equal(env.state().inputs?.length, 2);
      assert.equal(env.state().evaluations?.length, 0);
    }),
  ));

test("End commits closure, cancels local work and retains uncertainty and durable deactivation", async () =>
  run(
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const retired = yield* Deferred.make<void>();
      const env = yield* setup(() =>
        Deferred.succeed(entered, undefined).pipe(
          Effect.andThen(Effect.never),
          Effect.ensuring(Deferred.succeed(retired, undefined)),
        ),
      );
      yield* env.activate;
      yield* Deferred.await(entered);
      const turnId = env.state().pendingRequestId;
      const receipt = yield* env.root.ask<GoalCommandReply>((replyTo) => ({
        _tag: "Route",
        slug: "travel",
        command: { _tag: "End", requestId: "end", replyTo },
      }));
      assert.equal(receipt._tag, "Accepted");
      assert.equal(env.state().status, "completed");
      yield* Deferred.await(retired);
      yield* env.wait((state) => state.deactivation?.status === "delivered");
      assert.equal(env.state().pendingRequestId, turnId);
      assert.equal(env.state().evaluations?.[0]?.status, "reconciliation_required");
      assert.equal((yield* env.submit("after-end", "Start more work"))._tag, "Rejected");
    }),
  ));

test("readiness waits do not block input routing or activation", async () =>
  run(
    Effect.gen(function* () {
      const restore = yield* Deferred.make<readonly ActorRef<unknown>[]>();
      const env = yield* setup((input) => Effect.succeed(answer(input)), {
        waitForRestore: false,
        reconcile: () => Deferred.await(restore),
      });
      const pending = yield* env.root
        .ask<GoalReadyReply>((replyTo) => ({ _tag: "AwaitReady", replyTo }))
        .pipe(Effect.forkChild);
      assert.equal((yield* env.submit("before-ready", "Research destinations"))._tag, "Accepted");
      yield* env.root.tell({ _tag: "Initialize" });
      assert.equal(env.state().evaluations?.length, 0);
      yield* Deferred.succeed(restore, []);
      assert.equal((yield* Fiber.join(pending))._tag, "Ready");
      yield* env.wait((state) => state.evaluations?.[0]?.status === "completed");
      assert.equal(
        env.state().inputs?.filter((input) => input.payload._tag === "GoalStarted").length,
        1,
      );
    }),
  ));

test("known failure wakes the remaining bounded input batches", async () =>
  run(
    Effect.gen(function* () {
      let calls = 0;
      const env = yield* setup(
        (input) =>
          Effect.suspend(() =>
            ++calls === 1
              ? Effect.fail(
                  new GoalReasoningError({
                    operation: "plan",
                    outcome: "failed",
                    message: "Known failure",
                  }),
                )
              : Effect.succeed(answer(input)),
          ),
        { contextTokens: 3 },
      );
      yield* env.submit("first", "First instruction");
      yield* env.submit("second", "Second instruction");
      yield* env.activate;
      yield* env.wait(
        (state) => state.evaluations?.length === 3 && state.evaluations[2].status === "completed",
      );
      assert.equal(env.state().evaluations?.[0].status, "failed");
      assert.equal(calls, 3);
    }),
  ));

test("bounded admission derives authority only from the included user instruction", async () =>
  run(
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const env = yield* setup(
        () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
        { contextTokens: 3 },
      );
      yield* env.submit("first", "First instruction");
      yield* env.submit("second", "Second instruction");
      yield* env.activate;
      yield* Deferred.await(entered);
      const handoff = env.state().pendingHandoff!;
      assert.equal(handoff.input?.inputs?.length, 1);
      assert.equal(handoff.input!.inputs![0].payload._tag, "UserInput");
      assert.equal(handoff.causal?.rootRequestId, "first");
    }),
  ));

const legacyStore = (patch: Partial<GoalState>): ContextStore => {
  const record: ContextRecord = {
    path: "/goals/travel",
    messages: [],
    description: "Research travel options",
    revision: 1,
    state: {
      slug: "travel",
      description: "Research travel options",
      status: "active",
      summary: "Existing findings",
      progress: "Existing findings",
      tasks: [],
      evaluations: [],
      historyThrough: 0,
      historyCount: 0,
      pendingEvaluation: false,
      receivedEvents: [],
      ...patch,
    },
  };
  return { loadAll: () => [record], save: () => {} };
};

test("legacy idle state is adopted without inventing another startup", async () =>
  run(
    Effect.gen(function* () {
      const env = yield* setup(() => Effect.die("Unexpected model invocation"), {
        store: legacyStore({}),
      });
      yield* env.activate;
      assert.equal(env.state().activated, true);
      assert.equal(env.state().inputs?.length ?? 0, 0);
    }),
  ));

test("legacy failed turns without immutable input membership reject retry before admission", async () =>
  run(
    Effect.gen(function* () {
      const env = yield* setup((input) => Effect.succeed(answer(input)), {
        store: legacyStore({
          evaluations: [
            {
              evaluationId: "old",
              reason: "Old request",
              historyThrough: 0,
              startedAt: "2026-01-01T00:00:00Z",
              status: "failed",
              error: "Failed",
              observedAt: "2026-01-01T00:00:01Z",
            },
          ],
        }),
      });
      yield* env.activate;
      const reply = yield* env.root.ask<GoalCommandReply>((replyTo) => ({
        _tag: "Route",
        slug: "travel",
        command: { _tag: "RetryTurn", requestId: "retry-old", turnId: "old", replyTo },
      }));
      assert.equal(reply._tag, "Rejected");
      assert.equal(env.state().retryTurnId, undefined);
      yield* env.submit("fresh", "Continue with a new instruction");
      yield* env.wait((state) => state.evaluations?.[1]?.status === "completed");
    }),
  ));

test("legacy pending requests inspect only their saved session result", async () =>
  run(
    Effect.gen(function* () {
      const env = yield* setup(
        (input) =>
          Effect.sync(() => {
            assert.equal(input.durable.replayOnly, true);
            assert.equal(input.durable.requestId, "legacy");
            assert.deepEqual(input.messages, []);
            return { progress: "Recovered findings", evidence: [], completed: false, signals: [] };
          }),
        {
          store: legacyStore({
            pendingRequestId: "legacy",
            pendingHandoff: { requestId: "legacy", reason: "Old request", through: 0 },
            evaluations: [
              {
                evaluationId: "legacy",
                reason: "Old request",
                historyThrough: 0,
                startedAt: "2026-01-01T00:00:00Z",
                status: "running",
              },
            ],
          }),
        },
      );
      yield* env.activate;
      yield* env.wait((state) => state.summary === "Recovered findings");
      assert.equal(env.state().pendingRequestId, undefined);
      assert.equal(env.state().evaluations?.[0].status, "completed");
    }),
  ));

test("a late settlement with another generation cannot apply to the active turn", async () =>
  run(
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const env = yield* setup(() =>
        Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
      );
      yield* env.activate;
      yield* Deferred.await(entered);
      const before = structuredClone(env.state());
      const child = yield* env.system.select("/user/goals/travel").resolve();
      yield* child.tell({
        _tag: "TurnSettled",
        turnId: before.pendingRequestId,
        resultId: before.pendingRequestId,
        generation: "retired-generation",
        result: {
          _tag: "Success",
          value: {
            version: 2,
            turnId: before.pendingRequestId,
            resultId: before.pendingRequestId,
            disposition: "advance",
            progress: "Must not apply",
            evidence: [],
            nextStep: { _tag: "WaitForInput", questions: ["Stale"] },
          },
        },
      });
      // A direct readiness acknowledgement is a mailbox barrier after the stale message.
      yield* child.ask<GoalReadyReply>((replyTo) => ({ _tag: "AwaitReady", replyTo }));
      assert.deepEqual(env.state(), before);
    }),
  ));

test("retry authority survives later input received before activation", async () =>
  run(
    Effect.gen(function* () {
      const records = new Map<string, ContextRecord>();
      const history = makeMemoryGoalHistory();
      const store: ContextStore = {
        loadAll: () => [...records.values()],
        save: (record) => {
          records.set(record.path, structuredClone(record));
        },
      };
      let turnId = "";
      yield* Effect.scoped(
        Effect.gen(function* () {
          const env = yield* setup(
            () =>
              Effect.fail(
                new GoalReasoningError({
                  operation: "plan",
                  outcome: "failed",
                  message: "Known failure",
                }),
              ),
            { store, history },
          );
          yield* env.activate;
          yield* env.wait((state) => !!state.lastError);
          turnId = env.state().evaluations![0].evaluationId;
        }),
      );
      const entered = yield* Deferred.make<void>();
      const env = yield* setup(
        () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
        { store, history },
      );
      const response = yield* env.root.ask<GoalCommandReply>((replyTo) => ({
        _tag: "Route",
        slug: "travel",
        command: { _tag: "RetryTurn", requestId: "retry-authority", turnId, replyTo },
      }));
      assert.equal(response._tag, "Accepted");
      yield* env.submit("later-authority", "A separate instruction");
      yield* env.activate;
      yield* Deferred.await(entered);
      assert.equal(env.state().pendingHandoff?.causal?.rootRequestId, "retry-authority");
      assert.deepEqual(
        env.state().evaluations?.[1].inputIds,
        env.state().evaluations?.[0].inputIds,
      );
    }),
  ));
