import { personalReasoningLayer, personalDisabled } from "./workflow-fixtures.js";
import type { PersonalReasoner } from "../src/index.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem, ActorTestKit } from "@aster/actor";
import {
  ApplicationError,
  PersonalMessage,
  PersonalState,
  type SignalDeliveryInput,
  type PersonalSignalCommandInput,
} from "@aster/api-contracts";
import { Clock, Effect, Layer, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import {
  ApprovalQueueActor,
  ContextRegistry,
  ExternalAgents,
  PersonalActions,
  PersonalAgentActor,
  SignalDefinitions,
  SignalRootActor,
  makeApplicationApi,
  makeContextRegistry,
  type ContextRecord,
  type GoalCommand,
  type PersonalReply,
  type SignalDefinition,
} from "../src/index.js";
import type { SignalCommandReply, SignalConfigureReply } from "../src/signals/actors.js";
import { fakeAgent, preparationLayer } from "./fixtures.js";
import { BusinessOutbox } from "../src/notifications/inbox.js";
import { SignalState } from "../src/signals/state.js";

const proposal: PersonalSignalCommandInput = {
  requestId: "signal-1",
  causationId: "user-1",
  expectedRevision: 1,
  operation: "createSignal",
  signalSlug: "personal--release",
  signalRevision: 0,
  definition: {
    action: { _tag: "PublishResult", channelPath: "/lark/im/chats/oc_test", identity: "user" },
    when: "A release blocker changes",
    task: "Inspect release blockers",
    agent: "test",
    schedule: { type: "once", at: "2099-01-01T00:00:00Z" },
  },
  active: true,
};
const fixture = (
  records = new Map<string, ContextRecord>(),
  loseAck = false,
  clock?: Clock.Clock,
  options: {
    definitions?: readonly SignalDefinition[];
    processor?: PersonalReasoner;
  } = {},
) =>
  Effect.gen(function* () {
    const registry = yield* makeContextRegistry({
      loadAll: () => [...records.values()],
      save: (record) => {
        records.set(record.path, structuredClone(record));
      },
    });
    const agents = Layer.succeed(ExternalAgents, {
      test: fakeAgent({
        submit: () => Effect.die(new Error("Unapproved Signal must never submit")),
      }),
    });
    const actions = yield* PersonalActions.pipe(
      Effect.provide(PersonalActions.layer.pipe(Layer.provide(agents))),
    );
    let attempts = records.get("/personal")
      ? (Schema.decodeUnknownSync(PersonalState)(records.get("/personal")!.state).outbox?.[0]
          ?.attempts ?? 0)
      : 0;
    const system = yield* ActorSystem.make().pipe(
      ActorSystem.provide(
        Layer.succeed(ContextRegistry, registry),
        agents,
        preparationLayer,
        Layer.succeed(SignalDefinitions, options.definitions ?? []),
        options.processor ? personalReasoningLayer(options.processor) : personalDisabled,
        Layer.succeed(Clock.Clock, clock ?? (yield* Clock.Clock)),
        Layer.succeed(PersonalActions, {
          ...actions,
          applySignal: (input) =>
            Effect.gen(function* () {
              const outbox = Schema.decodeUnknownSync(PersonalState)(
                records.get("/personal")!.state,
              ).outbox!;
              if (input.causationId === "model-input") {
                const personal = records.get("/personal")!;
                const state = Schema.decodeUnknownSync(PersonalState)(personal.state);
                const messages = Schema.decodeUnknownSync(Schema.Array(PersonalMessage))(
                  personal.messages,
                );
                assert.equal(state.runs?.[0]?.status, "completed");
                assert.equal(state.pendingRequestIds.length, 0);
                assert.equal(messages[1]?.payload._tag, "AgentReply");
                assert.equal(outbox[0]?.acceptedRevision, messages[1]?.revision);
              }
              assert.equal(
                outbox.find((item) => item.input.requestId === input.requestId)?.attempts,
                ++attempts,
              );
              const result = yield* actions.applySignal(input);
              if (loseAck) {
                loseAck = false;
                return yield* new ApplicationError({
                  kind: "unavailable",
                  message: "Injected lost acknowledgement",
                });
              }
              return result;
            }),
        }),
      ),
    );
    yield* system.spawn("approvals", ApprovalQueueActor);
    const root = yield* system.spawn("signals", SignalRootActor);
    const personal = yield* system.spawn("personal", PersonalAgentActor);
    yield* actions.bind(undefined, root);
    const api = makeApplicationApi({ registry, personal, inspect: Effect.succeed(null) });
    yield* api.personal.get;
    const until = (predicate: () => boolean) =>
      Effect.gen(function* () {
        const stream = yield* registry.subscribe;
        if (!predicate())
          yield* stream.pipe(
            Stream.filter(() => predicate()),
            Stream.take(1),
            Stream.runDrain,
          );
      });
    const status = () =>
      Schema.decodeUnknownSync(PersonalState)(registry.get("/personal")!.state).outbox?.[0];
    const command = (input: SignalDeliveryInput) =>
      root.ask<SignalCommandReply>((replyTo) => ({ _tag: "ApplyPersonalCommand", input, replyTo }));
    return { registry, api, root, system, until, status, command };
  });

test("Personal Signal command survives lost acknowledgement and restart without changing its timer twice", async () => {
  const records = new Map<string, ContextRecord>();
  let accepted: unknown;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* fixture(records, true);
        accepted = yield* env.api.personal.applySignal(proposal);
        yield* env.until(() => env.status()?.status === "unknown");
        assert.equal(env.registry.get("/signals/personal--release")?.revision, 1);
        assert.equal(env.status()?.attempts, 1);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
  const original = records.get("/signals/personal--release")!;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* fixture(records);
        yield* env.until(() => env.status()?.status === "delivered");
        assert.deepEqual(env.registry.get(original.path), original);
        assert.equal(env.status()?.attempts, 2);
        assert.equal("error" in env.status()!, false);
        assert.deepEqual(yield* env.api.personal.applySignal(proposal), accepted);
        assert.equal(env.status()?.attempts, 2);
        const input = env.status()!.input;
        assert.ok("definition" in input);
        if (!("definition" in input)) return;
        const conflict = yield* env.command({
          ...input,
          definition: { ...input.definition, task: "Different work" },
        });
        assert.equal(conflict._tag, "Rejected");
        const update = {
          ...input,
          requestId: "signal-2",
          operation: "updateSignal" as const,
          expectedRevision: 1,
          definition: { when: "New evidence", task: "New work", agent: "test" },
          active: false,
        };
        const updated = yield* env.command(update);
        assert.equal(updated._tag, "Accepted");
        const snapshot = env.registry.get(original.path)!;
        assert.equal(snapshot.revision, 2);
        assert.equal("schedule" in snapshot.state, false);
        assert.equal("action" in snapshot.state, false);
        assert.equal("nextDue" in snapshot.state, false);
        assert.equal((snapshot.state as { mode: string }).mode, "confirm");
        assert.equal((yield* env.command({ ...update, requestId: "stale" }))._tag, "Rejected");
        assert.deepEqual(env.registry.get(original.path), snapshot);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("Personal Signal ownership is enforced by both Personal and legacy configuration commands", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const definition = {
          slug: "personal--configured",
          when: "Changed",
          task: "Configured work",
          agent: "test",
          mode: "confirm" as const,
        };
        const records = new Map<string, ContextRecord>([
          [
            "/signals/personal--goal",
            {
              path: "/signals/personal--goal",
              description: "Goal owned",
              revision: 1,
              state: { ...definition, slug: "personal--goal", goal: "release", active: false },
              messages: [],
            },
          ],
        ]);
        const env = yield* fixture(records, false, undefined, { definitions: [definition] });
        for (const slug of ["personal--configured", "personal--goal"]) {
          const rejected = yield* env.command({
            operation: "updateSignal",
            requestId: slug,
            causationId: "user",
            source: "/personal",
            target: `/signals/${slug}`,
            expectedRevision: 1,
            createdAt: "2026-10-02T00:00:00Z",
            definition: { when: "Changed", task: "Overwrite", agent: "test" },
            active: false,
          });
          assert.equal(rejected._tag, "Rejected");
          if (rejected._tag === "Rejected")
            assert.equal(rejected.error.message, "Signal belongs to another owner");
          assert.equal(
            (env.registry.get(`/signals/${slug}`)!.state as { task: string }).task,
            "Configured work",
          );
        }
        yield* env.api.personal.applySignal(proposal);
        yield* env.until(() => env.status()?.status === "delivered");
        const original = env.registry.get("/signals/personal--release");
        const subscriber = yield* ActorTestKit.probe<GoalCommand>();
        const reply = yield* env.root.ask<SignalConfigureReply>((replyTo) => ({
          _tag: "Upsert",
          definition: { ...definition, slug: "personal--release" },
          goal: "release",
          subscriber: subscriber.ref,
          active: false,
          replyTo,
        }));
        assert.equal(reply._tag, "Rejected");
        assert.deepEqual(env.registry.get("/signals/personal--release"), original);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("Personal model Signal intent and reply commit together before delivery", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* fixture(new Map(), false, undefined, {
          processor: {
            enabled: true,
            run: () =>
              Effect.succeed({ text: "Monitoring the release", signalCommands: [proposal] }),
          },
        });
        yield* env.api.personal.sendMessage({
          requestId: "model-input",
          causationId: "model-input",
          expectedRevision: 1,
          text: "Monitor the release",
        });
        yield* env.until(() => env.status()?.status === "delivered");
        assert.equal(env.status()?.input.causationId, "model-input");
        assert.equal(env.status()?.attempts, 1);
        const signal = env.registry.get("/signals/personal--release")!;
        assert.equal(signal.revision, 1);
        assert.equal((signal.state as { mode: string }).mode, "confirm");
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("Signal owner rejects malformed timing, unknown executors and another owner's namespace", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* fixture();
        const input: SignalDeliveryInput = {
          operation: "createSignal",
          requestId: "new",
          causationId: "user",
          source: "/personal",
          target: "/signals/personal--test",
          expectedRevision: 0,
          createdAt: "2026-10-02T00:00:00Z",
          definition: { when: "Changed", task: "Inspect", agent: "test" },
          active: false,
        };
        for (const invalid of [
          { ...input, target: "/signals/goal--owned" },
          { ...input, definition: { ...input.definition, agent: "missing" } },
          {
            ...input,
            definition: {
              ...input.definition,
              schedule: { type: "once" as const, at: "tomorrow" },
            },
          },
          {
            ...input,
            definition: {
              ...input.definition,
              schedule: { type: "cron" as const, expression: "invalid", timeZone: "Asia/Shanghai" },
            },
          },
        ])
          assert.equal((yield* env.command(invalid))._tag, "Rejected");
        assert.equal(env.registry.get(input.target), undefined);
        assert.equal((yield* env.command(input))._tag, "Accepted");
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

for (const kind of ["once", "cron"] as const)
  test(`Personal ${kind} Signal recovers one due occurrence after restart and requires approval`, async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const records = new Map<string, ContextRecord>();
          const start = Date.parse("2026-10-02T00:00:00Z");
          const clock = yield* TestClock.make();
          yield* clock.adjust(start);
          yield* Effect.scoped(
            Effect.gen(function* () {
              const env = yield* fixture(records, false, clock);
              yield* env.api.personal.applySignal({
                ...proposal,
                definition: {
                  ...proposal.definition,
                  schedule:
                    kind === "once"
                      ? { type: "once", at: new Date(start + 60_000).toISOString() }
                      : { type: "cron", expression: "1 * * * *", timeZone: "UTC" },
                },
              });
              yield* env.until(() => env.status()?.status === "delivered");
            }),
          );
          yield* clock.adjust(61_000);
          yield* Effect.scoped(
            Effect.gen(function* () {
              const env = yield* fixture(records, false, clock);
              yield* env.until(
                () =>
                  (env.registry.get("/approvals")?.state as { entries?: unknown[] })?.entries
                    ?.length === 1,
              );
              const signal = env.registry.get("/signals/personal--release")!.state as {
                occurrences: unknown[];
                owner: string;
                active: boolean;
              };
              assert.equal(signal.owner, "/personal");
              assert.equal(signal.active, true);
              assert.equal(signal.occurrences.length, 1);
              const notifications = Schema.decodeUnknownSync(BusinessOutbox)(signal).businessOutbox;
              assert.equal(notifications.length, 1);
              assert.equal(notifications[0]!.kind, "SignalMatched");
              assert.equal(notifications[0]!.source, "/signals/personal--release");
              const retained = Schema.decodeUnknownSync(SignalState)(signal);
              assert.deepEqual(
                notifications[0]!.causal,
                kind === "cron"
                  ? { rootRequestId: retained.occurrences![0]!.id, remainingAgentTurns: 4 }
                  : retained.causal,
              );
              const run = Object.values(env.registry.snapshot()).find((record) =>
                record.path.includes("/runs/"),
              )!;
              assert.equal((run.state as { status: string }).status, "awaiting-confirmation");
            }),
          );
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  });

test("recurring Signal grants bounded per-occurrence reactions beyond eight ticks and across restart", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const records = new Map<string, ContextRecord>();
        const clock = yield* TestClock.make();
        yield* clock.adjust(Date.parse("2026-10-03T00:00:00Z"));
        for (let restart = 0; restart < 2; restart++) {
          yield* Effect.scoped(
            Effect.gen(function* () {
              const env = yield* fixture(records, false, clock);
              if (restart === 0) {
                yield* env.api.personal.applySignal({
                  ...proposal,
                  definition: {
                    ...proposal.definition,
                    schedule: { type: "cron", expression: "* * * * *", timeZone: "UTC" },
                  },
                });
                yield* env.until(() => env.status()?.status === "delivered");
              }
              const personal = yield* env.system.select("/user/personal").resolve();
              const state = () =>
                Schema.decodeUnknownSync(SignalState)(
                  env.registry.get("/signals/personal--release")!.state,
                );
              for (let tick = restart * 5 + 1; tick <= (restart + 1) * 5; tick++) {
                yield* clock.adjust(60_000);
                yield* env.until(() => state().occurrences?.length === tick);
                const occurrence = state().occurrences!.at(-1)!;
                const input = state().businessOutbox!.at(-1)!;
                assert.deepEqual(input.causal, {
                  rootRequestId: occurrence.id,
                  remainingAgentTurns: 4,
                });
                const notify = () =>
                  personal.ask<PersonalReply>((replyTo) => ({ _tag: "Notify", input, replyTo }));
                const receipt = yield* notify();
                assert.equal(receipt._tag, "Accepted");
                assert.deepEqual(yield* notify(), receipt);
                const current = env.registry.get("/personal")!;
                const messages = Schema.decodeUnknownSync(Schema.Array(PersonalMessage))(
                  current.messages,
                );
                const progress = messages.filter(
                  (message) => message.payload._tag === "ProgressEvent",
                );
                assert.equal(progress.length, tick);
                assert.ok(
                  progress.every(
                    (message) =>
                      message.payload._tag === "ProgressEvent" &&
                      message.payload.processing === "queued",
                  ),
                );
                assert.equal(
                  new Set(progress.map((message) => message.causal!.rootRequestId)).size,
                  tick,
                );
              }
            }),
          );
        }
      }),
    ).pipe(Effect.timeout("10 seconds")),
  );
});

test("Personal immediate Signal creates no timer and waits for confirmation after a source trigger", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* fixture();
        yield* env.api.personal.createSignal({
          ...proposal,
          definition: { when: "A blocker changed", task: "Inspect the blocker", agent: "test" },
        });
        yield* env.until(() => env.status()?.status === "delivered");
        const signal = env.registry.get("/signals/personal--release")!;
        assert.equal("schedule" in signal.state, false);
        assert.equal("nextDue" in signal.state, false);
        yield* env.root.tell({
          _tag: "Trigger",
          slug: "personal--release",
          sourceContext: {
            path: "/evidence",
            description: "Release evidence",
            revision: 2,
            state: { blocker: "CI failed" },
            messages: [],
          },
        });
        yield* env.until(
          () =>
            (env.registry.get("/approvals")?.state as { entries?: unknown[] })?.entries?.length ===
            1,
        );
        const runs = Object.values(env.registry.snapshot()).filter((record) =>
          record.path.includes("/runs/"),
        );
        assert.equal(runs.length, 1);
        assert.equal((runs[0].state as { status: string }).status, "awaiting-confirmation");
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});
