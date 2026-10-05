import {
  taskExecutionLayer,
  personalReasoningLayer,
  personalDisabled,
} from "./workflow-fixtures.js";
import type { PersonalReasoner } from "../src/index.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import {
  ApplicationError,
  PersonalMessage,
  PersonalState,
  type PersonalStartTaskInput,
  type TaskDeliveryInput,
} from "@aster/api-contracts";
import { Deferred, Effect, Layer, Schema, Stream } from "effect";
import {
  ApprovalQueueActor,
  ContextRegistry,
  ExternalAgents,
  PersonalActions,
  PersonalAgentActor,
  RunRootActor,
  approvalEntries,
  makeApplicationApi,
  type ContextRecord,
  type RunAdmissionReply,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";
import { RunState } from "../src/tasks/run-state.js";
import { personalTaskIntent, personalTaskPath } from "../src/tasks/admission.js";
import { fakeAgent } from "./fixtures.js";

const input: PersonalStartTaskInput = {
  requestId: "task-1",
  causationId: "user-1",
  expectedRevision: 1,
  agent: "test",
  task: {
    instructions: "Analyze release evidence",
    input: [{ content: "Frozen evidence", sources: ["/source/chat"] }],
  },
};
const fixture = (
  records = new Map<string, ContextRecord>(),
  options: {
    loseAck?: boolean;
    ready?: Effect.Effect<boolean>;
    submit?: () => void;
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
        submit: (task) =>
          Effect.sync(() => {
            options.submit?.();
            assert.deepEqual(task, input.task, "execution uses the exact frozen Task");
            return { sessionId: "existing-session", runId: "existing-run" };
          }),
      }),
    });
    const actions = yield* PersonalActions.pipe(
      Effect.provide(PersonalActions.layer.pipe(Layer.provide(agents))),
    );
    let loseAck = options.loseAck;
    const system = yield* ActorSystem.make().pipe(
      ActorSystem.provide(
        agents,
        Layer.succeed(ContextRegistry, registry),
        taskExecutionLayer({
          prepare: () => Effect.die(new Error("An exact Task must not be rewritten")),
          ready: () => options.ready ?? Effect.succeed(true),
        }),
        options.processor ? personalReasoningLayer(options.processor) : personalDisabled,
        Layer.succeed(PersonalActions, {
          ...actions,
          startTask: (command) =>
            Effect.gen(function* () {
              const source = Schema.decodeUnknownSync(PersonalState)(
                records.get("/personal")!.state,
              );
              const intent = source.outbox!.find(
                (item) => item.input.requestId === command.requestId,
              )!;
              assert.ok(intent.attempts! > 0);
              if (command.causationId === "model-input") {
                assert.equal(source.runs?.[0]?.status, "completed");
                const messages = Schema.decodeUnknownSync(Schema.Array(PersonalMessage))(
                  records.get("/personal")!.messages,
                );
                assert.equal(messages[1].revision, intent.acceptedRevision);
              }
              const receipt = yield* actions.startTask(command);
              assert.deepEqual(
                Schema.decodeUnknownSync(RunState)(records.get(command.target)!.state).admission
                  ?.receipt,
                receipt,
              );
              if (loseAck) {
                loseAck = false;
                return yield* new ApplicationError({
                  kind: "unavailable",
                  message: "Injected lost Task acknowledgement",
                });
              }
              return receipt;
            }),
        }),
      ),
    );
    const approvals = yield* system.spawn("approvals", ApprovalQueueActor);
    const runs = yield* system.spawn("runs", RunRootActor);
    const personal = yield* system.spawn("personal", PersonalAgentActor);
    yield* actions.bind(undefined, undefined, approvals, runs);
    const api = makeApplicationApi({
      registry,
      personal,
      approvals,
      inspect: Effect.succeed(null),
    });
    yield* api.personal.get;
    const until = (condition: () => boolean) =>
      Effect.gen(function* () {
        const changes = yield* registry.subscribe;
        if (!condition())
          yield* changes.pipe(Stream.filter(condition), Stream.take(1), Stream.runDrain);
      });
    const status = () =>
      Schema.decodeUnknownSync(PersonalState)(registry.get("/personal")!.state).outbox?.[0];
    const command = (input: TaskDeliveryInput) =>
      runs.ask<RunAdmissionReply>((replyTo) => ({ _tag: "StartTask", input, replyTo }));
    const run = (path: string) => {
      const record = registry.get(path);
      return record && Schema.decodeUnknownSync(RunState)(record.state);
    };
    return { registry, approvals, runs, api, until, status, command, run };
  });

test("Personal Task admission survives acknowledgement loss/restart and executes once only after confirmation", async () => {
  const records = new Map<string, ContextRecord>();
  let submissions = 0;
  let receipt: unknown;
  const path = personalTaskPath(input.requestId);
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* fixture(records, { loseAck: true, submit: () => submissions++ });
        receipt = yield* env.api.personal.startTask(input);
        yield* env.until(
          () => env.status()?.status === "unknown" && approvalEntries(env.registry).length === 1,
        );
        assert.equal(submissions, 0);
        assert.equal(env.run(path)?.status, "awaiting-confirmation");
        assert.equal(
          Object.keys(env.registry.snapshot()).some((key) => key.startsWith("/signals/")),
          false,
        );
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* fixture(records, { submit: () => submissions++ });
        yield* env.until(() => env.status()?.status === "delivered");
        assert.equal(env.status()?.attempts, 2);
        assert.deepEqual(yield* env.api.personal.startTask(input), receipt);
        assert.equal(approvalEntries(env.registry).length, 1);
        assert.equal(submissions, 0);
        yield* env.api.approvals.respond(`${path}:confirm`, { decision: "approve" });
        yield* env.approvals.tell({ _tag: "Deliver" });
        yield* env.until(() => env.run(path)?.status === "completed");
        assert.equal(submissions, 1);
        assert.deepEqual(yield* env.api.personal.startTask(input), receipt);
        assert.equal(
          Object.keys(env.registry.snapshot()).filter((key) => key.startsWith("/runs/personal--"))
            .length,
          1,
        );
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* fixture(records, { submit: () => submissions++ });
        assert.deepEqual(yield* env.api.personal.startTask(input), receipt);
        assert.equal(env.run(path)?.status, "completed");
        assert.equal(submissions, 1);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("Task receiver rejects changed payload, invalid identity and unconfigured executors", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<boolean>();
        const env = yield* fixture(new Map(), {
          ready: Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
        });
        const intent = personalTaskIntent(input, { ...input, createdAt: "2026-10-02T00:00:00Z" });
        const accepted = yield* env.command(intent);
        assert.equal(accepted._tag, "Accepted");
        yield* Deferred.await(entered);
        const snapshot = env.registry.get(intent.target);
        assert.deepEqual(yield* env.command(intent), accepted);
        for (const invalid of [
          { ...intent, task: { ...intent.task, instructions: "Different work" } },
          { ...intent, requestId: "wrong-target" },
          personalTaskIntent(
            { ...input, agent: "missing" },
            { requestId: "missing", causationId: "user", createdAt: intent.createdAt },
          ),
        ])
          assert.equal((yield* env.command(invalid))._tag, "Rejected");
        assert.deepEqual(env.registry.get(intent.target), snapshot);
        assert.equal(env.registry.get(personalTaskPath("missing")), undefined);
        yield* Deferred.succeed(release, false);
        yield* env.until(() => env.run(intent.target)?.status === "blocked");
        assert.equal(approvalEntries(env.registry).length, 0);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("model-proposed Task intent commits atomically with Personal reply before Run admission", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* fixture(new Map(), {
          processor: {
            enabled: true,
            run: () =>
              Effect.succeed({
                text: "Queued for confirmation",
                tasks: [{ agent: input.agent, task: input.task }],
              }),
          },
        });
        yield* env.api.personal.sendMessage({
          requestId: "model-input",
          causationId: "model-input",
          expectedRevision: 1,
          text: "Analyze the release evidence",
        });
        yield* env.until(
          () => env.status()?.status === "delivered" && approvalEntries(env.registry).length === 1,
        );
        assert.equal(env.status()?.input.operation, "startTask");
        assert.equal(env.status()?.input.causationId, "model-input");
        assert.equal(env.run(env.status()!.input.target)?.status, "awaiting-confirmation");
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});
