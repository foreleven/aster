import { TaskPreparationError } from "../src/index.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Layer, Match } from "effect";
import { Actor, ActorSystem } from "@aster/actor";
import {
  ApprovalQueueActor,
  ApprovalResolved,
  approvalEntries,
  sendApproval,
  ContextRegistry,
  makeContextRegistry,
  makeContextProcessor,
  ExternalAgents,
  SignalDefinitions,
  SignalRootActor,
  TaskPreparation,
  type ApprovalEntry,
  type ApprovalResponse,
  type InputRequest,
  type ContextRecord,
  type ContextStore,
} from "../src/index.js";
import { fakeAgent, preparationLayer } from "./fixtures.js";
const until = (condition: () => boolean) =>
  Effect.gen(function* () {
    while (!condition()) yield* Effect.sleep(5);
  }).pipe(Effect.timeout("5 seconds"));

test("approval validation rejects invalid answers without saving and accepts complete responses", async () => {
  const questions = [
    { id: "environment", prompt: "Which environment?" },
    { id: "version", prompt: "Which version?" },
  ];
  const cases: readonly {
    name: string;
    missing?: boolean;
    kind?: ApprovalEntry["kind"];
    status?: ApprovalEntry["status"];
    questions?: InputRequest["questions"];
    response: ApprovalResponse;
    accepted?: ApprovalResponse;
    error?: string;
  }[] = [
    {
      name: "invalid offered option",
      questions: [{ id: "choice", prompt: "Pick", options: ["A"] }],
      response: { answers: { choice: ["B"] } },
      error: "Answer must match an offered option",
    },
    {
      name: "single choice rejects multiple",
      questions: [{ id: "choice", prompt: "Pick", options: ["A", "B"], multiple: false }],
      response: { answers: { choice: ["A", "B"] } },
      error: "Single-choice question requires one answer",
    },
    {
      name: "custom input is provider controlled",
      questions: [{ id: "choice", prompt: "Pick", options: ["A"], allowOther: true }],
      response: { answers: { choice: ["B"] } },
    },
    { name: "missing entry", missing: true, response: {}, error: "Approval not found" },
    ...(["resolved", "acknowledged", "revoked"] as const).map((status) => ({
      name: `${status} entry`,
      status,
      response: {},
      error: "Approval already resolved",
    })),
    {
      name: "confirmation needs a decision",
      kind: "confirmation",
      response: { text: "Yes" },
      error: "Approval decision is required",
    },
    {
      name: "approval needs a decision",
      kind: "approval",
      response: {},
      error: "Approval decision is required",
    },
    { name: "explicit approval", kind: "confirmation", response: { decision: "approve" } },
    { name: "explicit rejection", kind: "approval", response: { decision: "reject" } },
    { name: "empty input", response: { decision: "approve" }, error: "Input is required" },
    {
      name: "blank text and answers",
      questions,
      response: { text: "  ", answers: { environment: ["", "\t"] } },
      error: "Input is required",
    },
    { name: "free-form input", response: { text: "Use staging" } },
    {
      name: "single question accepts text",
      questions: questions.slice(0, 1),
      response: { text: "Use staging" },
      accepted: { text: "Use staging", answers: { environment: ["Use staging"] } },
    },
    {
      name: "multiple questions cannot share free text",
      questions,
      response: { text: "Use staging" },
      error: "Every question requires an answer",
    },
    {
      name: "partial answers",
      questions,
      response: { answers: { environment: ["staging"] } },
      error: "Every question requires an answer",
    },
    {
      name: "unrelated answers",
      questions: questions.slice(0, 1),
      response: { answers: { other: ["staging"] } },
      error: "Every question requires an answer",
    },
    {
      name: "complete keyed answers",
      questions,
      response: { answers: { environment: ["", "staging"], version: ["v2"] } },
    },
  ];

  for (const scenario of cases) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const kind = scenario.kind ?? "input";
          const entry: ApprovalEntry = {
            id: "validation",
            target: "/user/receiver",
            contextPath: "/delegations/validation",
            kind,
            status: scenario.status ?? "pending",
            request: {
              id: "external",
              kind: kind === "input" ? "input" : "approval",
              prompt: "Confirm or provide input",
              questions: scenario.questions,
            },
          };
          const initial = {
            path: "/approvals",
            description: "Approval queue",
            state: { entries: scenario.missing ? [] : [entry] },
            messages: [],
          };
          let saves = 0;
          const registry = yield* makeContextRegistry({
            loadAll: () => [initial],
            save: () => {
              saves++;
            },
          });
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(Layer.succeed(ContextRegistry, registry)),
          );
          const queue = yield* system.spawn("approvals", ApprovalQueueActor);
          const reply = yield* queue.ask<{ error?: string }>((replyTo) => ({
            _tag: "Resolve",
            id: entry.id,
            response: scenario.response,
            replyTo,
          }));
          if (scenario.error) {
            assert.deepEqual(reply, { error: scenario.error }, scenario.name);
            assert.equal(saves, 0, scenario.name);
            assert.deepEqual(registry.get("/approvals"), initial, scenario.name);
          } else {
            assert.deepEqual(reply, {}, scenario.name);
            assert.equal(saves, 1, scenario.name);
            assert.deepEqual(
              approvalEntries(registry),
              [{ ...entry, status: "resolved", response: scenario.accepted ?? scenario.response }],
              scenario.name,
            );
          }
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  }
});

test("persisted approval answers reach a recreated Actor only after it exists, with acknowledgement", async () => {
  const records = new Map<string, ContextRecord>();
  const store: ContextStore = {
    loadAll: () => [...records.values()].map((r) => structuredClone(r)),
    save: (record) => {
      records.set(record.path, structuredClone(record));
    },
  };
  let received = 0;
  class Receiver extends Actor.Service<Receiver>()("test/approvalReceiver", {
    command: ApprovalResolved,
  }) {
    static readonly layer = Layer.succeed(
      Receiver,
      Receiver.of({
        receive: (command, context) =>
          Match.value(command).pipe(
            Match.tag("ApprovalResolved", ({ requestId, response }) =>
              Effect.gen(function* () {
                assert.equal(response.decision, "approve");
                received++;
                yield* sendApproval(context, {
                  _tag: "Acknowledge",
                  id: requestId,
                  target: context.path,
                });
              }),
            ),
            Match.exhaustive,
          ),
      }),
    );
  }
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry(store);
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(Layer.succeed(ContextRegistry, registry)),
        );
        const queue = yield* system.spawn("approvals", ApprovalQueueActor);
        yield* queue.tell({
          _tag: "Enqueue",
          entry: {
            id: "approval-1",
            target: "/user/receiver",
            contextPath: "/delegations/one",
            kind: "approval",
            request: { id: "external-1", kind: "approval", prompt: "Allow?" },
            status: "pending",
          },
        });
        const reply = yield* queue.ask<{ error?: string }>((replyTo) => ({
          _tag: "Resolve",
          id: "approval-1",
          response: { decision: "approve" },
          replyTo,
        }));
        assert.deepEqual(reply, {});
        assert.equal(approvalEntries(registry)[0]!.status, "resolved");
      }),
    ),
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry(store);
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(Layer.succeed(ContextRegistry, registry)),
        );
        yield* system.spawn("approvals", ApprovalQueueActor);
        yield* Effect.sleep(30);
        assert.equal(received, 0);
        yield* system.spawn("receiver", Receiver);
        yield* until(() => approvalEntries(registry)[0]?.status === "acknowledged");
        yield* Effect.sleep(1100);
        assert.equal(received, 1);
        let evaluated = false;
        const process = makeContextProcessor(
          registry,
          () => Effect.void,
          () =>
            Effect.sync(() => {
              evaluated = true;
            }),
          () => Effect.sync(() => "unused"),
        );
        yield* process({
          path: "/approvals",
          record: registry.get("/approvals")!,
          stateChanged: true,
          created: false,
        });
        assert.equal(evaluated, false);
      }),
    ),
  );
});

test("prepared Task is checked before confirmation and the approved Task is submitted once", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        let submissions = 0;
        const task = {
          instructions: "Prepared task",
          input: [{ content: "Evidence", sources: ["/source"] }],
        };
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(SignalDefinitions, [
              { slug: "review", when: "now", task: "Review", agent: "codex", mode: "confirm" },
            ]),
            Layer.succeed(TaskPreparation, {
              prepare: () => Effect.sync(() => task),
              ready: (_definition, _source, prepared) =>
                Effect.sync(() => {
                  assert.deepEqual(prepared, task);
                  assert.ok(
                    Object.values(registry.snapshot()).some((record) =>
                      record.messages.some((m) => (m as { type?: string }).type === "TaskPrepared"),
                    ),
                  );
                  return true;
                }),
            }),
            Layer.succeed(ExternalAgents, {
              codex: fakeAgent({
                submit: (prepared) =>
                  Effect.sync(() => {
                    assert.deepEqual(prepared, task);
                    submissions++;
                    return { sessionId: "session", runId: "run" };
                  }),
              }),
            }),
          ),
        );
        const queue = yield* system.spawn("approvals", ApprovalQueueActor);
        const root = yield* system.spawn("signals", SignalRootActor);
        yield* root.tell({
          _tag: "Trigger",
          slug: "review",
          sourceContext: { path: "/source", description: "Evidence", state: {}, messages: [] },
        });
        yield* until(() => approvalEntries(registry).length === 1);
        assert.equal(submissions, 0);
        const id = approvalEntries(registry)[0]!.id;
        yield* queue.ask((replyTo) => ({
          _tag: "Resolve",
          id,
          response: { decision: "approve" },
          replyTo,
        }));
        yield* until(() =>
          Object.values(registry.snapshot()).some(
            (record) =>
              record.path.startsWith("/delegations/") &&
              (record.state as { status?: string }).status === "completed",
          ),
        );
        assert.equal(submissions, 1);
        assert.equal(approvalEntries(registry)[0]!.status, "acknowledged");
      }),
    ),
  );
});

test("external approval returns through its owning Actor and targets the original session/run", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        let responses = 0;
        let submitted = 0;
        const agent = fakeAgent({
          submit: () =>
            Effect.sync(() => {
              submitted++;
              return { sessionId: "s", runId: "r" };
            }),
          status: () =>
            Effect.sync(() =>
              responses
                ? { state: "completed", result: { text: "approved and done" } }
                : {
                    state: "waiting_input",
                    requests: [{ id: "control", kind: "approval", prompt: "Permission?" }],
                  },
            ),
          respond: (session, request, response) =>
            Effect.sync(() => {
              assert.equal(session.sessionId, "s");
              assert.equal(session.runId, "r");
              assert.equal(request.id, "control");
              assert.equal(response.decision, "approve");
              responses++;
            }),
        });
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            preparationLayer,
            Layer.succeed(SignalDefinitions, [
              { slug: "work", when: "now", task: "Work", agent: "test", mode: "auto" },
            ]),
            Layer.succeed(ExternalAgents, { test: agent }),
          ),
        );
        const queue = yield* system.spawn("approvals", ApprovalQueueActor);
        const root = yield* system.spawn("signals", SignalRootActor);
        yield* root.tell({
          _tag: "Trigger",
          slug: "work",
          sourceContext: { path: "/source", description: "Evidence", state: {}, messages: [] },
        });
        yield* until(() => approvalEntries(registry).length === 1);
        yield* queue.ask((replyTo) => ({
          _tag: "Resolve",
          id: approvalEntries(registry)[0]!.id,
          response: { decision: "approve" },
          replyTo,
        }));
        yield* until(() =>
          Object.values(registry.snapshot()).some(
            (record) =>
              record.path.includes("/runs/") &&
              (record.state as { status?: string }).status === "completed",
          ),
        );
        assert.equal(responses, 1);
        assert.equal(submitted, 1);
      }),
    ),
  );
});

test("failed preparation and rejected readiness stay recorded without submitting or retrying", async () => {
  for (const fails of [true, false])
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* makeContextRegistry();
          let attempts = 0,
            submits = 0;
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              Layer.succeed(ContextRegistry, registry),
              Layer.succeed(SignalDefinitions, [
                { slug: "blocked", when: "now", task: "Task", agent: "test", mode: "auto" },
              ]),
              Layer.succeed(TaskPreparation, {
                prepare: () =>
                  Effect.gen(function* () {
                    attempts++;
                    if (fails)
                      return yield* new TaskPreparationError({
                        operation: "prepare",
                        message: "Preparation failed",
                        cause: "test",
                      });
                    return { instructions: "Task", input: [] };
                  }),
                ready: () => Effect.sync(() => false),
              }),
              Layer.succeed(ExternalAgents, {
                test: fakeAgent({
                  submit: () =>
                    Effect.sync(() => {
                      submits++;
                      return { sessionId: "wrong" };
                    }),
                }),
              }),
            ),
          );
          const root = yield* system.spawn("signals", SignalRootActor);
          yield* root.tell({
            _tag: "Trigger",
            slug: "blocked",
            sourceContext: { path: "/source", description: "source", state: {}, messages: [] },
          });
          yield* until(() =>
            Object.values(registry.snapshot()).some(
              (record) =>
                (record.state as { status?: string }).status ===
                (fails ? "preparation-failed" : "blocked"),
            ),
          );
          assert.equal(attempts, 1);
          assert.equal(submits, 0);
        }),
      ),
    );
});

test("a resolved execution confirmation survives restart and uses the saved Task without rebuilding it", async () => {
  const records = new Map<string, ContextRecord>();
  const store: ContextStore = {
    loadAll: () => [...records.values()].map((r) => structuredClone(r)),
    save: (r) => {
      records.set(r.path, structuredClone(r));
    },
  };
  const definition = {
    slug: "restore",
    when: "now",
    task: "Task",
    agent: "test",
    mode: "confirm" as const,
  };
  let prepared = 0,
    submitted = 0;
  for (const restarting of [false, true])
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* makeContextRegistry(store);
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              Layer.succeed(ContextRegistry, registry),
              Layer.succeed(SignalDefinitions, [definition]),
              Layer.succeed(TaskPreparation, {
                prepare: () =>
                  Effect.sync(() => {
                    prepared++;
                    return { instructions: "Original prepared task", input: [] };
                  }),
                ready: () => Effect.sync(() => true),
              }),
              Layer.succeed(ExternalAgents, {
                test: fakeAgent({
                  submit: (task) =>
                    Effect.sync(() => {
                      assert.equal(task.instructions, "Original prepared task");
                      submitted++;
                      return { sessionId: "restored" };
                    }),
                }),
              }),
            ),
          );
          const approvals = yield* system.spawn("approvals", ApprovalQueueActor);
          const root = yield* system.spawn("signals", SignalRootActor);
          if (!restarting) {
            yield* root.tell({
              _tag: "Trigger",
              slug: "restore",
              sourceContext: { path: "/source", description: "source", state: {}, messages: [] },
            });
            yield* until(() => approvalEntries(registry).length === 1);
            yield* approvals.ask((replyTo) => ({
              _tag: "Resolve",
              id: approvalEntries(registry)[0]!.id,
              response: { decision: "approve" },
              replyTo,
            }));
            assert.equal(submitted, 0);
          } else {
            yield* until(() =>
              Object.values(registry.snapshot()).some(
                (r) =>
                  r.path.includes("/runs/") &&
                  (r.state as { status?: string }).status === "completed",
              ),
            );
            assert.equal(approvalEntries(registry)[0]!.status, "acknowledged");
          }
        }),
      ),
    );
  assert.equal(prepared, 1);
  assert.equal(submitted, 1);
});
