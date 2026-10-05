import { taskExecutionLayer, personalReasoningLayer } from "./workflow-fixtures.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import { Deferred, Effect, Layer, Schema } from "effect";
import {
  ContextRegistry,
  contextView,
  defineContext,
  makeApplicationApi,
  makeContextMaintenance,
  makeMemoryGoalHistory,
  PersonalActions,
  PersonalAgentActor,
  type ContextRecord,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";

const secret = "PRIVATE_PROVIDER_SENTINEL";
const record = (path: string, state: object, messages: readonly unknown[] = []): ContextRecord => ({
  path,
  revision: 7,
  description: path,
  state,
  messages,
});
const request = {
  id: "permission",
  kind: "approval",
  prompt: "Allow this task?",
  metadata: { credential: secret },
};
const task = { instructions: "Read public evidence", input: [] };
const fixtures = [
  record(
    "/delegations/work",
    {
      status: "waiting_input",
      request: { runPath: "/signals/watch/runs/one", agent: "fake", task },
      session: { sessionId: secret, metadata: { token: secret } },
      requests: { permission: request },
      responses: {},
    },
    [
      { type: "Submitted", text: "Execution started", session: { metadata: secret } },
      { role: "toolResult", content: secret },
    ],
  ),
  record("/signals/watch/runs/one", { status: "running", source: { private: secret }, task }, [
    { type: "Triggered", sourceContext: { private: secret } },
  ]),
  record("/signals/watch", {
    slug: "watch",
    when: "changed",
    task: "Read",
    agent: "fake",
    mode: "confirm",
    occurrences: [
      {
        id: "occurrence",
        text: "Changed",
        delivered: true,
        source: { path: "/source", state: { secret }, messages: [] },
      },
    ],
  }),
  record("/approvals", {
    entries: [
      {
        id: "permission",
        target: "/user/delegations/work",
        contextPath: "/delegations/work",
        kind: "approval",
        request,
        status: "pending",
      },
    ],
    commandReceipts: [{ private: secret }],
  }),
  record("/unknown", { credential: secret }, [{ private: secret }]),
  record(
    "/goals/project",
    {
      status: "active",
      tasks: [],
      summary: "Public summary",
      pendingHandoff: { private: secret },
      evaluations: [
        {
          evaluationId: "evaluation",
          status: "completed",
          resultId: "evaluation",
          reason: "Committed input",
          historyThrough: 1,
          startedAt: "2026-10-02T00:00:00Z",
          appliedAt: "2026-10-02T00:01:00Z",
          nativeTranscript: secret,
          result: {
            progress: "Public conclusion",
            completed: false,
            evidence: [],
            signals: [],
            provider: secret,
          },
        },
      ],
    },
    [
      {
        role: "assistant",
        content: [
          { type: "text", text: "Public conclusion", signature: secret },
          { type: "toolCall", id: "call", name: secret, arguments: { secret } },
        ],
        provider: secret,
      },
    ],
  ),
];
const assertPublic = (value: unknown) =>
  assert.equal(JSON.stringify(value).includes(secret), false);

test("application reads project business fields and history without altering canonical recovery data", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* makeContextRegistry({ loadAll: () => fixtures, save: () => {} });
      const before = registry.snapshot();
      const history = makeMemoryGoalHistory();
      yield* history.append("project", { role: "user", content: "Public input", timestamp: 1 });
      yield* history.append("project", {
        role: "toolResult",
        toolCallId: "call",
        toolName: "private",
        content: [{ type: "text", text: secret }],
        isError: false,
        timestamp: 2,
      });
      const api = makeApplicationApi({ registry, history, inspect: Effect.succeed(null) });
      assertPublic(yield* api.contexts);
      assertPublic(yield* api.dashboard);
      assertPublic(yield* api.goals.list);
      assertPublic(yield* api.approvals.list);
      for (const fixture of fixtures) assertPublic(yield* api.context(fixture.path));
      const delegation = yield* api.context("/delegations/work");
      const goal = yield* api.context("/goals/project");
      assert.equal("evaluations" in goal.state, false);
      assert.equal((delegation.state as { status: string }).status, "waiting_input");
      assert.equal(delegation.messages.length, 1);
      assert.deepEqual((yield* api.context("/unknown")).projection, {
        version: 1,
        visibility: "restricted",
        reason: "missing-policy",
      });
      const last = yield* api.goals.history("project", { limit: 1 });
      assert.deepEqual(last, { entries: [], total: 2, nextBefore: 2 });
      const first = yield* api.goals.history("project", { before: last.nextBefore!, limit: 1 });
      assert.equal(first.entries[0]?.seq, 1);
      assert.equal(first.nextBefore, null);
      assert.deepEqual(registry.snapshot(), before);
      assert.ok(JSON.stringify(before).includes(secret));
      Object.assign(delegation.state, { status: "tampered" });
      assert.equal(
        (registry.get(delegation.path)!.state as { status: string }).status,
        "waiting_input",
      );
    }),
  );
});

test("Personal mailbox and processor read tools expose only public snapshots", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry({ loadAll: () => fixtures, save: () => {} });
        const read = yield* Deferred.make<void>();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            PersonalActions.unavailable,
            personalReasoningLayer({
              enabled: true,
              run: (_message, reads) =>
                Effect.gen(function* () {
                  assertPublic(yield* reads.list.pipe(Effect.orDie));
                  assertPublic(yield* reads.read("/delegations/work").pipe(Effect.orDie));
                  const inspection = yield* reads
                    .inspectDelegation("/delegations/work")
                    .pipe(Effect.orDie);
                  assertPublic(inspection);
                  assert.equal(inspection.hasExecution, true);
                  assert.equal(inspection.requests[0]?.prompt, "Allow this task?");
                  yield* Deferred.succeed(read, undefined);
                  return { text: "Read allowed evidence" };
                }),
            }),
          ),
        );
        const personal = yield* system.spawn("personal", PersonalAgentActor);
        const api = makeApplicationApi({ registry, personal, inspect: Effect.succeed(null) });
        assertPublic(yield* api.personal.listContexts);
        assertPublic(yield* api.personal.readContext("/delegations/work"));
        yield* api.personal.sendMessage({
          requestId: "read",
          causationId: "user",
          expectedRevision: 1,
          text: "Read context",
        });
        yield* Deferred.await(read);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("owner policies fail closed and project the original change for reactions and capture", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* makeContextRegistry();
      const view = contextView({
        state: Schema.Struct({ summary: Schema.String }),
        message: Schema.Struct({ text: Schema.String }),
      });
      const definition = defineContext({
        state: Schema.ObjectKeyword,
        message: Schema.Unknown,
        view,
        changes: "durable-state",
      });
      yield* registry.register("/source", definition);
      const source = yield* registry.commit(
        record("/source", { summary: "first", metadata: secret }, [
          { text: "Evidence", token: secret },
        ]),
        { expectedRevision: 0 },
      );
      yield* registry.commit(
        { ...source, state: { summary: "newer", metadata: secret } },
        { expectedRevision: source.revision! },
      );
      let captured = false;
      yield* makeContextMaintenance({
        registry,
        capture: (capture) =>
          Effect.sync(() => {
            assertPublic(capture);
            assert.equal((capture.records[0]!.state as { summary: string }).summary, "first");
            captured = true;
          }),
        captures: { select: (source) => ({ sessionId: "capture", records: [source] }) },
        descriptions: { identity: () => undefined },
        describe: () => Effect.succeed("source"),
      })({ record: source });
      const evidence = registry.backend.journal()[0]!.record;
      assertPublic(evidence);
      assert.equal((evidence.state as { summary: string }).summary, "first");
      assert.equal(evidence.revision, source.revision);
      assert.ok(captured);
      const invalid = registry.views.project(record("/source", { summary: 42, token: secret }));
      assert.deepEqual(invalid.projection, {
        version: 1,
        visibility: "restricted",
        reason: "invalid-data",
      });
      assertPublic(invalid);
      yield* registry.views.register([
        contextView({
          matches: (path) => path.startsWith("/archive/"),
          state: Schema.Struct({ summary: Schema.String }),
        }),
      ]);
      const archived = registry.views.project(
        record("/archive/one", { summary: "Saved", token: secret }),
      );
      assert.equal(archived.projection?.visibility, "public");
      assertPublic(archived);
    }),
  );
});

test("Run preparation and readiness receive projected frozen evidence", async () => {
  const { ExternalAgents, SignalRunActor, contextSpawnOptions } = await import("../src/index.js");
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry({ loadAll: () => fixtures, save: () => {} });
        const source = fixtures[0]!;
        const checked = yield* Deferred.make<void>();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(ExternalAgents, {}),
            taskExecutionLayer({
              prepare: (_definition, evidence, snapshot) =>
                Effect.sync(() => {
                  assertPublic(evidence);
                  assertPublic(snapshot);
                  assert.equal(evidence.revision, source.revision);
                  return task;
                }),
              ready: (_definition, evidence) =>
                Effect.gen(function* () {
                  assertPublic(evidence);
                  assert.equal(evidence.path, source.path);
                  yield* Deferred.succeed(checked, undefined);
                  return false;
                }),
            }),
          ),
        );
        const path = "/signals/watch/runs/preparation";
        const actor = yield* system.spawn("preparation", SignalRunActor, contextSpawnOptions(path));
        yield* actor.tell({
          _tag: "Initialize",
          path,
          definition: {
            slug: "watch",
            when: "changed",
            task: "Read",
            agent: "fake",
            mode: "confirm",
          },
          sourceContext: source,
        });
        yield* Deferred.await(checked);
        assert.ok(
          JSON.stringify(registry.get(path)!.state).includes(secret),
          "owner retains exact recovery evidence",
        );
        assertPublic(registry.views.project(registry.get(path)!));
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});
