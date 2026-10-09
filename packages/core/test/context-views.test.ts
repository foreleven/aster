import { PublicApprovalEntry } from "../src/approvals/view.js";
import { approvalEntries } from "../src/approvals/actor.js";
import { goalTimeline } from "../src/goals/view.js";
import { testConversations } from "./conversation-fixtures.js";
import assert from "node:assert/strict";
import { test } from "node:test";

import { Effect, Schema } from "effect";
import { contextView, type ContextSnapshot } from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";

const secret = "PRIVATE_PROVIDER_SENTINEL";
const record = (
  path: string,
  state: object,
  messages: readonly unknown[] = [],
): ContextSnapshot => ({
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
      request: { taskPath: "/signals/watch/tasks/one", agent: "fake", task },
      session: { sessionId: secret, metadata: { token: secret } },
      requests: { permission: request },
      responses: {},
    },
    [
      { type: "Submitted", text: "Execution started", session: { metadata: secret } },
      { role: "toolResult", content: secret },
    ],
  ),
  record("/signals/watch/tasks/one", { status: "running", source: { private: secret }, task }, [
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
    revokedIds: [],
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
      definition: { slug: "project", description: "Project" },
      inputs: [],
      receipts: [],
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
      const registry = yield* makeContextRegistry({
        loadAll: () => fixtures.map((snapshot) => ({ snapshot, events: [] })),
        save: () => {},
      });
      const before = registry.snapshot();
      const history = testConversations();
      yield* history.append("/goals/project", "public", "goal.input", {
        payload: { _tag: "UserInput", text: "Public input" },
      });
      yield* history.append("/goals/project", "tool", "tool.record", {
        role: "toolResult",
        toolCallId: "call",
        toolName: "private",
        content: [{ type: "text", text: secret }],
        isError: false,
        timestamp: 2,
      });
      assertPublic(Object.values(registry.reader.snapshot()));
      assertPublic(
        Object.values(registry.reader.snapshot()).filter((r) => /^\/goals\/[^/]+$/.test(r.path)),
      );
      assertPublic(
        Schema.decodeUnknownSync(Schema.Array(PublicApprovalEntry))(approvalEntries(registry)),
      );
      for (const fixture of fixtures) assertPublic(registry.reader.get(fixture.path)!);
      const delegation = registry.reader.get("/delegations/work")!;
      const goal = registry.reader.get("/goals/project")!;
      assert.equal("evaluations" in goal.state, false);
      assert.equal(delegation.projection?.visibility, "restricted");
      assert.equal(delegation.messages.length, 0);
      assert.deepEqual(registry.reader.get("/unknown")!.projection, {
        visibility: "restricted",
        reason: "missing-policy",
      });
      const last = yield* goalTimeline(registry, history, "project", { limit: 1 });
      assert.equal(last.messages.length, 1);
      assert.equal(last.total, 1);
      assert.equal(last.nextBefore, null);
      assert.match(JSON.stringify(last), /Public input/);
      assertPublic(last);
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

test("owner policies fail closed and project the original change for reactions and capture", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* makeContextRegistry();
      const view = contextView({
        state: Schema.Struct({ summary: Schema.String }),
        message: Schema.Struct({ text: Schema.String }),
      });
      const definition = {
        state: Schema.ObjectKeyword,
        message: Schema.Unknown,
        view,
        changes: "durable-state" as const,
      };
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
      const captured = registry.views.project(source);
      assertPublic(captured);
      assert.equal((captured.state as { summary: string }).summary, "first");
      const evidence = registry.backend.journal()[0]!.record;
      assertPublic(evidence);
      assert.equal((evidence.state as { summary: string }).summary, "first");
      assert.equal(evidence.revision, source.revision);
      assert.ok(captured);
      const invalid = registry.views.project(record("/source", { summary: 42, token: secret }));
      assert.deepEqual(invalid.projection, {
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
